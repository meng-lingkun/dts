/*
 * QMigration DB2 read-log provider.
 *
 * Build only on the DB2 Log Agent host against IBM Data Server Client
 * headers/libdb2. The pure-Go QMigration binaries never link libdb2.
 */
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <db2ApiDf.h>
#include <sqlca.h>
#include <sqlcli1.h>

#ifndef QMIGRATION_DB2_API_VERSION
# ifdef db2Version11580
#  define QMIGRATION_DB2_API_VERSION db2Version11580
# elif defined(db2Version1150)
#  define QMIGRATION_DB2_API_VERSION db2Version1150
# elif defined(db2Version1110)
#  define QMIGRATION_DB2_API_VERSION db2Version1110
# else
#  define QMIGRATION_DB2_API_VERSION 0
# endif
#endif

static SQLHENV henv = SQL_NULL_HENV;
static SQLHDBC hdbc = SQL_NULL_HDBC;

static int host_little(void) {
    uint16_t x = 1;
    return *((unsigned char *)&x) == 1;
}

static uint16_t load16(const unsigned char *p) {
    uint16_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}

static void print_lri(db2LRI x) {
    printf("{\"type\":%" PRIu64 ",\"part1\":%" PRIu64 ",\"part2\":%" PRIu64 "}",
           (uint64_t)x.lriType, (uint64_t)x.part1, (uint64_t)x.part2);
}

static int lri_zero(db2LRI x) {
    return x.lriType == 0 && x.part1 == 0 && x.part2 == 0;
}

static int parse_lri(const char *s, db2LRI *out) {
    unsigned long long a, b, c;
    if (s == NULL || sscanf(s, "%llx:%llx:%llx", &a, &b, &c) != 3) {
        return -1;
    }
    memset(out, 0, sizeof(*out));
    out->lriType = (db2Uint64)a;
    out->part1 = (db2Uint64)b;
    out->part2 = (db2Uint64)c;
    return 0;
}

static const char b64tab[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char *base64_encode(const unsigned char *src, size_t n) {
    size_t outn = 4 * ((n + 2) / 3);
    char *out = (char *)malloc(outn + 1);
    size_t i = 0, j = 0;
    if (out == NULL) {
        return NULL;
    }
    while (i < n) {
        uint32_t a = i < n ? src[i++] : 0;
        uint32_t b = i < n ? src[i++] : 0;
        uint32_t c = i < n ? src[i++] : 0;
        uint32_t triple = (a << 16) | (b << 8) | c;
        out[j++] = b64tab[(triple >> 18) & 63];
        out[j++] = b64tab[(triple >> 12) & 63];
        out[j++] = b64tab[(triple >> 6) & 63];
        out[j++] = b64tab[triple & 63];
    }
    if (n % 3 != 0) {
        out[outn - 1] = '=';
    }
    if (n % 3 == 1) {
        out[outn - 2] = '=';
    }
    out[outn] = '\0';
    return out;
}

static void disconnect_db(void) {
    if (hdbc != SQL_NULL_HDBC) {
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        hdbc = SQL_NULL_HDBC;
    }
    if (henv != SQL_NULL_HENV) {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        henv = SQL_NULL_HENV;
    }
}

static int connect_db(void) {
    const char *db = getenv("QMIGRATION_DB2_DATABASE");
    const char *user = getenv("QMIGRATION_DB2_USER");
    const char *pw = getenv("QMIGRATION_DB2_PASSWORD");
    SQLRETURN rc;

    if (db == NULL || *db == '\0' || user == NULL || *user == '\0') {
        fprintf(stderr, "QMIGRATION_DB2_DATABASE and QMIGRATION_DB2_USER are required\n");
        return 2;
    }
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv) != SQL_SUCCESS) {
        return 2;
    }
    if (SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0) != SQL_SUCCESS) {
        return 2;
    }
    if (SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc) != SQL_SUCCESS) {
        return 2;
    }
    rc = SQLConnect(hdbc, (SQLCHAR *)db, SQL_NTS,
                    (SQLCHAR *)user, SQL_NTS,
                    (SQLCHAR *)(pw != NULL ? pw : ""), SQL_NTS);
    if (!(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)) {
        fprintf(stderr, "SQLConnect failed\n");
        return 2;
    }
    (void)SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
                            (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    return 0;
}

static int call_readlog(db2ReadLogStruct *request, struct sqlca *ca) {
    int rc;
    memset(ca, 0, sizeof(*ca));
    rc = db2ReadLog(QMIGRATION_DB2_API_VERSION, request, ca);
    if (rc != 0 && ca->sqlcode < 0) {
        fprintf(stderr, "db2ReadLog failed rc=%d sqlcode=%ld\n",
                rc, (long)ca->sqlcode);
        return -1;
    }
    return 0;
}

static int query_position(db2ReadLogInfoStruct *info) {
    db2ReadLogStruct request;
    struct sqlca ca;
    memset(&request, 0, sizeof(request));
    memset(info, 0, sizeof(*info));
    request.iCallerAction = DB2READLOG_QUERY;
    request.iFilterOption = DB2READLOG_FILTER_ON;
    request.poReadLogInfo = info;
    return call_readlog(&request, &ca);
}

static int emit_position(void) {
    db2ReadLogInfoStruct info;
    db2LRI current;
    const char *db = getenv("QMIGRATION_DB2_DATABASE");

    if (query_position(&info) != 0) {
        return 3;
    }
    /* nextStartLRI is available on pre-11.5.8 clients and is the documented
     * cursor for the next sequential read. Avoid finalLRI here because that
     * field was added in 11.5.8 and older SDK headers do not contain it. */
    current = info.nextStartLRI;
    printf("{\"initial_lri\":");
    print_lri(info.initialLRI);
    printf(",\"next_start_lri\":");
    print_lri(current);
    printf(",\"current_end_lri\":");
    print_lri(current);
    printf(",\"byte_order\":\"%s\",\"recoverable\":true,\"database\":\"%s\"}\n",
           host_little() ? "little" : "big", db != NULL ? db : "");
    return 0;
}

struct parsed_record {
    db2ReadLogFilterData *filter;
    unsigned char *raw;
    size_t rawlen;
};

static int emit_read(db2LRI start, int max_records, size_t max_bytes) {
    char *buffer;
    db2ReadLogInfoStruct info;
    db2ReadLogStruct request;
    struct sqlca ca;
    db2LRI end;
    struct parsed_record *records;
    size_t off = 0;
    int count = 0;
    int emit_count;
    db2LRI next;
    int read_to_current = 0;

    if (max_records <= 0) {
        max_records = 4096;
    }
    if (max_records > 16384) {
        max_records = 16384;
    }
    if (max_bytes < 65536) {
        max_bytes = 65536;
    }
    if (max_bytes > (size_t)256 * 1024 * 1024) {
        max_bytes = (size_t)256 * 1024 * 1024;
    }

    buffer = (char *)malloc(max_bytes);
    if (buffer == NULL) {
        fprintf(stderr, "log buffer allocation failed\n");
        return 3;
    }
    memset(&end, 0, sizeof(end));
    end.lriType = DB2READLOG_LRI_1;
    end.part1 = (db2Uint64)~(db2Uint64)0;
    end.part2 = (db2Uint64)~(db2Uint64)0;
    memset(&request, 0, sizeof(request));
    memset(&info, 0, sizeof(info));
    request.iCallerAction = DB2READLOG_READ;
    request.piStartLRI = &start;
    request.piEndLRI = &end;
    request.poLogBuffer = buffer;
    request.iLogBufferSize = (db2Uint32)max_bytes;
    request.iFilterOption = DB2READLOG_FILTER_ON;
    request.poReadLogInfo = &info;
    if (call_readlog(&request, &ca) != 0) {
        free(buffer);
        return 3;
    }

    records = (struct parsed_record *)calloc(
        (size_t)(info.logRecsWritten != 0 ? info.logRecsWritten : 1),
        sizeof(*records));
    if (records == NULL) {
        free(buffer);
        return 3;
    }

    while (off + sizeof(db2ReadLogFilterData) <= info.logBytesWritten &&
           count < (int)info.logRecsWritten) {
        db2ReadLogFilterData *fd = (db2ReadLogFilterData *)(buffer + off);
        size_t need = sizeof(*fd) + (size_t)fd->realLogRecLen;
        if (fd->sqlcode != 0) {
            fprintf(stderr, "db2ReadLog row decompression sqlcode=%d\n",
                    (int)fd->sqlcode);
            free(records);
            free(buffer);
            return 3;
        }
        if (fd->realLogRecLen < 40 || off + need > info.logBytesWritten) {
            fprintf(stderr, "invalid filtered log record length\n");
            free(records);
            free(buffer);
            return 3;
        }
        records[count].filter = fd;
        records[count].raw = (unsigned char *)(fd + 1);
        records[count].rawlen = fd->realLogRecLen;
        count++;
        off += need;
    }

    emit_count = count;
    if (emit_count > max_records) {
        emit_count = max_records;
    }
    printf("{\"records\":[");
    for (int i = 0; i < emit_count; i++) {
        unsigned char *raw = records[i].raw;
        uint16_t type = load16(raw + 4);
        uint16_t flags = load16(raw + 6);
        char tid[13];
        char *encoded;
        db2LRI record_next = (i + 1 < count)
            ? records[i + 1].filter->recordLRIType1
            : info.nextStartLRI;
        if (i != 0) {
            printf(",");
        }
        for (int j = 0; j < 6; j++) {
            (void)sprintf(tid + j * 2, "%02x", raw[32 + j]);
        }
        tid[12] = '\0';
        encoded = base64_encode(raw, records[i].rawlen);
        if (encoded == NULL) {
            free(records);
            free(buffer);
            return 3;
        }
        printf("{\"lri\":");
        print_lri(records[i].filter->recordLRIType1);
        printf(",\"next_lri\":");
        print_lri(record_next);
        printf(",\"log_type\":%u,\"flags\":%u,\"tid\":\"%s\","
               "\"byte_order\":\"%s\",\"raw_base64\":\"%s\"}",
               (unsigned)type, (unsigned)flags, tid,
               host_little() ? "little" : "big", encoded);
        free(encoded);
    }

    next = info.nextStartLRI;
    if (emit_count < count) {
        next = records[emit_count].filter->recordLRIType1;
    }
#ifdef SQLU_RLOG_READ_TO_CURRENT
    if (ca.sqlcode == SQLU_RLOG_READ_TO_CURRENT) {
        read_to_current = 1;
    }
#endif
    printf("],\"next_start_lri\":");
    print_lri(next);
    printf(",\"current_end_lri\":");
    print_lri(info.nextStartLRI);
    printf(",\"read_to_current\":%s}\n",
           read_to_current ? "true" : "false");

    free(records);
    free(buffer);
    return 0;
}

int main(int argc, char **argv) {
    int rc;
    if (argc < 2) {
        fprintf(stderr,
                "usage: %s position | read --start-lri LRI "
                "[--max-records N] [--max-bytes N]\n",
                argv[0]);
        return 2;
    }
    rc = connect_db();
    if (rc != 0) {
        disconnect_db();
        return rc;
    }
    if (strcmp(argv[1], "position") == 0) {
        rc = emit_position();
        disconnect_db();
        return rc;
    }
    if (strcmp(argv[1], "read") == 0) {
        const char *start_arg = NULL;
        int max_records = 4096;
        size_t max_bytes = 32 * 1024 * 1024;
        db2LRI start;
        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "--start-lri") == 0 && i + 1 < argc) {
                start_arg = argv[++i];
            } else if (strcmp(argv[i], "--max-records") == 0 && i + 1 < argc) {
                max_records = atoi(argv[++i]);
            } else if (strcmp(argv[i], "--max-bytes") == 0 && i + 1 < argc) {
                max_bytes = (size_t)strtoull(argv[++i], NULL, 10);
            }
        }
        if (parse_lri(start_arg, &start) != 0) {
            fprintf(stderr, "invalid --start-lri\n");
            disconnect_db();
            return 2;
        }
        rc = emit_read(start, max_records, max_bytes);
        disconnect_db();
        return rc;
    }
    fprintf(stderr, "unknown action %s\n", argv[1]);
    disconnect_db();
    return 2;
}
