package oracleconnector

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net"
	"testing"

	"qmigration/backend/internal/domain"
)

func encodeFakeDescribeColumn(w *ttcEncoder, dataType byte, precision byte, maxLen uint64, charsetID uint64, charsetForm byte, maxCharLen uint64, nullable bool, name, typeName string, ttcVersion byte) {
	w.byte(dataType)
	w.byte(0)
	w.byte(precision)
	w.byte(0) // signed compact scale=0
	w.compactUint(maxLen, 4)
	w.compactUint(0, 4)
	w.compactUint(0, 4)
	w.clr(nil)
	w.compactUint(0, 2)
	w.compactUint(charsetID, 2)
	w.byte(charsetForm)
	w.compactUint(maxCharLen, 4)
	if nullable {
		w.byte(1)
	} else {
		w.byte(0)
	}
	w.byte(0)
	w.clr([]byte(name))
	w.clr(nil)
	w.clr([]byte(typeName))
	if ttcVersion >= 3 {
		w.compactUint(0, 2)
	}
	if ttcVersion >= 6 {
		w.compactUint(0, 4)
	}
}

func fakeDescribePayload(ttcVersion byte, columns ...func(*ttcEncoder)) []byte {
	w := &ttcEncoder{}
	w.byte(0) // describe header length
	w.compactUint(128, 4)
	w.compactUint(uint64(len(columns)), 4)
	if len(columns) > 0 {
		w.byte(0)
	}
	for _, col := range columns {
		col(w)
	}
	w.clr(nil)
	if ttcVersion >= 3 {
		w.compactUint(0, 4)
		w.compactUint(0, 4)
	}
	if ttcVersion >= 4 {
		w.compactUint(0, 4)
		w.compactUint(0, 4)
	}
	if ttcVersion >= 5 {
		w.clr(nil)
	}
	return w.Bytes()
}

func TestBuildTTCSelectRequest(t *testing.T) {
	const sql = "SELECT 1 AS QMIGRATION_PROBE FROM DUAL"
	b, err := buildTTCSelectRequest(sql, 12, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 8 || b[0] != 3 || b[1] != 0x5e || b[2] != 0 {
		t.Fatalf("request prefix=%x", b[:min(8, len(b))])
	}
	if !bytes.Contains(b, []byte(sql)) {
		t.Fatalf("SQL missing from request: %x", b)
	}
}

func TestTTCDescribeAndRowCodec(t *testing.T) {
	payload := fakeDescribePayload(12,
		func(w *ttcEncoder) {
			encodeFakeDescribeColumn(w, oracleTypeNUMBER, 38, 22, 0, 0, 0, false, "ID", "", 12)
		},
		func(w *ttcEncoder) {
			encodeFakeDescribeColumn(w, oracleTypeVARCHAR, 0, 100, 873, 1, 100, true, "NAME", "", 12)
		},
	)
	cols, err := parseTTCDescribe(payload, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || cols[0].Name != "ID" || cols[0].DataType != oracleTypeNUMBER || cols[1].Name != "NAME" || !cols[1].Nullable {
		t.Fatalf("cols=%+v", cols)
	}
	row := &ttcEncoder{}
	row.clr([]byte{0xc1, 0x02}) // NUMBER 1
	row.clr([]byte("alice"))
	values, err := parseTTCRow(row.Bytes(), cols)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(values[0]); got != "1" || values[1] != "alice" {
		t.Fatalf("row=%#v", values)
	}
}

func TestOracleNumberCanonicalDecimal(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
	}{
		{[]byte{0x80}, "0"},
		{[]byte{0xc1, 0x02}, "1"},
		{[]byte{0xc2, 0x02}, "100"},
		{[]byte{0xc2, 0x02, 0x18, 0x2e}, "123.45"},
		{[]byte{0x3e, 0x64, 0x66}, "-1"},
	}
	for _, tc := range cases {
		got, err := decodeOracleNumberString(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("%x => %q err=%v want=%q", tc.in, got, err, tc.want)
		}
	}
}

func TestTTCDescribeRejectsTruncation(t *testing.T) {
	if _, err := parseTTCDescribe([]byte{0, 1}, 12); err == nil {
		t.Fatal("expected truncated describe error")
	}
}

func domainDataSourceOracle(port int, password string) domain.DataSource {
	return domain.DataSource{Type: domain.DataSourceOracle, Host: "127.0.0.1", Port: port, Database: "ORCL", Username: "scott", Password: password}
}

func TestNativeOracleExperimentalTTCQueryTranscript(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY", "1")
	const password = "Secret123!"
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		if typ, _, err := readTNSPacket(c); err != nil || typ != tnsConnect {
			serverErr <- fmt.Errorf("connect typ=%d err=%v", typ, err)
			return
		}
		if err := sendTNSPacket(c, tnsAccept, []byte{0x01, 0x39}); err != nil {
			serverErr <- err
			return
		}
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 3 || body[2] != 1 {
			serverErr <- fmt.Errorf("protocol typ=%d body=%x err=%v", typ, body, err)
			return
		}
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0}, fakeTTCProtocolResponse()...)); err != nil {
			serverErr <- err
			return
		}
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 3 || body[2] != 2 {
			serverErr <- fmt.Errorf("datatype typ=%d body=%x err=%v", typ, body, err)
			return
		}
		if err := sendTNSPacket(c, tnsData, []byte{0, 0, 2, 0}); err != nil {
			serverErr <- err
			return
		}
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 4 || body[2] != 3 || body[3] != 118 {
			serverErr <- fmt.Errorf("auth init typ=%d body=%x err=%v", typ, body, err)
			return
		}
		saltHex := "00112233445566778899AABBCCDDEEFF"
		salt, _ := hex.DecodeString(saltHex)
		h := sha1.Sum(append([]byte(password), salt...))
		key := append(append([]byte(nil), h[:]...), 0, 0, 0, 0)
		serverKey := bytes.Repeat([]byte{0x5a}, 48)
		enc, err := aesCBCEncryptHex(key, serverKey, false)
		if err != nil {
			serverErr <- err
			return
		}
		challenge := &ttcEncoder{}
		challenge.byte(8)
		challenge.compactUint(2, 4)
		challenge.keyVal([]byte("AUTH_SESSKEY"), []byte(enc), 1)
		challenge.keyVal([]byte("AUTH_VFR_DATA"), []byte(saltHex), 6949)
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0}, challenge.Bytes()...)); err != nil {
			serverErr <- err
			return
		}
		if typ, body, err := readTNSPacket(c); err != nil || typ != tnsData || len(body) < 4 || body[2] != 3 || body[3] != 0x73 {
			serverErr <- fmt.Errorf("auth response typ=%d body=%x err=%v", typ, body, err)
			return
		} else if bytes.Contains(body, []byte(password)) {
			serverErr <- fmt.Errorf("plaintext password leaked")
			return
		}
		result := &ttcEncoder{}
		result.byte(8)
		result.compactUint(1, 4)
		result.keyVal([]byte("AUTH_SESSION_ID"), []byte("42"), 0)
		result.byte(4)
		result.compactUint(0, 4)
		result.compactUint(0, 2)
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0}, result.Bytes()...)); err != nil {
			serverErr <- err
			return
		}

		// OALL8 parse+execute request.
		typ, body, err := readTNSPacket(c)
		if err != nil || typ != tnsData || len(body) < 6 || body[2] != 3 || body[3] != 0x5e || !bytes.Contains(body, []byte("SELECT 1 AS QMIGRATION_PROBE FROM DUAL")) {
			serverErr <- fmt.Errorf("query request typ=%d body=%x err=%v", typ, body, err)
			return
		}
		describe := fakeDescribePayload(12, func(w *ttcEncoder) {
			encodeFakeDescribeColumn(w, oracleTypeNUMBER, 38, 22, 0, 0, 0, false, "QMIGRATION_PROBE", "", 12)
		})
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0, ttcDescribe}, describe...)); err != nil {
			serverErr <- err
			return
		}
		header := fakeTTCRowHeaderPayload(1, 1, nil)
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0, ttcRowHeader}, header...)); err != nil {
			serverErr <- err
			return
		}
		row := &ttcEncoder{}
		row.clr([]byte{0xc1, 0x02})
		if err := sendTNSPacket(c, tnsData, append([]byte{0, 0, ttcRowData}, row.Bytes()...)); err != nil {
			serverErr <- err
			return
		}
		if err := sendTNSPacket(c, tnsData, []byte{0, 0, ttcFunctionStat}); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()
	addr := ln.Addr().(*net.TCPAddr)
	raw, err := NewFactory().New(domainDataSourceOracle(addr.Port, password))
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.TestConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := raw.GetVersion(context.Background())
	if err != nil || v != "oracle-ttc-v6-charset-873-ttc12-auth-query" {
		t.Fatalf("version=%q err=%v", v, err)
	}
	if c := raw.(*Connector); c.sessionProperties["AUTH_SESSION_ID"] != "42" {
		t.Fatalf("props=%v", c.sessionProperties)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestOracleTimestampAndBinaryFloatDecode(t *testing.T) {
	// 2026-08-30 17:18:19.123456789
	ts := []byte{120, 126, 8, 30, 18, 19, 20, 0x07, 0x5b, 0xcd, 0x15}
	got, err := decodeOracleTTCScalar(oracleTTCColumn{DataType: oracleTypeTimestamp}, ts)
	if err != nil || got != "2026-08-30 17:18:19.123456789" {
		t.Fatalf("timestamp=%v err=%v", got, err)
	}
	tz := append(append([]byte(nil), ts...), 28, 60) // +08:00
	got, err = decodeOracleTTCScalar(oracleTTCColumn{DataType: oracleTypeTimestampTZ}, tz)
	if err != nil || got != "2026-08-30 17:18:19.123456789+08:00" {
		t.Fatalf("timestamp tz=%v err=%v", got, err)
	}
	got, err = decodeOracleTTCScalar(oracleTTCColumn{DataType: oracleTypeBinaryFloat}, []byte{0x3f, 0xc0, 0x00, 0x00})
	if err != nil || got != "1.5" {
		t.Fatalf("binary float=%v err=%v", got, err)
	}
	got, err = decodeOracleTTCScalar(oracleTTCColumn{DataType: oracleTypeBinaryDouble}, []byte{0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	if err != nil || got != "2.5" {
		t.Fatalf("binary double=%v err=%v", got, err)
	}
}
