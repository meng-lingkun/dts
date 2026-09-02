#ifndef QMIGRATION_GBASE8S_CDC_PROVIDER_H
#define QMIGRATION_GBASE8S_CDC_PROVIDER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define QM_GBASE8S_CDC_ABI_VERSION 4u

/*
 * Stable C ABI used by qmigration-gbase8s-cdc-agent.
 *
 * All returned char* values (successful JSON responses and error_text) must be
 * heap objects owned by the provider and released by qm_gbase8s_cdc_free().
 * JSON strings must be UTF-8 and NUL terminated. Column bytes that are not safe
 * UTF-8 must be emitted as CDCField {"encoding":"base64",...}.
 *
 * The provider is datasource-local. It owns GBase Client-SDK credentials and
 * cdc_opensess/cdc_startcapture/cdc_activatesess/ifx_lo_read calls. It must not
 * expect QMigration to send database credentials in checkpoint/read requests.
 */
uint32_t qm_gbase8s_cdc_abi_version(void);

/* config_json is local agent configuration (default {}). */
void *qm_gbase8s_cdc_open(const char *config_json, char **error_text);

/* Return 0 when the CSDK/syscdcv1 path is usable. */
int qm_gbase8s_cdc_health(void *handle, char **error_text);

/*
 * request_json schema (abbreviated):
 *   {"database":"...","tables":[{"id":123,"schema":"s","table":"t",
 *     "columns":[...],"schema_columns":[...],"primary_keys":[...],
 *     "schema_fingerprint":"<64-hex>"}]}
 * response_json schema:
 *   {"sequence":"12345","source_timestamp_ms":0,"resource":"...",
 *    "api_version":"cabi-v4","capture_lineage":"<64-hex>","schema_fences":[{"table_id":123,"fingerprint":"<64-hex>"}],"smart_lob_image_contract":"cdc-event-owned-lob-v1"}
 *
 * The provider MUST derive/verify schema_fences from live CDC_REC_TABSCHEMA +
 * catalog metadata. Echoing an unverified requested fingerprint is not valid.
 */
char *qm_gbase8s_cdc_checkpoint(void *handle, const char *request_json, char **error_text);

/*
 * request_json schema:
 *   {"database":"...","start_sequence":"12345","expected_capture_lineage":"<64-hex>","tables":[...],"max_records":4096,"max_bytes":33554432}
 * response_json schema:
 *   {"records":[RecordEnvelope...],"next_sequence":"12346",
 *    "read_to_current":false,"capture_lineage":"<same-64-hex>","schema_fences":[{"table_id":123,"fingerprint":"<64-hex>"}],"smart_lob_image_contract":"cdc-event-owned-lob-v1"}
 *
 * RecordEnvelope kinds:
 *   BEGIN, COMMIT, ROLLBACK, INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER,
 *   DISCARD, TRUNCATE, TABLE_SCHEMA, TIMEOUT, ERROR. A forwarded TABLE_SCHEMA
 *   RecordEnvelope must include schema_fingerprint.
 *
 * capture_lineage must identify the logical capture generation durably. It must
 * remain stable across reads/resume of that same generation and must change if
 * the provider creates/replaces the capture instead of resuming it. The read
 * request expected_capture_lineage must be checked before records are returned.
 *
 * RC28: if a selected schema column has smart_lob=blob|clob, checkpoint/read
 * responses must declare smart_lob_image_contract=cdc-event-owned-lob-v1. Each
 * non-NULL LOB row field must carry one smart_lob_proofs item with column, kind,
 * byte_length, sha256 and acquisition=cdc-event-owned-lob-v1. The bytes must be
 * obtained from the CDC event-owned CSDK locator/stream; a SELECT of the current
 * table row is not a valid historical-image implementation.
 *
 * The provider must preserve the exact CDC sequence number, transaction ID and
 * cdc_startcapture user_data value (table_id). It may split smart-LOB reads at
 * arbitrary byte boundaries internally, but this function may return only
 * complete normalized CDC records. next_sequence must never move backwards.
 */
char *qm_gbase8s_cdc_read(void *handle, const char *request_json, char **error_text);

void qm_gbase8s_cdc_free(char *value);
void qm_gbase8s_cdc_close(void *handle);

#ifdef __cplusplus
}
#endif
#endif
