//go:build linux && cgo

package gbase8scdc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"qmigration/backend/internal/domain"
	"testing"
)

const nativeProviderTestC = `
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct { int marker; } state;
uint32_t qm_gbase8s_cdc_abi_version(void) { return 4; }
void qm_gbase8s_cdc_free(char *p) { free(p); }
void* qm_gbase8s_cdc_open(const char *config, char **err) {
    (void)err;
    if (!config || strstr(config, "test") == NULL) return NULL;
    state *s = (state*)calloc(1, sizeof(state)); s->marker = 7; return s;
}
int qm_gbase8s_cdc_health(void *h, char **err) { (void)err; return h ? 0 : 1; }
char* qm_gbase8s_cdc_checkpoint(void *h, const char *req, char **err) {
    (void)err; if (!h || !req || strstr(req, "orders") == NULL) return NULL;
    return strdup("{\"sequence\":\"42\",\"api_version\":\"cabi-v4\",\"capture_lineage\":\"__LINEAGE__\",\"schema_fences\":[{\"table_id\":1,\"fingerprint\":\"__FP__\"}]}");
}
char* qm_gbase8s_cdc_read(void *h, const char *req, char **err) {
    (void)err; if (!h || !req) return NULL;
    return strdup("{\"records\":[{\"kind\":\"BEGIN\",\"sequence\":\"42\",\"transaction_id\":9}],\"next_sequence\":\"43\",\"capture_lineage\":\"__LINEAGE__\",\"schema_fences\":[{\"table_id\":1,\"fingerprint\":\"__FP__\"}]}");
}
void qm_gbase8s_cdc_close(void *h) { free(h); }
`

func buildNativeProviderTestLibrary(t *testing.T, fingerprint string) string {
	t.Helper()
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("cc is not installed")
	}
	dir := t.TempDir()
	src, lib := filepath.Join(dir, "provider.c"), filepath.Join(dir, "provider.so")
	source := strings.ReplaceAll(nativeProviderTestC, "__FP__", fingerprint)
	source = strings.ReplaceAll(source, "__LINEAGE__", strings.Repeat("a", CaptureLineageHexLength))
	if err := os.WriteFile(src, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(cc, "-shared", "-fPIC", "-O2", "-o", lib, src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build provider: %v\n%s", err, out)
	}
	return lib
}

func TestNativeProviderCABI(t *testing.T) {
	sel, err := BuildTableSelection("app", "orders", []domain.ColumnInfo{{Name: "id", ColumnType: "INTEGER", Nullable: false}}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	sel.ID = 1
	lib := buildNativeProviderTestLibrary(t, sel.SchemaFingerprint)
	b, err := os.ReadFile(lib)
	if err != nil {
		t.Fatal(err)
	}
	sum := fmt.Sprintf("%x", sha256.Sum256(b))
	a, err := OpenNativeProvider(lib, sum, `{"mode":"test"}`)
	if err != nil {
		t.Fatal(err)
	}
	defer a.(interface{ Close() error }).Close()
	if err := a.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	cp, err := a.Checkpoint(context.Background(), CheckpointRequest{Database: "app", Tables: []TableSelection{sel}})
	if err != nil {
		t.Fatal(err)
	}
	if cp.Sequence != "42" || cp.APIVersion != "cabi-v4" {
		t.Fatalf("checkpoint=%+v", cp)
	}
	if err := ValidateSchemaFences([]TableSelection{sel}, cp.SchemaFences); err != nil {
		t.Fatal(err)
	}
	rr, err := a.Read(context.Background(), ReadRequest{Database: "app", StartSequence: "42", ExpectedCaptureLineage: strings.Repeat("a", CaptureLineageHexLength), Tables: []TableSelection{sel}})
	if err != nil {
		t.Fatal(err)
	}
	if rr.NextSequence != "43" || len(rr.Records) != 1 || rr.Records[0].Kind != KindBegin {
		t.Fatalf("read=%+v", rr)
	}
	if err := ValidateSchemaFences([]TableSelection{sel}, rr.SchemaFences); err != nil {
		t.Fatal(err)
	}
}

func TestNativeProviderRejectsWrongSHA(t *testing.T) {
	lib := buildNativeProviderTestLibrary(t, strings.Repeat("0", 64))
	if _, err := OpenNativeProvider(lib, string(make([]byte, 64)), `{"mode":"test"}`); err == nil {
		t.Fatal("expected invalid sha rejection")
	}
}
