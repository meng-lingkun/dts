package oracleconnector

import (
	"bytes"
	"testing"
)

func TestBuildTTCDataTypeRequestAdvertisesMigrationTypes(t *testing.T) {
	server := ttcProtocolInfo{
		ServerCharset:  873,
		ServerNCharset: 2000,
		ServerFlags:    1,
		CompileTimeCaps: []byte{
			6, 1, 0, 0, 0x28, 1, 1, 8,
			1, 1, 1, 1, 1, 1, 0, 1,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 1, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 2,
		},
		RuntimeCaps: []byte{2, 1, 0, 0, 0, 0, 6},
	}
	info, request, err := buildTTCDataTypeRequest(server)
	if err != nil {
		t.Fatal(err)
	}
	if len(request) == 0 || request[0] != 2 {
		t.Fatalf("datatype request=%x", request)
	}
	if len(info.CompileTimeCaps) == 0 || len(info.RuntimeCaps) == 0 {
		t.Fatalf("datatype info=%+v", info)
	}
	if info.TTCVersion != 8 {
		t.Fatalf("TTCVersion=%d want 8", info.TTCVersion)
	}
	// The migration-relevant type table must at least advertise VARCHAR2,
	// NUMBER and DATE; this catches accidental empty/metadata-only negotiation.
	if !bytes.Contains(request, []byte{0, 1, 0, 1, 0, 1, 0, 0}) ||
		!bytes.Contains(request, []byte{0, 2, 0, 2, 0, 10, 0, 0}) ||
		!bytes.Contains(request, []byte{0, 12, 0, 12, 0, 10, 0, 0}) {
		t.Fatalf("datatype request missing migration scalar reps: %x", request)
	}
}

func TestParseTTCDataTypeResponseAllowsEmptyRepresentationList(t *testing.T) {
	info := ttcDataTypeInfo{CompileTimeCaps: make([]byte, 38), RuntimeCaps: []byte{2, 0}, TTCVersion: 6}
	if err := parseTTCDataTypeResponse([]byte{2}, &info); err != nil {
		t.Fatal(err)
	}
}

func TestParseTTCDataTypeResponseRejectsMissingTerminator(t *testing.T) {
	info := ttcDataTypeInfo{CompileTimeCaps: make([]byte, 38), RuntimeCaps: []byte{2, 0}, TTCVersion: 6}
	if err := parseTTCDataTypeResponse([]byte{2, 1, 1, 1}, &info); err == nil {
		t.Fatal("expected missing datatype terminator to fail")
	}
}

func TestParseTTCDataTypeResponseRequiresTimezoneWhenNegotiated(t *testing.T) {
	info := ttcDataTypeInfo{CompileTimeCaps: make([]byte, 38), RuntimeCaps: []byte{2, 1}, TTCVersion: 6}
	if err := parseTTCDataTypeResponse([]byte{2, 0}, &info); err == nil {
		t.Fatal("expected truncated timezone to fail")
	}
}

func TestTTCVersionUsesLowerServerCapability(t *testing.T) {
	server := ttcProtocolInfo{
		ServerCharset:   873,
		ServerNCharset:  2000,
		CompileTimeCaps: []byte{6, 1, 0, 0, 0, 0, 0, 5},
		RuntimeCaps:     []byte{2, 0},
	}
	info, _, err := buildTTCDataTypeRequest(server)
	if err != nil {
		t.Fatal(err)
	}
	if info.TTCVersion != 5 {
		t.Fatalf("TTCVersion=%d want 5", info.TTCVersion)
	}
}

func TestOracleExperimentalTTCDoesNotAdvertiseFullOrCDC(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH", "1")
	d := NewFactory().Capabilities("ORACLE")
	if len(d.Capabilities) != 1 || string(d.Capabilities[0]) != "protocol-probe" {
		t.Fatalf("experimental TTC leaked production capabilities: %+v", d.Capabilities)
	}
}
