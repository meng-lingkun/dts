package ddlsidecar

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestReconstructHybrid(t *testing.T) {
	native := []domain.CDCEvent{{Operation: domain.CDCInsert, SourceSchema: "s", SourceTable: "t"}, {Operation: domain.CDCUpdate, SourceSchema: "s", SourceTable: "t"}}
	proof := &Response{PositionType: "KINGBASE_LSN", PositionValue: "0/20", Sequence: []Item{{Kind: "DML"}, {Kind: "DDL", SQL: "ALTER TABLE s.t ADD COLUMN c int"}, {Kind: "DML"}}}
	got, err := Reconstruct(native, proof, []string{"s.t"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].Operation != domain.CDCDDL {
		t.Fatalf("unexpected %+v", got)
	}
}
func TestReconstructRejectsUnsafeDDL(t *testing.T) {
	_, err := Reconstruct(nil, &Response{PositionType: "X", PositionValue: "1", Sequence: []Item{{Kind: "DDL", SQL: "DROP TABLE s.t"}}}, []string{"s.t"})
	if err == nil {
		t.Fatal("expected unsafe DDL rejection")
	}
}
