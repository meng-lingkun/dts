package oracleconnector

import "testing"

func TestCoalesceOracleLogMinerFragments(t *testing.T) {
	in := []OracleLogMinerRecord{
		{SCN: 10, CommitSCN: 20, RSID: "r1", SSN: "1", SQLRedo: "ALTER TABLE T ", SQLUndo: "", CSF: true, Info: "USER DDL"},
		{SCN: 10, CommitSCN: 20, RSID: "r1", SSN: "1", SQLRedo: "ADD C NUMBER", SQLUndo: "", CSF: false, Info: "USER DDL"},
		{SCN: 11, CommitSCN: 20, RSID: "r2", SSN: "2", SQLRedo: "UPDATE T SET C=1", CSF: false},
	}
	out := coalesceOracleLogMinerFragments(in)
	if len(out) != 2 {
		t.Fatalf("len=%d out=%+v", len(out), out)
	}
	if out[0].SQLRedo != "ALTER TABLE T ADD C NUMBER" || out[0].CSF {
		t.Fatalf("coalesced=%+v", out[0])
	}
	if out[1].SQLRedo != "UPDATE T SET C=1" {
		t.Fatalf("second=%+v", out[1])
	}
}

func TestCoalesceOracleLogMinerFragmentsDoesNotCrossRecordBoundary(t *testing.T) {
	in := []OracleLogMinerRecord{
		{RSID: "r1", SSN: "1", SQLRedo: "A", CSF: true},
		{RSID: "r2", SSN: "1", SQLRedo: "B", CSF: false},
	}
	out := coalesceOracleLogMinerFragments(in)
	if len(out) != 2 || out[0].SQLRedo != "A" || out[1].SQLRedo != "B" {
		t.Fatalf("out=%+v", out)
	}
}

func TestOracleLogMinerUserDDL(t *testing.T) {
	for _, v := range []string{"USER DDL", "USER_DDL", "  user   ddl "} {
		if !oracleLogMinerUserDDL(v) {
			t.Fatalf("expected user ddl: %q", v)
		}
	}
	for _, v := range []string{"INTERNAL DDL", "INTERNAL_DDL", "", "USER DML"} {
		if oracleLogMinerUserDDL(v) {
			t.Fatalf("unexpected user ddl: %q", v)
		}
	}
}
