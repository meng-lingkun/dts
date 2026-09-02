package oracleconnector

import (
	"bytes"
	"testing"
)

func TestOracleNumberEncodeDecodeRoundTrip(t *testing.T) {
	cases := []string{
		"0", "1", "-1", "100", "123.45", "0.25", "0.0025", "-0.0025",
		"99999999999999999999999999999999999999", "1e10", "-2.5E-3",
	}
	for _, in := range cases {
		enc, err := encodeOracleNumberString(in)
		if err != nil {
			t.Fatalf("encode %s: %v", in, err)
		}
		got, err := decodeOracleNumberString(enc)
		if err != nil {
			t.Fatalf("decode %s (%x): %v", in, enc, err)
		}
		_, whole, frac, err := expandOracleDecimal(in)
		if err != nil {
			t.Fatal(err)
		}
		want := whole
		if frac != "" {
			want += "." + frac
		}
		if in[0] == '-' && want != "0" {
			want = "-" + want
		}
		if got != want {
			t.Fatalf("%s => %x => %s; want %s", in, enc, got, want)
		}
	}
}

func TestOracleNumberKnownWireValues(t *testing.T) {
	cases := map[string][]byte{
		"0":      {0x80},
		"1":      {0xc1, 0x02},
		"100":    {0xc2, 0x02},
		"123.45": {0xc2, 0x02, 0x18, 0x2e},
		"-1":     {0x3e, 0x64, 0x66},
	}
	for in, want := range cases {
		got, err := encodeOracleNumberString(in)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s => %x err=%v want=%x", in, got, err, want)
		}
	}
}

func TestTTCBindAndPreparedRequestShape(t *testing.T) {
	n, err := oracleNumberInputBind("42")
	if err != nil {
		t.Fatal(err)
	}
	s := oracleStringInputBind([]byte("alice"), 873)
	r := oracleRawInputBind([]byte{1, 2, 3})
	rows := [][]oracleTTCBind{{n, s, r}, {n, oracleStringInputBind([]byte("bob"), 873), oracleRawInputBind([]byte{4, 5})}}
	q := `INSERT INTO "APP"."T" ("ID","NAME","RAWV") VALUES (:1,:2,:3)`
	full, sig, err := buildTTCBindStatementRequest(q, 12, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	if sig == "" || len(full) < 20 || full[0] != 3 || full[1] != 0x5e || !bytes.Contains(full, []byte(q)) {
		t.Fatalf("bad bind request %x sig=%q", full, sig)
	}
	// Two row-data markers must be present after metadata for the array bind.
	if bytes.Count(full, []byte{ttcRowData}) < 2 {
		t.Fatalf("array row markers missing: %x", full)
	}
	re, err := buildTTCPreparedReexecuteRequest(77, rows[:1])
	if err != nil {
		t.Fatal(err)
	}
	if len(re) < 7 || !bytes.Equal(re[:3], []byte{3, 4, 0}) || re[len(re)-1] == 0xff {
		t.Fatalf("bad prepared request %x", re)
	}
}

func TestTTCBindDescriptorMismatchFailsClosed(t *testing.T) {
	n, _ := oracleNumberInputBind("1")
	rows := [][]oracleTTCBind{{n}, {oracleStringInputBind([]byte("1"), 873)}}
	if _, _, err := buildTTCBindStatementRequest("INSERT INTO T VALUES (:1)", 12, rows, false); err == nil {
		t.Fatal("expected descriptor mismatch")
	}
}
