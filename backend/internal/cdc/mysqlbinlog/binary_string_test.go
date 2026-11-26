package mysqlbinlog

import (
	"encoding/base64"
	"testing"

	"dts/backend/internal/domain"
)

func TestBinaryStringTypesSurviveCDCSerialization(t *testing.T) {
	raw := []byte{0xff, 0, 0x80}
	encoded := append([]byte{byte(len(raw))}, raw...)
	for _, tc := range []struct {
		typ      byte
		dataType string
	}{{TypeVarchar, "varbinary"}, {TypeVarString, "varbinary"}, {TypeString, "binary"}} {
		f, n, err := decodeValue(encoded, tc.typ, 16, domain.ColumnInfo{Name: "v", DataType: tc.dataType}, nil)
		if err != nil || n != len(encoded) || f.Encoding != "base64" || f.Value != base64.StdEncoding.EncodeToString(raw) {
			t.Errorf("%s: field=%+v n=%d err=%v", tc.dataType, f, n, err)
		}
	}
	if _, _, err := decodeValue(encoded, TypeVarchar, 16, domain.ColumnInfo{Name: "v", DataType: "varchar"}, nil); err == nil {
		t.Fatal("non-UTF-8 text would become replacement characters in JSON")
	}
}

func TestMemberLabelsPreserveLiteralValues(t *testing.T) {
	def := "enum('2','1','it''s,ok','x\\\\y','line\\nbreak','')"
	labels, err := parseMemberLabels(def, "enum")
	if err != nil {
		t.Fatal(err)
	}
	members := memberCache{def: labels, "set('a','b','c')": {"a", "b", "c"}, "set('a')": {"a"}}
	for i, want := range []string{"2", "1", "it's,ok", "x\\y", "line\nbreak", ""} {
		got, err := members.value("enum", def, uint64(i+1))
		if err != nil || got != want {
			t.Fatalf("index %d: %q, %v", i+1, got, err)
		}
	}
	got, err := members.value("set", "set('a','b','c')", 5)
	if err != nil || got != "a,c" {
		t.Fatalf("SET: %q, %v", got, err)
	}
	if _, err := members.value("enum", def, 0); err == nil {
		t.Fatal("silently projected invalid ENUM index zero")
	}
	if _, err := members.value("set", "set('a')", 2); err == nil {
		t.Fatal("accepted out-of-range set mask")
	}
}

func TestHistoricalEnumNeedsOriginalLabels(t *testing.T) {
	tm := &TableMap{ColumnTypes: []byte{TypeString}, ColumnMeta: []byte{TypeEnum, 1}}
	types, _ := ColumnMetadata(tm)
	col := []domain.ColumnInfo{{Name: "c", DataType: "enum", ColumnType: "enum('B','A')"}}
	if _, err := eventMemberLabels(tm, types, col); err == nil {
		t.Fatal("used current dictionary for a historical row without recorded labels")
	}
	tm.optionalMetadata = []byte{6, 5, 2, 1, 'A', 1, 'B'}
	if _, err := eventMemberLabels(tm, types, col); err == nil {
		t.Fatal("accepted reordered source dictionary")
	}
	col[0].ColumnType = "enum('A','B')"
	members, err := eventMemberLabels(tm, types, col)
	if err != nil || members["c"][0] != "A" {
		t.Fatalf("matching historical metadata rejected: %+v %v", members, err)
	}
}
