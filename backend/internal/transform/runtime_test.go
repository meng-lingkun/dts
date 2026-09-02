package transform

import (
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestBooleanNormalization(t *testing.T) {
	p, err := Compile(
		[]domain.ColumnInfo{{Name: "b", DataType: "boolean"}},
		[]domain.ColumnInfo{{Name: "b", DataType: "tinyint", ColumnType: "tinyint(1)"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := p.TransformRows([][]connector.Value{{{Raw: []byte("t")}}, {{Raw: []byte("false")}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(rows[0][0].Raw) != "1" || string(rows[1][0].Raw) != "0" {
		t.Fatalf("rows=%#v", rows)
	}
}

func TestIntegerToBooleanRejectsNonBinaryInteger(t *testing.T) {
	p, _ := Compile(
		[]domain.ColumnInfo{{Name: "b", DataType: "tinyint"}},
		[]domain.ColumnInfo{{Name: "b", DataType: "boolean"}},
	)
	if _, err := p.TransformRows([][]connector.Value{{{Raw: []byte("2")}}}); err == nil {
		t.Fatal("expected unsafe boolean conversion to fail")
	}
}

func TestJSONValidationAndMaterialization(t *testing.T) {
	p, _ := Compile(
		[]domain.ColumnInfo{{Name: "j", DataType: "json"}},
		[]domain.ColumnInfo{{Name: "j", DataType: "jsonb"}},
	)
	src := []byte(` {"a":1} `)
	rows, err := p.TransformRows([][]connector.Value{{{Raw: src}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(rows[0][0].Raw) != `{"a":1}` {
		t.Fatalf("value=%q", rows[0][0].Raw)
	}
	src[1] = 'x'
	if string(rows[0][0].Raw) != `{"a":1}` {
		t.Fatal("transform output aliases source buffer")
	}
	if _, err := p.TransformRows([][]connector.Value{{{Raw: []byte(`{bad`)}}}); err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestZeroDateRequiresPolicy(t *testing.T) {
	p, _ := Compile(
		[]domain.ColumnInfo{{Name: "d", DataType: "date"}},
		[]domain.ColumnInfo{{Name: "d", DataType: "date"}},
	)
	if _, err := p.TransformRows([][]connector.Value{{{Raw: []byte("0000-00-00")}}}); err == nil {
		t.Fatal("expected zero-date rejection")
	}
}

func TestTransformRulesZeroDateAndStringPolicies(t *testing.T) {
	src := []domain.ColumnInfo{{Name: "d", DataType: "date"}, {Name: "name", DataType: "varchar"}}
	dst := []domain.ColumnInfo{{Name: "d", DataType: "date"}, {Name: "name", DataType: "varchar"}}
	p, err := CompileWithRules(src, dst, []domain.TransformRule{
		{SourceTable: "orders", Column: "d", Action: domain.TransformZeroDateToNull},
		{SourceTable: "orders", Column: "name", Action: domain.TransformTrim},
		{SourceTable: "orders", Column: "name", Action: domain.TransformUpper},
	}, "app", "orders")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := p.TransformRows([][]connector.Value{{{Raw: []byte("0000-00-00")}, {Raw: []byte("  alice  ")}}})
	if err != nil {
		t.Fatal(err)
	}
	if !rows[0][0].Null || string(rows[0][1].Raw) != "ALICE" {
		t.Fatalf("unexpected transformed row: %#v", rows[0])
	}
}

func TestTransformRulesNullReplaceJSONCompact(t *testing.T) {
	src := []domain.ColumnInfo{{Name: "v", DataType: "varchar"}, {Name: "j", DataType: "json"}}
	dst := []domain.ColumnInfo{{Name: "v", DataType: "varchar"}, {Name: "j", DataType: "jsonb"}}
	p, err := CompileWithRules(src, dst, []domain.TransformRule{
		{Column: "v", Action: domain.TransformNullToValue, Value: "unknown"},
		{Column: "v", Action: domain.TransformReplaceLiteral, Match: "known", Value: "KNOWN"},
		{Column: "j", Action: domain.TransformJSONCompact},
	}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := p.TransformRows([][]connector.Value{{{Null: true}, {Raw: []byte("{ \"a\" : 1 }")}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(rows[0][0].Raw) != "unKNOWN" {
		t.Fatalf("rules should run in declaration order, got %q", rows[0][0].Raw)
	}
	if string(rows[0][1].Raw) != `{"a":1}` {
		t.Fatalf("json was not compacted: %q", rows[0][1].Raw)
	}
}

func TestValidateRulesRejectsUnsafeOrIncompleteRules(t *testing.T) {
	if err := ValidateRules([]domain.TransformRule{{Column: "x", Action: "SQL_EXPR"}}); err == nil {
		t.Fatal("expected arbitrary action rejection")
	}
	if err := ValidateRules([]domain.TransformRule{{Column: "x", Action: domain.TransformReplaceLiteral}}); err == nil {
		t.Fatal("expected missing match rejection")
	}
}
