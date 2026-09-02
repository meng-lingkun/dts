package schema

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestConvertColumnsCrossFamily(t *testing.T) {
	src := []domain.ColumnInfo{{Name: "id", DataType: "bigint", ColumnType: "bigint", Nullable: false}, {Name: "payload", DataType: "json", ColumnType: "json", Nullable: true}, {Name: "amount", DataType: "decimal", ColumnType: "decimal(18,4)"}}
	pg, _ := ConvertColumns(src, domain.DataSourcePostgreSQL)
	if pg[0].DataType != "bigint" || pg[1].DataType != "jsonb" || pg[2].DataType != "numeric(18,4)" {
		t.Fatalf("unexpected pg conversion: %#v", pg)
	}
	my, _ := ConvertColumns([]domain.ColumnInfo{{Name: "u", DataType: "uuid", ColumnType: "uuid"}}, domain.DataSourceMySQL)
	if my[0].DataType != "char(36)" {
		t.Fatalf("unexpected mysql uuid: %#v", my)
	}
}

func TestConvertColumnsOracleAndSQLServerTargets(t *testing.T) {
	src := []domain.ColumnInfo{
		{Name: "id", DataType: "bigint", ColumnType: "bigint"},
		{Name: "payload", DataType: "json", ColumnType: "json"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(18,4)"},
		{Name: "u", DataType: "uuid", ColumnType: "uuid"},
	}
	ora, _ := ConvertColumns(src, domain.DataSourceOracle)
	if ora[0].DataType != "number(19)" || ora[1].DataType != "clob" || ora[2].DataType != "number(18,4)" || ora[3].DataType != "varchar2(36)" {
		t.Fatalf("oracle mapping=%#v", ora)
	}
	ms, _ := ConvertColumns(src, domain.DataSourceSQLServer)
	if ms[0].DataType != "bigint" || ms[1].DataType != "nvarchar(max)" || ms[2].DataType != "decimal(18,4)" || ms[3].DataType != "uniqueidentifier" {
		t.Fatalf("sqlserver mapping=%#v", ms)
	}
}

func TestConvertColumnsGBase8aBounds(t *testing.T) {
	src := []domain.ColumnInfo{
		{Name: "short_text", DataType: "varchar", ColumnType: "varchar(8000)"},
		{Name: "long_text", DataType: "varchar", ColumnType: "varchar(20000)"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(80,40)"},
		{Name: "payload", DataType: "bytea", ColumnType: "bytea"},
		{Name: "doc", DataType: "jsonb", ColumnType: "jsonb"},
		{Name: "u", DataType: "uuid", ColumnType: "uuid"},
	}
	got, _ := ConvertColumns(src, domain.DataSourceGBase)
	want := []string{"varchar(8000)", "longtext", "decimal(65,30)", "longblob", "longtext", "varchar(36)"}
	if len(got) != len(want) {
		t.Fatalf("gbase mappings=%#v", got)
	}
	for i := range want {
		if got[i].DataType != want[i] {
			t.Fatalf("gbase mapping[%d]=%q want %q; all=%#v", i, got[i].DataType, want[i], got)
		}
	}
}

func TestConvertColumnsGBase8sBounds(t *testing.T) {
	src := []domain.ColumnInfo{
		{Name: "short_text", DataType: "varchar", ColumnType: "varchar(128)"},
		{Name: "long_text", DataType: "varchar", ColumnType: "varchar(50000)"},
		{Name: "amount", DataType: "decimal", ColumnType: "decimal(80,40)"},
		{Name: "payload", DataType: "bytea", ColumnType: "bytea"},
		{Name: "doc", DataType: "jsonb", ColumnType: "jsonb"},
		{Name: "u", DataType: "uuid", ColumnType: "uuid"},
	}
	got, _ := ConvertColumns(src, domain.DataSourceGBase8s)
	want := []string{"varchar(128)", "lvarchar(32739)", "decimal(32,32)", "blob", "clob", "varchar(36)"}
	for i := range want {
		if got[i].DataType != want[i] {
			t.Fatalf("gbase8s mapping[%d]=%q want=%q all=%#v", i, got[i].DataType, want[i], got)
		}
	}
}
