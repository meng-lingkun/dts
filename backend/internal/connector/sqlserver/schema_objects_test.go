package sqlserverconnector

import (
	"strings"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestSQLServerSchemaObjectType(t *testing.T) {
	cases := map[string]domain.SchemaObjectType{"V": domain.SchemaObjectView, "SO": domain.SchemaObjectSequence, "TR": domain.SchemaObjectTrigger, "P": domain.SchemaObjectProcedure, "FN": domain.SchemaObjectFunction, "TF": domain.SchemaObjectFunction}
	for raw, want := range cases {
		got, ok := sqlServerSchemaObjectType(raw)
		if !ok || got != want {
			t.Fatalf("%s => %v,%v want %v", raw, got, ok, want)
		}
	}
	if _, ok := sqlServerSchemaObjectType("U"); ok {
		t.Fatal("table type must not be returned as schema object")
	}
}

func TestSQLServerSequenceDDL(t *testing.T) {
	ddl, err := sqlServerSequenceDDL("dbo", "order_seq", "bigint", "100", "5", "1", "999999", false, true, "50")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CREATE SEQUENCE [dbo].[order_seq]", "AS bigint", "START WITH 100", "INCREMENT BY 5", "MINVALUE 1", "MAXVALUE 999999", "NO CYCLE", "CACHE 50"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("DDL %q missing %q", ddl, want)
		}
	}
	if _, err := sqlServerSequenceDDL("dbo", "x", "bigint", "1;DROP TABLE x", "1", "", "", false, false, ""); err == nil {
		t.Fatal("unsafe numeric sequence literal should be rejected")
	}
}

func TestSQLServerTargetTypeMapping(t *testing.T) {
	cases := []struct {
		col  domain.ColumnInfo
		want string
	}{
		{domain.ColumnInfo{DataType: "int", ColumnType: "int unsigned"}, "bigint"},
		{domain.ColumnInfo{DataType: "bigint", ColumnType: "bigint unsigned"}, "decimal(20,0)"},
		{domain.ColumnInfo{DataType: "varchar", ColumnType: "varchar(128)"}, "nvarchar(128)"},
		{domain.ColumnInfo{DataType: "json", ColumnType: "json"}, "nvarchar(max)"},
		{domain.ColumnInfo{DataType: "blob", ColumnType: "longblob"}, "varbinary(max)"},
		{domain.ColumnInfo{DataType: "timestamp", ColumnType: "timestamp"}, "datetime2(6)"},
		{domain.ColumnInfo{DataType: "rowversion", ColumnType: "varbinary(8)"}, "varbinary(8)"},
	}
	for _, tc := range cases {
		if got := sqlServerTargetType(tc.col); got != tc.want {
			t.Fatalf("%+v => %s want %s", tc.col, got, tc.want)
		}
	}
}

func TestSQLServerIdentityClause(t *testing.T) {
	clause, ok, err := sqlServerIdentityClause(domain.ColumnInfo{Extra: "IDENTITY(100,5)"})
	if err != nil || !ok || clause != " IDENTITY(100,5)" {
		t.Fatalf("clause=%q ok=%v err=%v", clause, ok, err)
	}
	clause, ok, err = sqlServerIdentityClause(domain.ColumnInfo{Extra: "auto_increment"})
	if err != nil || !ok || clause != " IDENTITY(1,1)" {
		t.Fatalf("auto increment clause=%q ok=%v err=%v", clause, ok, err)
	}
	if _, _, err := sqlServerIdentityClause(domain.ColumnInfo{Extra: "IDENTITY(1;DROP,1)"}); err == nil {
		t.Fatal("unsafe identity literal should fail")
	}
}

func TestSQLServerExperimentalCapabilitiesIncludeSchemaObjects(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "")
	d := NewFactory().Capabilities(domain.DataSourceSQLServer)
	for _, cap := range []connector.Capability{
		connector.CapabilityMetadata,
		connector.CapabilityFullRead,
		connector.CapabilityFullWrite,
		connector.CapabilitySchemaCreate,
		connector.CapabilitySchemaObjects,
		connector.CapabilityCDCApply,
		connector.CapabilityCDCTransactional,
		connector.CapabilityDDLApply,
	} {
		if !d.Has(cap) {
			t.Fatalf("missing capability %s: %#v", cap, d.Capabilities)
		}
	}
	if d.Has(connector.CapabilityCDCRead) {
		t.Fatalf("source CDC must remain separately gated: %#v", d.Capabilities)
	}
}
