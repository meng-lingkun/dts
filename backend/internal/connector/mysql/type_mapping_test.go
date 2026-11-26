package mysqlconnector

import (
	"dts/backend/internal/domain"
	"testing"
)

func TestMySQLTargetType(t *testing.T) {
	cases := []struct {
		in   domain.ColumnInfo
		want string
	}{{domain.ColumnInfo{DataType: "integer", ColumnType: "integer"}, "int"}, {domain.ColumnInfo{DataType: "bytea", ColumnType: "bytea"}, "longblob"}, {domain.ColumnInfo{DataType: "uuid", ColumnType: "uuid"}, "char(36)"}}
	for _, c := range cases {
		if got := mysqlTargetType(c.in); got != c.want {
			t.Fatalf("got %s want %s", got, c.want)
		}
	}
}

func TestMySQLTargetPreservesMemberDefinitions(t *testing.T) {
	for _, col := range []domain.ColumnInfo{
		{DataType: "enum", ColumnType: "enum('CASE','character varying')"},
		{DataType: "set", ColumnType: "set('First','Second')"},
	} {
		if got := mysqlTargetType(col); got != col.ColumnType {
			t.Fatalf("column definition %q changed to %q", col.ColumnType, got)
		}
	}
}
