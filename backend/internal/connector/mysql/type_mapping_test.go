package mysqlconnector

import (
	"qmigration/backend/internal/domain"
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
