package postgresconnector

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestPGTargetType(t *testing.T) {
	cases := []struct {
		in   domain.ColumnInfo
		want string
	}{{domain.ColumnInfo{DataType: "int", ColumnType: "int(11)"}, "integer"}, {domain.ColumnInfo{DataType: "blob", ColumnType: "longblob"}, "bytea"}, {domain.ColumnInfo{DataType: "json", ColumnType: "json"}, "jsonb"}}
	for _, c := range cases {
		if got := pgTargetType(c.in); got != c.want {
			t.Fatalf("got %s want %s", got, c.want)
		}
	}
}
