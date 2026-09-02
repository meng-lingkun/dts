package schema

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestNormalizeType(t *testing.T) {
	cases := []struct {
		in   domain.ColumnInfo
		want domain.UniversalDataType
	}{{domain.ColumnInfo{Name: "id", DataType: "bigint", ColumnType: "bigint"}, domain.UniversalBigInt}, {domain.ColumnInfo{Name: "n", DataType: "numeric", ColumnType: "numeric(18,4)"}, domain.UniversalDecimal}, {domain.ColumnInfo{Name: "j", DataType: "jsonb"}, domain.UniversalJSON}, {domain.ColumnInfo{Name: "b", DataType: "bytea"}, domain.UniversalBinary}}
	for _, c := range cases {
		got := NormalizeType(c.in)
		if got.Type != c.want {
			t.Fatalf("%s got %s want %s", c.in.Name, got.Type, c.want)
		}
	}
}
