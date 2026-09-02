package postgresconnector

import (
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestPostgresNumericLiteralFailClosed(t *testing.T) {
	col := domain.ColumnInfo{Name: "n", DataType: "numeric"}
	if err := validatePostgresValue(col, connector.Value{Raw: []byte("12345678901234567890.123")}); err != nil {
		t.Fatal(err)
	}
	if err := validatePostgresValue(col, connector.Value{Raw: []byte("1;DROP TABLE x")}); err == nil {
		t.Fatal("unsafe numeric text should fail")
	}
	f := domain.ColumnInfo{Name: "f", DataType: "float8"}
	if err := validatePostgresValue(f, connector.Value{Raw: []byte("NaN")}); err != nil {
		t.Fatal(err)
	}
}
