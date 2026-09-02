package mysqlconnector

import (
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestMySQLNumericLiteralFailClosed(t *testing.T) {
	col := domain.ColumnInfo{Name: "id", DataType: "bigint"}
	if err := validateMySQLValue(col, connector.Value{Raw: []byte("12345678901234567890")}); err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLValue(col, connector.Value{Raw: []byte("1 OR 1=1")}); err == nil {
		t.Fatal("unsafe numeric text should fail")
	}
}
