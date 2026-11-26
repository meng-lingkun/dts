package mysqlconnector

import (
	"context"
	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"strings"
	"testing"
)

func TestDeleteByKeyRejectsMalformedNumericKeysBeforeConnecting(t *testing.T) {
	col := domain.ColumnInfo{Name: "id", DataType: "bigint"}
	bad := connector.Value{Raw: []byte("invalid numeric key")}
	for _, req := range []connector.DeleteByKeyRequest{
		{PrimaryKey: "id", Column: col, Value: bad},
		{PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{col}, Values: []connector.Value{bad}},
	} {
		if err := (&Connector{}).DeleteByKey(context.Background(), req); err == nil || !strings.Contains(err.Error(), "delete key") {
			t.Fatalf("wanted a key error before I/O, got %v", err)
		}
	}
}

func TestReadBatchRejectsMalformedNumericBoundsBeforeConnecting(t *testing.T) {
	bad := []connector.Value{{Raw: []byte("invalid numeric key")}}
	base := connector.ReadBatchRequest{UseKeyset: true, PrimaryKey: "id", Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}}}
	for _, field := range []string{"cursor", "lower", "upper"} {
		req := base
		switch field {
		case "cursor":
			req.Cursor = bad
		case "lower":
			req.Cursor = []connector.Value{{Raw: []byte("4")}}
			req.LowerBound = bad
		case "upper":
			req.UpperBound = bad
		}
		if _, err := (&Connector{}).ReadBatch(context.Background(), req); err == nil || !strings.Contains(err.Error(), "keyset bound") {
			t.Fatalf("%s: wanted a bound error before I/O, got %v", field, err)
		}
	}
}

func TestMySQLNumericLiteralFailClosed(t *testing.T) {
	col := domain.ColumnInfo{Name: "id", DataType: "bigint"}
	if err := validateMySQLValue(col, connector.Value{Raw: []byte("12345678901234567890")}); err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLValue(col, connector.Value{Raw: []byte("1 OR 1=1")}); err == nil {
		t.Fatal("unsafe numeric text should fail")
	}
}
