package postgresconnector

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

func TestPostgresNumericLiteralFailClosed(t *testing.T) {
	col := domain.ColumnInfo{Name: "n", DataType: "numeric"}
	if err := validatePostgresValue(col, connector.Value{Raw: []byte("12345678901234567890.123")}); err != nil {
		t.Fatal(err)
	}
	if err := validatePostgresValue(col, connector.Value{Raw: []byte("1;DROP TABLE x")}); err == nil {
		t.Fatal("unsafe numeric text should fail")
	}
	for _, dt := range []string{"numeric", "decimal", "real", "double precision", "float8"} {
		for _, special := range []string{"NaN", "Infinity", "-Infinity"} {
			col := domain.ColumnInfo{Name: "f", DataType: dt}
			val := connector.Value{Raw: []byte(special)}
			if err := validatePostgresValue(col, val); err != nil {
				t.Fatal(err)
			}
			want := "'" + special + "'"
			if postgresValueLiteral(val, col) != want || pgValue(col, val) != want {
				t.Errorf("%s requires a quoted literal for %s", dt, special)
			}
		}
	}
}
