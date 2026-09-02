package migration

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestMapTargetColumns(t *testing.T) {
	src := []domain.ColumnInfo{{Name: "id"}, {Name: "name"}}
	out, pk, err := mapTargetColumns(src, "id", []domain.ColumnMapping{{SourceColumn: "id", TargetColumn: "order_id"}, {SourceColumn: "name", TargetColumn: "title"}})
	if err != nil {
		t.Fatal(err)
	}
	if pk != "order_id" || out[0].Name != "order_id" || out[1].Name != "title" {
		t.Fatalf("%s %+v", pk, out)
	}
}
func TestMapTargetColumnsRejectsUnknown(t *testing.T) {
	_, _, err := mapTargetColumns([]domain.ColumnInfo{{Name: "id"}}, "id", []domain.ColumnMapping{{SourceColumn: "missing", TargetColumn: "x"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDefaultSchemaSeparatesPostgresDatabaseAndSchema(t *testing.T) {
	pg := domain.DataSource{Type: domain.DataSourcePostgreSQL, Database: "appdb"}
	if got := defaultSchema(pg); got != "public" {
		t.Fatalf("expected public, got %s", got)
	}
	pg.Schema = "biz"
	if got := defaultSchema(pg); got != "biz" {
		t.Fatalf("expected biz, got %s", got)
	}
	my := domain.DataSource{Type: domain.DataSourceMySQL, Database: "app"}
	if got := defaultSchema(my); got != "app" {
		t.Fatalf("expected app, got %s", got)
	}
}
