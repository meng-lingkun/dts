//go:build integration

package integration

import (
	"context"
	"os"
	"testing"

	"qmigration/backend/internal/connector"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	postgresconnector "qmigration/backend/internal/connector/postgres"
	"qmigration/backend/internal/domain"
)

func mysqlDS() domain.DataSource {
	return domain.DataSource{Type: domain.DataSourceMySQL, Host: env("QMIGRATION_E2E_MYSQL_HOST", "127.0.0.1"), Port: 13306, Username: "root", Password: env("QMIGRATION_E2E_MYSQL_PASSWORD", "qmigration"), Database: "qmigration_e2e"}
}
func pgDS() domain.DataSource {
	return domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: env("QMIGRATION_E2E_POSTGRES_HOST", "127.0.0.1"), Port: 15432, Username: "postgres", Password: env("QMIGRATION_E2E_POSTGRES_PASSWORD", "qmigration"), Database: "qmigration", Schema: "qmigration_e2e"}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func TestMySQLCompositeKeysetToPostgres(t *testing.T) {
	ctx := context.Background()
	mr, err := mysqlconnector.NewFactory().New(mysqlDS())
	if err != nil {
		t.Fatal(err)
	}
	my := mr.(*mysqlconnector.Connector)
	defer my.Close()
	pr, err := postgresconnector.NewFactory().New(pgDS())
	if err != nil {
		t.Fatal(err)
	}
	pg := pr.(*postgresconnector.Connector)
	defer pg.Close()
	if err := my.TestConnection(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pg.TestConnection(ctx); err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS `qmigration_e2e` CHARACTER SET utf8mb4",
		"DROP TABLE IF EXISTS `qmigration_e2e`.`orders`",
		"CREATE TABLE `qmigration_e2e`.`orders` (`tenant_id` varchar(32) NOT NULL,`id` bigint NOT NULL,`payload` varchar(64),PRIMARY KEY (`tenant_id`,`id`))",
		"INSERT INTO `qmigration_e2e`.`orders` VALUES ('a',1,'a1'),('a',2,'a2'),('b',1,'b1'),('c',9,'c9')",
	} {
		if err := my.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS qmigration_e2e",
		"DROP TABLE IF EXISTS qmigration_e2e.orders",
		"CREATE TABLE qmigration_e2e.orders (tenant_id varchar(32) NOT NULL,id bigint NOT NULL,payload varchar(64),PRIMARY KEY (tenant_id,id))",
	} {
		if err := pg.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	meta, err := my.GetTableMetadata(ctx, "qmigration_e2e", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.PrimaryKeys) != 2 || meta.PrimaryKeys[0] != "tenant_id" || meta.PrimaryKeys[1] != "id" {
		t.Fatalf("unexpected PK metadata: %+v", meta.PrimaryKeys)
	}
	cols := meta.Columns
	var cursor []connector.Value
	var moved int64
	for {
		b, err := my.ReadBatch(ctx, connector.ReadBatchRequest{Schema: "qmigration_e2e", Table: "orders", PrimaryKey: "tenant_id", PrimaryKeys: meta.PrimaryKeys, Columns: cols, Cursor: cursor, UseKeyset: true, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(b.Rows) == 0 {
			break
		}
		n, err := pg.WriteBatch(ctx, connector.WriteBatchRequest{Schema: "qmigration_e2e", Table: "orders", Columns: cols, Rows: b.Rows, PrimaryKeys: meta.PrimaryKeys})
		if err != nil {
			t.Fatal(err)
		}
		moved += n
		cursor = b.LastKey
	}
	if moved != 4 {
		t.Fatalf("moved=%d", moved)
	}
	var targetCursor []connector.Value
	var got int
	for {
		b, err := pg.ReadBatch(ctx, connector.ReadBatchRequest{Schema: "qmigration_e2e", Table: "orders", PrimaryKey: "tenant_id", PrimaryKeys: meta.PrimaryKeys, Columns: cols, Cursor: targetCursor, UseKeyset: true, Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		if len(b.Rows) == 0 {
			break
		}
		got += len(b.Rows)
		targetCursor = b.LastKey
	}
	if got != 4 {
		t.Fatalf("target rows=%d", got)
	}
}
