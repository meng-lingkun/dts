//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"dts/backend/internal/connector"
	mysqlconnector "dts/backend/internal/connector/mysql"
	postgresconnector "dts/backend/internal/connector/postgres"
	"dts/backend/internal/domain"
)

func mysqlDS() domain.DataSource {
	return domain.DataSource{Type: domain.DataSourceMySQL, Host: env("DTS_E2E_MYSQL_HOST", "127.0.0.1"), Port: testPort("DTS_E2E_MYSQL_PORT", 13306), Username: "root", Password: env("DTS_E2E_MYSQL_PASSWORD", "dts"), Database: "dts_e2e"}
}
func pgDS() domain.DataSource {
	return domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: env("DTS_E2E_POSTGRES_HOST", "127.0.0.1"), Port: testPort("DTS_E2E_POSTGRES_PORT", 15432), Username: "postgres", Password: env("DTS_E2E_POSTGRES_PASSWORD", "dts"), Database: env("DTS_E2E_POSTGRES_DATABASE", "dts"), Schema: "dts_e2e"}
}

func testPort(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			panic(key + " must be a TCP port between 1 and 65535")
		}
		return port
	}
	return fallback
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func TestMySQLCompositeKeysetToPostgres(t *testing.T) {
	requireIsolated(t)
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
		"CREATE DATABASE IF NOT EXISTS `dts_e2e` CHARACTER SET utf8mb4",
		"DROP TABLE IF EXISTS `dts_e2e`.`orders`",
		"CREATE TABLE `dts_e2e`.`orders` (`tenant_id` varchar(32) NOT NULL,`id` bigint NOT NULL,`payload` varchar(64),PRIMARY KEY (`tenant_id`,`id`))",
		"INSERT INTO `dts_e2e`.`orders` VALUES ('a',1,'a1'),('a',2,'a2'),('b',1,'b1'),('c',9,'c9')",
	} {
		if err := my.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS dts_e2e",
		"DROP TABLE IF EXISTS dts_e2e.orders",
		"CREATE TABLE dts_e2e.orders (tenant_id varchar(32) NOT NULL,id bigint NOT NULL,payload varchar(64),PRIMARY KEY (tenant_id,id))",
	} {
		if err := pg.ExecSQL(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	meta, err := my.GetTableMetadata(ctx, "dts_e2e", "orders")
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
		b, err := my.ReadBatch(ctx, connector.ReadBatchRequest{Schema: "dts_e2e", Table: "orders", PrimaryKey: "tenant_id", PrimaryKeys: meta.PrimaryKeys, Columns: cols, Cursor: cursor, UseKeyset: true, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(b.Rows) == 0 {
			break
		}
		n, err := pg.WriteBatch(ctx, connector.WriteBatchRequest{Schema: "dts_e2e", Table: "orders", Columns: cols, Rows: b.Rows, PrimaryKeys: meta.PrimaryKeys})
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
		b, err := pg.ReadBatch(ctx, connector.ReadBatchRequest{Schema: "dts_e2e", Table: "orders", PrimaryKey: "tenant_id", PrimaryKeys: meta.PrimaryKeys, Columns: cols, Cursor: targetCursor, UseKeyset: true, Limit: 3})
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

func TestFilteredReadAndTextRoundTrip(t *testing.T) {
	requireIsolated(t)
	for _, name := range []string{"mysql", "postgres"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var ds = mysqlDS()
			var factory connector.Factory = mysqlconnector.NewFactory()
			mode := "SET SESSION sql_mode='NO_BACKSLASH_ESCAPES'"
			if name == "postgres" {
				ds, factory = pgDS(), postgresconnector.NewFactory()
				mode = "SET standard_conforming_strings=off"
			}
			raw, err := factory.New(ds)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			c := raw.(nativeTarget)
			schema := "dts_e2e"
			if name == "postgres" {
				if err := c.ExecSQL(ctx, "CREATE SCHEMA IF NOT EXISTS dts_e2e"); err != nil {
					t.Fatal(err)
				}
			}
			table := fmt.Sprintf("text_roundtrip_%d", time.Now().UnixNano())
			if err := c.ExecSQL(ctx, mode); err != nil {
				t.Fatal(err)
			}
			if err := c.ExecSQL(ctx, "CREATE TABLE "+schema+"."+table+" (id bigint PRIMARY KEY, label varchar(64), payload varchar(128))"); err != nil {
				t.Fatal(err)
			}
			defer c.ExecSQL(context.Background(), "DROP TABLE "+schema+"."+table)
			cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "label", DataType: "varchar"}, {Name: "payload", DataType: "varchar"}}
			payload := "订单\\2026\nO'Brien"
			rows := [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("O'Brien")}, {Raw: []byte(payload)}}, {{Raw: []byte("2")}, {Raw: []byte("other")}, {Raw: []byte("ignored")}}}
			if _, err := c.WriteBatch(ctx, connector.WriteBatchRequest{Schema: schema, Table: table, PrimaryKey: "id", Columns: cols, Rows: rows}); err != nil {
				t.Fatal(err)
			}
			got, err := c.ReadBatch(ctx, connector.ReadBatchRequest{Schema: schema, Table: table, PrimaryKey: "id", Columns: cols, StartPK: 1, EndPK: 2, CustomWhere: "label = 'O''Brien'", Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Rows) != 1 || string(got.Rows[0][2].Raw) != payload {
				t.Fatalf("unexpected filtered row or text: %+v", got)
			}
		})
	}
}
