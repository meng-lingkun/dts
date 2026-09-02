package mysqlconnector

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func serveTwoColumns(c net.Conn, left, right string) error {
	if err := writeTestPacket(c, 1, []byte{2}); err != nil {
		return err
	}
	if err := writeTestPacket(c, 2, columnDef("Table")); err != nil {
		return err
	}
	if err := writeTestPacket(c, 3, columnDef("Create Table")); err != nil {
		return err
	}
	if err := writeTestPacket(c, 4, []byte{0xfe, 0, 0, 2, 0}); err != nil {
		return err
	}
	row := append(lenStr(left), lenStr(right)...)
	if err := writeTestPacket(c, 5, row); err != nil {
		return err
	}
	return writeTestPacket(c, 6, []byte{0xfe, 0, 0, 2, 0})
}

func runFakeGBaseExecWithDDL(t *testing.T, showDDL string) (string, int, <-chan []string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	queries := make(chan []string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if writeTestPacket(c, 0, testHandshakePayload()) != nil {
			return
		}
		if _, _, err = readTestPacket(c); err != nil {
			return
		}
		if writeTestPacket(c, 2, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
			return
		}
		got := []string{}
		for {
			_, pkt, err := readTestPacket(c)
			if err != nil {
				queries <- got
				return
			}
			if len(pkt) == 0 || pkt[0] != 0x03 {
				queries <- got
				return
			}
			q := string(pkt[1:])
			got = append(got, q)
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(q)), "SHOW CREATE TABLE") {
				if serveTwoColumns(c, "orders", showDDL) != nil {
					queries <- got
					return
				}
				continue
			}
			if writeTestPacket(c, 1, []byte{0, 0, 0, 2, 0, 0, 0}) != nil {
				queries <- got
				return
			}
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	stop := func() { _ = ln.Close(); <-done }
	return "127.0.0.1", addr.Port, queries, stop
}

func runFakeGBaseExec(t *testing.T) (string, int, <-chan []string, func()) {
	t.Helper()
	ddl := "CREATE TABLE `orders` (`id` bigint NOT NULL,`name` varchar(100) NULL,PRIMARY KEY (`id`)) ENGINE=EXPRESS DISTRIBUTED BY('id') DEFAULT CHARSET=utf8mb4"
	return runFakeGBaseExecWithDDL(t, ddl)
}

func TestGBaseTargetUsesExpressAndIdempotentMerge(t *testing.T) {
	host, port, queryCh, stop := runFakeGBaseExec(t)
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceGBase, Host: host, Port: port, Username: "gbase", Password: "secret", Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	c := cRaw.(*Connector)
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", Nullable: false}, {Name: "name", DataType: "varchar", ColumnType: "varchar(100)", Nullable: true}}
	if err := c.CreateTableWithPrimaryKeys(context.Background(), "app", "orders", cols, []string{"id"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteBatch(context.Background(), connector.WriteBatchRequest{Schema: "app", Table: "orders", Columns: cols, PrimaryKeys: []string{"id"}, Rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("alpha")}}}}); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	stop()
	got := <-queryCh
	joined := strings.Join(got, "\n")
	for _, want := range []string{
		"SET NAMES utf8mb4",
		"ENGINE=EXPRESS DISTRIBUTED BY('id') DEFAULT CHARSET=utf8mb4",
		"SHOW CREATE TABLE `app`.`orders`",
		"CREATE TABLE `app`.`_qm_",
		" LIKE `app`.`orders`",
		"INSERT INTO `app`.`_qm_",
		"MERGE INTO `app`.`orders` qm_t USING `app`.`_qm_",
		"qm_t.`id`=qm_s.`id`",
		"WHEN MATCHED THEN UPDATE SET qm_t.`name`=qm_s.`name`",
		"WHEN NOT MATCHED THEN INSERT (`id`,`name`) VALUES (qm_s.`id`,qm_s.`name`)",
		"DROP TABLE IF EXISTS `app`.`_qm_",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("queries missing %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "ENGINE=InnoDB") || strings.Contains(joined, "ON DUPLICATE KEY") {
		t.Fatalf("GBase auto-target leaked MySQL semantics:\n%s", joined)
	}
}

func TestGBaseTargetRejectsKeylessReplay(t *testing.T) {
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceGBase}}
	_, err := c.WriteBatch(context.Background(), connector.WriteBatchRequest{Schema: "app", Table: "t", Columns: []domain.ColumnInfo{{Name: "v", DataType: "int"}}, Rows: [][]connector.Value{{{Raw: []byte("1")}}}})
	if err == nil || !strings.Contains(err.Error(), "requires a primary/migration key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGBaseTargetTypeBounds(t *testing.T) {
	cases := []struct {
		col  domain.ColumnInfo
		want string
	}{
		{domain.ColumnInfo{DataType: "bytea"}, "longblob"},
		{domain.ColumnInfo{DataType: "jsonb"}, "longtext"},
		{domain.ColumnInfo{DataType: "timestamp with time zone"}, "datetime"},
		{domain.ColumnInfo{DataType: "numeric", ColumnType: "numeric(20,4)"}, "decimal(20,4)"},
		{domain.ColumnInfo{DataType: "numeric", ColumnType: "numeric(80,40)"}, "decimal(65,30)"},
		{domain.ColumnInfo{DataType: "varchar", ColumnType: "varchar(8191)"}, "varchar(8191)"},
		{domain.ColumnInfo{DataType: "varchar", ColumnType: "varchar(8192)"}, "longtext"},
		{domain.ColumnInfo{DataType: "uuid", ColumnType: "uuid"}, "varchar(36)"},
	}
	for _, tc := range cases {
		if got := gbaseTargetType(tc.col); got != tc.want {
			t.Fatalf("gbaseTargetType(%+v)=%q want %q", tc.col, got, tc.want)
		}
	}
}

func TestGBaseChooseDistributionKeyFromCompositeMigrationKey(t *testing.T) {
	cols := []domain.ColumnInfo{
		{Name: "event_time", DataType: "timestamp", Nullable: false},
		{Name: "tenant_id", DataType: "bigint", Nullable: false},
		{Name: "payload", DataType: "text", Nullable: true},
	}
	got, err := gbaseChooseDistributionKeys(cols, []string{"event_time", "tenant_id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "tenant_id" {
		t.Fatalf("distribution keys=%v", got)
	}
}

func TestGBaseChooseDistributionKeyFailsWhenMigrationKeyNotHashable(t *testing.T) {
	cols := []domain.ColumnInfo{{Name: "event_time", DataType: "timestamp", Nullable: false}}
	_, err := gbaseChooseDistributionKeys(cols, []string{"event_time"})
	if err == nil || !strings.Contains(err.Error(), "pre-create a HASH target") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGBaseDistributionDDLParser(t *testing.T) {
	cases := []struct {
		ddl  string
		kind string
		cols []string
	}{
		{"CREATE TABLE t(a int) ENGINE=EXPRESS DISTRIBUTED BY('a') DEFAULT CHARSET=utf8", "hash", []string{"a"}},
		{"CREATE TABLE t(a int,b int) DISTRIBUTED BY HASH(a,b)", "hash", []string{"a", "b"}},
		{"CREATE TABLE t(a int) REPLICATED", "replicated", nil},
		{"CREATE TABLE t(a int) ENGINE=EXPRESS", "random", nil},
	}
	for _, tc := range cases {
		kind, cols := gbaseDistributionFromDDL(tc.ddl)
		if kind != tc.kind || strings.Join(cols, ",") != strings.Join(tc.cols, ",") {
			t.Fatalf("ddl=%q kind=%s cols=%v", tc.ddl, kind, cols)
		}
	}
}

func TestGBaseWriteRejectsRandomDistributionTarget(t *testing.T) {
	host, port, queryCh, stop := runFakeGBaseExecWithDDL(t, "CREATE TABLE `orders` (`id` bigint NOT NULL) ENGINE=EXPRESS DEFAULT CHARSET=utf8mb4")
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceGBase, Host: host, Port: port, Username: "gbase", Password: "secret", Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	c := cRaw.(*Connector)
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", Nullable: false}}
	_, err = c.WriteBatch(context.Background(), connector.WriteBatchRequest{Schema: "app", Table: "orders", Columns: cols, PrimaryKeys: []string{"id"}, Rows: [][]connector.Value{{{Raw: []byte("1")}}}})
	if err == nil || !strings.Contains(err.Error(), "requires a HASH-distributed target") {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c.Close()
	stop()
	got := <-queryCh
	for _, q := range got {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(q)), "MERGE ") || strings.Contains(q, "_qm_") {
			t.Fatalf("unsafe target should fail before staging/MERGE: %v", got)
		}
	}
}

func TestGBaseWriteRejectsHashColumnOutsideMigrationKey(t *testing.T) {
	host, port, queryCh, stop := runFakeGBaseExecWithDDL(t, "CREATE TABLE `orders` (`id` bigint NOT NULL,`tenant_id` bigint NOT NULL) ENGINE=EXPRESS DISTRIBUTED BY('tenant_id') DEFAULT CHARSET=utf8mb4")
	cRaw, err := NewFactory().New(domain.DataSource{Type: domain.DataSourceGBase, Host: host, Port: port, Username: "gbase", Password: "secret", Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	c := cRaw.(*Connector)
	cols := []domain.ColumnInfo{{Name: "id", DataType: "bigint", Nullable: false}, {Name: "tenant_id", DataType: "bigint", Nullable: false}}
	_, err = c.WriteBatch(context.Background(), connector.WriteBatchRequest{Schema: "app", Table: "orders", Columns: cols, PrimaryKeys: []string{"id"}, Rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("9")}}}})
	if err == nil || !strings.Contains(err.Error(), "distribution column tenant_id is not part of migration key") {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = c.Close()
	stop()
	_ = <-queryCh
}
