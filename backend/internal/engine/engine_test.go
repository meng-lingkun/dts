package engine

import (
	"context"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
)

func fixtures() (*domain.MigrationTask, domain.DataSource, domain.DataSource, []domain.MigrationTable) {
	task := &domain.MigrationTask{ID: "m1", Mode: domain.ModeFull, Parallelism: 4, ChunkRows: 1000, BatchRows: 100}
	src := domain.DataSource{Type: domain.DataSourceMySQL, Host: "src", Port: 3306, Username: "u", Password: "p", Database: "app"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527, Username: "u2", Password: "p2", Database: "app2"}
	tables := []domain.MigrationTable{{SourceSchema: "app", SourceTable: "orders", TargetSchema: "app2", TargetTable: "orders", PrimaryKey: "id", TargetPrimaryKey: "id", Columns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}, TargetColumns: []domain.ColumnInfo{{Name: "id", DataType: "bigint"}, {Name: "name", DataType: "varchar"}}}}
	return task, src, dst, tables
}

func TestUnifiedFullRender(t *testing.T) {
	task, s, d, tables := fixtures()
	cfg, err := NewUnified().Render(context.Background(), task, s, d, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 0 {
		t.Fatalf("FULL must stay inside QMigration data plane: %+v", cfg)
	}
	for _, marker := range []string{"chunk-pipeline", "bounded-channel-backpressure", "durable-checkpoint"} {
		if !strings.Contains(cfg.Content, marker) {
			t.Fatalf("unified plan missing %q: %s", marker, cfg.Content)
		}
	}
}

func TestUnifiedMySQLCDCRender(t *testing.T) {
	task, s, d, tables := fixtures()
	s.Type = domain.DataSourcePolarDBMySQL
	s.TLSMode = domain.TLSModeRequired
	s.TLSServerName = "mysql.internal"
	s.TLSCACert = "ca-pem"
	s.TLSClientCert = "cert-pem"
	s.TLSClientKey = "key-pem"
	task.Mode = domain.ModeIncremental
	task.CDCStartPositionType = "BINLOG"
	task.CDCStartPositionValue = "mysql-bin.000123:4567"
	cfg, err := NewUnified().Render(context.Background(), task, s, d, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-mysql-cdc" {
		t.Fatalf("unexpected unified MySQL CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_MYSQL_START_FILE"] != "mysql-bin.000123" || cfg.Env["QMIGRATION_MYSQL_START_POS"] != "4567" {
		t.Fatalf("bad MySQL start config: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_MYSQL_SOURCE_TYPE"] != string(domain.DataSourcePolarDBMySQL) || cfg.Env["QMIGRATION_MYSQL_TLS_MODE"] != "REQUIRED" || cfg.Env["QMIGRATION_MYSQL_TLS_SERVER_NAME"] != "mysql.internal" || cfg.Env["QMIGRATION_MYSQL_TLS_CA"] != "ca-pem" || cfg.Env["QMIGRATION_MYSQL_TLS_CLIENT_CERT"] != "cert-pem" || cfg.Env["QMIGRATION_MYSQL_TLS_CLIENT_KEY"] != "key-pem" {
		t.Fatalf("MySQL CDC must inherit datasource TLS/source-type configuration: %+v", cfg.Env)
	}
}

func TestUnifiedOceanBaseBinlogCDCRender(t *testing.T) {
	task := &domain.MigrationTask{ID: "ob-task", Mode: domain.ModeIncremental, CDCStartPositionType: "GTID", CDCStartPositionValue: "0f5addad-9be6-11ef-9c9d-00163e0808cc:1-9"}
	src := domain.DataSource{Type: domain.DataSourceOceanBase, Host: "ob-sql", Port: 2883, Username: "root@tenant", Password: "secret", Database: "app", CDCURL: "obbinlogs://odp-binlog:2883?server_name=odp.example&fallback=odp2:2883", TLSCACert: "ca", TLSClientCert: "cert", TLSClientKey: "key"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527}
	tables := []domain.MigrationTable{{SourceSchema: "app", SourceTable: "orders"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-mysql-cdc" {
		t.Fatalf("bad OceanBase CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_MYSQL_HOST"] != "odp-binlog" || cfg.Env["QMIGRATION_MYSQL_PORT"] != "2883" {
		t.Fatalf("OceanBase CDC must use cdc_url ODP endpoint: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_MYSQL_START_GTID"] != task.CDCStartPositionValue || cfg.Env["QMIGRATION_MYSQL_SOURCE_TYPE"] != string(domain.DataSourceOceanBase) {
		t.Fatalf("bad OceanBase GTID/source config: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_MYSQL_TLS_MODE"] != "REQUIRED" || cfg.Env["QMIGRATION_MYSQL_TLS_SERVER_NAME"] != "odp.example" {
		t.Fatalf("obbinlogs must enforce CDC endpoint TLS: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_MYSQL_FAILOVER_ENDPOINTS"] != "odp2:2883" {
		t.Fatalf("OceanBase CDC failover endpoints not propagated: %+v", cfg.Env)
	}
}

func TestUnifiedPostgresCDCRender(t *testing.T) {
	task := &domain.MigrationTask{ID: "pg-task", Mode: domain.ModeIncremental, CDCStartPositionType: "LSN", CDCStartPositionValue: "0/16B6C50", CDCStartResource: "qmigration_pg_task"}
	src := domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: "pg", Port: 5432, Username: "repl", Password: "secret", Database: "app"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBPostgreSQL, Host: "dst", Port: 5432, Database: "app2"}
	tables := []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders"}, {SourceSchema: "sales", SourceTable: "items"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-postgres-cdc" || cfg.Env["QMIGRATION_PG_SLOT"] != "qmigration_pg_task" {
		t.Fatalf("bad unified PostgreSQL CDC config %+v", cfg)
	}
}

func TestUnifiedOpenGaussCDCRender(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC", "1")
	task := &domain.MigrationTask{ID: "og", Mode: domain.ModeIncremental, CDCStartPositionType: "OPENGAUSS_LSN", CDCStartPositionValue: "0/16B6C50", CDCStartResource: "qmigration_og"}
	src := domain.DataSource{Type: domain.DataSourceOpenGauss, Host: "og", Port: 5432, Username: "repl", Password: "secret", Database: "app", TLSMode: domain.TLSModeRequired, TLSServerName: "og.internal"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527}
	tables := []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders"}, {SourceSchema: "sales", SourceTable: "items"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-opengauss-cdc" {
		t.Fatalf("bad openGauss CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_OPENGAUSS_SLOT"] != "qmigration_og" || cfg.Env["QMIGRATION_OPENGAUSS_START_LSN"] != "0/16B6C50" || cfg.Env["QMIGRATION_OPENGAUSS_TLS_MODE"] != "REQUIRED" || cfg.Env["QMIGRATION_OPENGAUSS_TLS_SERVER_NAME"] != "og.internal" {
		t.Fatalf("bad openGauss CDC env: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC"] != "1" {
		t.Fatalf("openGauss CDC gate missing: %+v", cfg.Env)
	}
}

func TestUnifiedKingbaseCDCRender(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC", "1")
	task := &domain.MigrationTask{ID: "kb-task", Mode: domain.ModeIncremental, CDCStartPositionType: "KINGBASE_LSN", CDCStartPositionValue: "0/16B6C50", CDCStartResource: "qmigration_kb"}
	src := domain.DataSource{Type: domain.DataSourceKingbase, Host: "kingbase", Port: 54321, Username: "repl", Password: "secret", Database: "app", TLSMode: domain.TLSModeRequired, TLSServerName: "kb.internal"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527}
	tables := []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-postgres-cdc" {
		t.Fatalf("bad Kingbase CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_PG_SOURCE_TYPE"] != string(domain.DataSourceKingbase) || cfg.Env["QMIGRATION_PG_SLOT"] != "qmigration_kb" || cfg.Env["QMIGRATION_PG_START_LSN"] != "0/16B6C50" {
		t.Fatalf("bad Kingbase CDC env: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC"] != "1" || cfg.Env["QMIGRATION_PG_TLS_MODE"] != "REQUIRED" || cfg.Env["QMIGRATION_PG_TLS_SERVER_NAME"] != "kb.internal" {
		t.Fatalf("Kingbase gate/TLS config missing: %+v", cfg.Env)
	}
	if !strings.Contains(cfg.Env["QMIGRATION_PG_PUBLICATION"], "qmigration_pub_kb_task") {
		t.Fatalf("unexpected Kingbase publication: %+v", cfg.Env)
	}
}

func TestUnifiedGaussDBCDCRender(t *testing.T) {
	task := &domain.MigrationTask{ID: "gauss", Mode: domain.ModeIncremental, CDCStartPositionType: "GAUSSDB_LSN", CDCStartPositionValue: "0/16B6C50", CDCStartResource: "qmigration_gauss"}
	src := domain.DataSource{Type: domain.DataSourceGaussDB, Host: "gauss", Port: 8000, Username: "repl", Password: "secret", Database: "app", TLSMode: domain.TLSModeRequired, TLSServerName: "gauss.internal"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527}
	tables := []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders"}, {SourceSchema: "sales", SourceTable: "items"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-gaussdb-cdc" {
		t.Fatalf("bad GaussDB CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_GAUSSDB_SLOT"] != "qmigration_gauss" || cfg.Env["QMIGRATION_GAUSSDB_START_LSN"] != "0/16B6C50" || cfg.Env["QMIGRATION_GAUSSDB_TLS_MODE"] != "REQUIRED" || cfg.Env["QMIGRATION_GAUSSDB_TLS_SERVER_NAME"] != "gauss.internal" {
		t.Fatalf("bad GaussDB CDC env: %+v", cfg.Env)
	}
	if cfg.Env["QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE"] != "1" || cfg.Env["QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC"] != "1" {
		t.Fatalf("GaussDB CDC gates missing from runtime: %+v", cfg.Env)
	}
}

func TestUnifiedGaussDBDDLRenderRequiresExplicitSourcePolicy(t *testing.T) {
	task := &domain.MigrationTask{ID: "gauss-ddl", Mode: domain.ModeIncremental, CDCStartPositionType: "GAUSSDB_LSN", CDCStartPositionValue: "0/16B6C50", CDCStartResource: "qmigration_gauss_ddl", CDCDDLMode: "SAME_FAMILY"}
	src := domain.DataSource{Type: domain.DataSourceGaussDB, Host: "gauss", Port: 8000, Username: "repl", Password: "secret", Database: "app"}
	dst := domain.DataSource{Type: domain.DataSourceGaussDB, Host: "gauss-target", Port: 8000, Username: "repl", Password: "secret", Database: "app"}
	tables := []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders", TargetSchema: "public", TargetTable: "orders"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Env["QMIGRATION_GAUSSDB_DDL_REPLAY"] != "1" {
		t.Fatalf("GaussDB SAME_FAMILY DDL did not enable reader DDL mode: %+v", cfg.Env)
	}
	if _, ok := cfg.Env["QMIGRATION_GAUSSDB_DDL_ONLY_TRANSACTIONS"]; ok {
		t.Fatalf("legacy DDL-only acknowledgement must not be required after hybrid reconstruction: %+v", cfg.Env)
	}
	if !strings.Contains(cfg.Content, "hybrid DDL/DML reconstruction") {
		t.Fatalf("runtime plan does not document hybrid DDL boundary: %s", cfg.Content)
	}
}

func TestUnifiedSQLServerCDCRender(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "1")
	task := &domain.MigrationTask{ID: "mssql-task", Mode: domain.ModeIncremental, CDCStartPositionType: "SQLSERVER_LSN", CDCStartPositionValue: "0x00000000000000000001"}
	src := domain.DataSource{Type: domain.DataSourceSQLServer, Host: "mssql", Port: 1433, Username: "sa", Password: "secret", Database: "app", TLSMode: domain.TLSModeRequired, TLSServerName: "mssql.internal"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527}
	tables := []domain.MigrationTable{{SourceSchema: "dbo", SourceTable: "orders"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-sqlserver-cdc" {
		t.Fatalf("bad SQL Server CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_SQLSERVER_START_LSN"] != task.CDCStartPositionValue || cfg.Env["QMIGRATION_SQLSERVER_TLS_MODE"] != "REQUIRED" {
		t.Fatalf("bad SQL Server CDC env: %+v", cfg.Env)
	}
}

func TestUnifiedOracleCDCRender(t *testing.T) {
	task := &domain.MigrationTask{ID: "ora", Mode: domain.ModeIncremental, CDCStartPositionType: "ORACLE_SCN", CDCStartPositionValue: "123456"}
	src := domain.DataSource{Type: domain.DataSourceOracle, Host: "ora", Port: 1521, Username: "qmigration", Password: "secret", Database: "ORCLPDB1"}
	dst := domain.DataSource{Type: domain.DataSourcePolarDBX, Host: "dst", Port: 8527}
	tables := []domain.MigrationTable{{SourceSchema: "APP", SourceTable: "ORDERS"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, dst, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-oracle-cdc" {
		t.Fatalf("bad Oracle CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_ORACLE_START_SCN"] != "123456" || cfg.Env["QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC"] != "1" {
		t.Fatalf("bad Oracle CDC env: %+v", cfg.Env)
	}
}

func TestUnifiedDB2CDCRender(t *testing.T) {
	task := &domain.MigrationTask{ID: "db2", Mode: domain.ModeIncremental, CDCStartPositionType: "DB2_LRI", CDCStartPositionValue: "1:0000000000000001:0000000000000002"}
	src := domain.DataSource{Type: domain.DataSourceDB2, Host: "db2", Port: 50000, Username: "db2inst1", Password: "secret", Database: "SAMPLE", CDCURL: "db2logs://agent:8787?server_name=db2log.internal"}
	tables := []domain.MigrationTable{{SourceSchema: "APP", SourceTable: "ORDERS"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, domain.DataSource{Type: domain.DataSourcePolarDBX}, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-db2-cdc" {
		t.Fatalf("bad DB2 CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_DB2_START_LRI"] != task.CDCStartPositionValue || cfg.Env["QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC"] != "1" {
		t.Fatalf("bad DB2 CDC env: %+v", cfg.Env)
	}
}

func TestUnifiedDamengCDCRender(t *testing.T) {
	task := &domain.MigrationTask{ID: "dm", Mode: domain.ModeIncremental, CDCStartPositionType: "DM_LSN", CDCStartPositionValue: "12345"}
	src := domain.DataSource{Type: domain.DataSourceDameng, Host: "dm", Port: 5236, Username: "APP", Password: "secret", Schema: "APP", DriverClass: "dm"}
	tables := []domain.MigrationTable{{SourceSchema: "APP", SourceTable: "ORDERS"}}
	cfg, err := NewUnified().Render(context.Background(), task, src, domain.DataSource{Type: domain.DataSourcePolarDBX}, tables)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Engine != "qmigration" || len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-dameng-cdc" {
		t.Fatalf("bad Dameng CDC config: %+v", cfg)
	}
	if cfg.Env["QMIGRATION_DAMENG_START_LSN"] != "12345" || cfg.Env["QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC"] != "1" {
		t.Fatalf("bad Dameng CDC env: %+v", cfg.Env)
	}
}

func TestUnifiedGBase8aCDCRender(t *testing.T) {
	lineage := strings.Repeat("ab", 32)
	task := &domain.MigrationTask{ID: "gbase8a", Mode: domain.ModeIncremental, CDCStartPositionType: "GBASE8A_CDC_SEQ", CDCStartPositionValue: "seq=42;capture=" + lineage}
	src := domain.DataSource{Type: domain.DataSourceGBase, Database: "app", CDCURL: "gbase8acdc://127.0.0.1:9288"}
	tables := []domain.MigrationTable{{SourceSchema: "app", SourceTable: "orders", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "integer", ColumnType: "INTEGER", Nullable: false, PrimaryKey: true}, {Name: "v", DataType: "varchar", ColumnType: "VARCHAR(64)", Nullable: true}}}}
	cfg, err := NewUnified().Render(context.Background(), task, src, domain.DataSource{Type: domain.DataSourcePolarDBX}, tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-gbase-cdc" || cfg.Env["QMIGRATION_GBASE8A_CDC_START_POSITION"] != task.CDCStartPositionValue {
		t.Fatalf("cfg=%+v", cfg)
	}
	if !strings.Contains(cfg.Env["QMIGRATION_GBASE8A_CDC_SELECTIONS_JSON"], `"schema_fingerprint"`) {
		t.Fatalf("selection=%s", cfg.Env["QMIGRATION_GBASE8A_CDC_SELECTIONS_JSON"])
	}
}

func TestRegistryExposesSingleUnifiedEngine(t *testing.T) {
	r := NewRegistry()
	r.Register(NewUnified())
	infos := r.Infos(context.Background())
	if len(infos) != 1 || infos[0].Name != "qmigration" || !infos[0].Available {
		t.Fatalf("unexpected engine registry: %+v", infos)
	}
}

func TestUnifiedGaussDBDDLRenderRejectsNonIdentityTarget(t *testing.T) {
	task := &domain.MigrationTask{ID: "gauss-ddl-bad", Mode: domain.ModeIncremental, CDCStartPositionType: "GAUSSDB_LSN", CDCStartPositionValue: "0/16B6C50", CDCStartResource: "qmigration_gauss_ddl", CDCDDLMode: "SAME_FAMILY"}
	src := domain.DataSource{Type: domain.DataSourceGaussDB, Host: "gauss", Port: 8000, Username: "repl", Password: "secret", Database: "app"}
	if _, err := NewUnified().Render(context.Background(), task, src, domain.DataSource{Type: domain.DataSourcePolarDBX}, []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders", TargetSchema: "public", TargetTable: "orders"}}); err == nil || !strings.Contains(err.Error(), "requires a GaussDB target") {
		t.Fatalf("heterogeneous GaussDB DDL target accepted: %v", err)
	}
	if _, err := NewUnified().Render(context.Background(), task, src, domain.DataSource{Type: domain.DataSourceGaussDB}, []domain.MigrationTable{{SourceSchema: "public", SourceTable: "orders", TargetSchema: "app", TargetTable: "orders"}}); err == nil || !strings.Contains(err.Error(), "identity mapping") {
		t.Fatalf("renamed GaussDB DDL mapping accepted: %v", err)
	}
}

func TestUnifiedGBase8sCDCRender(t *testing.T) {
	task := &domain.MigrationTask{Mode: domain.ModeFullAndIncremental, CDCStartPositionType: "GBASE8S_CDC_SEQ", CDCStartPositionValue: "restart=50;commit=100"}
	src := domain.DataSource{Type: domain.DataSourceGBase8s, Database: "app", CDCURL: "gbase8scdc://127.0.0.1:9188"}
	tables := []domain.MigrationTable{{SourceSchema: "app", SourceTable: "orders", PrimaryKeys: []string{"id"}, Columns: []domain.ColumnInfo{{Name: "id", DataType: "integer", ColumnType: "INTEGER", Nullable: false, PrimaryKey: true}, {Name: "v", DataType: "varchar", ColumnType: "VARCHAR(64)", Nullable: true}}}}
	cfg, err := NewUnified().Render(context.Background(), task, src, domain.DataSource{Type: domain.DataSourcePolarDBX}, tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Command) != 1 || cfg.Command[0] != "qmigration-gbase8s-cdc" || cfg.Env["QMIGRATION_GBASE8S_CDC_START_POSITION"] != "restart=50;commit=100" {
		t.Fatalf("cfg=%+v", cfg)
	}
	selJSON := cfg.Env["QMIGRATION_GBASE8S_CDC_SELECTIONS_JSON"]
	if !strings.Contains(selJSON, `"orders"`) || !strings.Contains(selJSON, `"schema_fingerprint"`) || !strings.Contains(selJSON, `"primary_keys":["id"]`) {
		t.Fatalf("selection env=%s", selJSON)
	}
}
