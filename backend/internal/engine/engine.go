package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"qmigration/backend/internal/cdc/gbase8acdc"
	"qmigration/backend/internal/cdc/gbase8scdc"
	"qmigration/backend/internal/cdc/obbinlog"
	"qmigration/backend/internal/cdc/ticdc"
	"qmigration/backend/internal/domain"
	"sort"
	"strings"
)

// Adapter describes QMigration's built-in execution engine and its internally supervised runtimes.
// Only the unified `qmigration` adapter is registered by the server.
type Adapter interface {
	Name() string
	Info(context.Context) domain.EngineInfo
	Render(context.Context, *domain.MigrationTask, domain.DataSource, domain.DataSource, []domain.MigrationTable) (*domain.RuntimeSpec, error)
}

type Registry struct{ adapters map[string]Adapter }

func NewRegistry() *Registry                        { return &Registry{adapters: map[string]Adapter{}} }
func (r *Registry) Register(a Adapter)              { r.adapters[a.Name()] = a }
func (r *Registry) Get(name string) (Adapter, bool) { a, ok := r.adapters[name]; return a, ok }
func (r *Registry) Infos(ctx context.Context) []domain.EngineInfo {
	out := make([]domain.EngineInfo, 0, len(r.adapters))
	for _, a := range r.adapters {
		out = append(out, a.Info(ctx))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func lookPath(names ...string) (string, bool) {
	dirs := []string{}
	if d := strings.TrimSpace(os.Getenv("QMIGRATION_BIN_DIR")); d != "" {
		dirs = append(dirs, d)
	}
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(exe))
	}
	for _, n := range names {
		if p, err := exec.LookPath(n); err == nil {
			return p, true
		}
		for _, d := range dirs {
			p := filepath.Join(d, n)
			if st, err := os.Stat(p); err == nil && !st.IsDir() && st.Mode()&0111 != 0 {
				return p, true
			}
		}
	}
	return "", false
}

func qualified(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// NativeMySQLCDCAdapter runs QMigration's built-in MySQL row-binlog reader.
// It currently requires ROW/FULL binlog images and tables whose binlog column
// types are supported by the native decoder; unsupported types fail without
// advancing the durable QMigration checkpoint.
type NativeMySQLCDCAdapter struct{}

func NewNativeMySQLCDC() *NativeMySQLCDCAdapter { return &NativeMySQLCDCAdapter{} }
func (*NativeMySQLCDCAdapter) Name() string     { return "native-mysql-cdc" }
func (*NativeMySQLCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-mysql-cdc")
	return domain.EngineInfo{Name: "native-mysql-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Built-in COM_BINLOG_DUMP/COM_BINLOG_DUMP_GTID row-event reader with transaction checkpointing; supports MySQL Transaction Payload ZSTD when zstd is available on the Worker and prefers durable GTID-set recovery"}
}
func (*NativeMySQLCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if !src.Type.IsMySQLFamily() {
		return nil, fmt.Errorf("native-mysql-cdc requires MySQL-family source, got %s", src.Type)
	}
	positionType := strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType))
	if positionType != "BINLOG" && positionType != "GTID" {
		return nil, fmt.Errorf("native-mysql-cdc requires a captured BINLOG or GTID start, got %q", task.CDCStartPositionType)
	}
	if strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-mysql-cdc requires a durable start position")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-mysql-cdc requires planned tables")
	}
	plan := map[string]any{"position_type": positionType, "position_value": task.CDCStartPositionValue, "tables": tableNames}
	env := map[string]string{
		"QMIGRATION_MYSQL_SOURCE_TYPE":     string(src.Type),
		"QMIGRATION_MYSQL_HOST":            src.Host,
		"QMIGRATION_MYSQL_PORT":            fmt.Sprint(src.Port),
		"QMIGRATION_MYSQL_USER":            src.Username,
		"MYSQL_PWD":                        src.Password,
		"QMIGRATION_MYSQL_DATABASE":        src.Database,
		"QMIGRATION_MYSQL_TABLES":          strings.Join(tableNames, ","),
		"QMIGRATION_MYSQL_TLS_MODE":        string(src.TLSMode),
		"QMIGRATION_MYSQL_TLS_SERVER_NAME": src.TLSServerName,
		"QMIGRATION_MYSQL_TLS_CA":          src.TLSCACert,
		"QMIGRATION_MYSQL_TLS_CLIENT_CERT": src.TLSClientCert,
		"QMIGRATION_MYSQL_TLS_CLIENT_KEY":  src.TLSClientKey,
	}
	if positionType == "GTID" {
		env["QMIGRATION_MYSQL_START_GTID"] = task.CDCStartPositionValue
	} else {
		parts := strings.LastIndex(task.CDCStartPositionValue, ":")
		if parts <= 0 || parts == len(task.CDCStartPositionValue)-1 {
			return nil, fmt.Errorf("invalid native MySQL start position %q", task.CDCStartPositionValue)
		}
		startFile := task.CDCStartPositionValue[:parts]
		startPos := task.CDCStartPositionValue[parts+1:]
		plan["start_file"] = startFile
		plan["start_pos"] = startPos
		env["QMIGRATION_MYSQL_START_FILE"] = startFile
		env["QMIGRATION_MYSQL_START_POS"] = startPos
	}
	buf, _ := json.MarshalIndent(plan, "", "  ")
	return &domain.RuntimeSpec{Engine: "native-mysql-cdc", Format: "json", Filename: "native-mysql-cdc.json", Content: string(buf), Command: []string{"qmigration-mysql-cdc"}, Env: env}, nil
}

// NativeOceanBaseCDCAdapter consumes OceanBase Binlog Service through an
// explicit tenant ODP subscription endpoint. OceanBase Binlog Service exposes
// MySQL Binlog V4/GTID, so QMigration reuses its native MySQL row-event decoder
// while keeping the SQL endpoint and CDC endpoint strictly separate.
type NativeOceanBaseCDCAdapter struct{}

func NewNativeOceanBaseCDC() *NativeOceanBaseCDCAdapter { return &NativeOceanBaseCDCAdapter{} }
func (*NativeOceanBaseCDCAdapter) Name() string         { return "native-oceanbase-binlog" }
func (*NativeOceanBaseCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-mysql-cdc")
	return domain.EngineInfo{Name: "native-oceanbase-binlog", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "OceanBase Binlog Service via tenant ODP using QMigration native MySQL Binlog V4/GTID decoder and durable apply-before-ACK checkpointing"}
}
func (*NativeOceanBaseCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceOceanBase {
		return nil, fmt.Errorf("native-oceanbase-binlog requires OceanBase MySQL source, got %s", src.Type)
	}
	subDS, ep, err := obbinlog.DataSourceForSubscription(src)
	if err != nil {
		return nil, err
	}
	positionType := strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType))
	if positionType != "BINLOG" && positionType != "GTID" {
		return nil, fmt.Errorf("native-oceanbase-binlog requires a captured BINLOG or GTID start, got %q", task.CDCStartPositionType)
	}
	if strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-oceanbase-binlog requires a durable start position")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-oceanbase-binlog requires planned tables")
	}
	plan := map[string]any{
		"position_type": positionType, "position_value": task.CDCStartPositionValue,
		"subscription_endpoint": ep.URL, "route": "tenant ODP -> OceanBase Binlog Service",
		"protocol": "MySQL Binlog V4", "tables": tableNames,
	}
	env := map[string]string{
		"QMIGRATION_MYSQL_SOURCE_TYPE":        string(domain.DataSourceOceanBase),
		"QMIGRATION_MYSQL_HOST":               subDS.Host,
		"QMIGRATION_MYSQL_PORT":               fmt.Sprint(subDS.Port),
		"QMIGRATION_MYSQL_USER":               subDS.Username,
		"MYSQL_PWD":                           subDS.Password,
		"QMIGRATION_MYSQL_DATABASE":           subDS.Database,
		"QMIGRATION_MYSQL_CDC_URL":            src.CDCURL,
		"QMIGRATION_MYSQL_FAILOVER_ENDPOINTS": ep.FailoverString(),
		"QMIGRATION_MYSQL_TABLES":             strings.Join(tableNames, ","),
		"QMIGRATION_MYSQL_TLS_MODE":           string(subDS.TLSMode),
		"QMIGRATION_MYSQL_TLS_SERVER_NAME":    subDS.TLSServerName,
		"QMIGRATION_MYSQL_TLS_CA":             subDS.TLSCACert,
		"QMIGRATION_MYSQL_TLS_CLIENT_CERT":    subDS.TLSClientCert,
		"QMIGRATION_MYSQL_TLS_CLIENT_KEY":     subDS.TLSClientKey,
	}
	if positionType == "GTID" {
		env["QMIGRATION_MYSQL_START_GTID"] = task.CDCStartPositionValue
	} else {
		i := strings.LastIndex(task.CDCStartPositionValue, ":")
		if i <= 0 || i == len(task.CDCStartPositionValue)-1 {
			return nil, fmt.Errorf("invalid OceanBase Binlog start position %q", task.CDCStartPositionValue)
		}
		env["QMIGRATION_MYSQL_START_FILE"] = task.CDCStartPositionValue[:i]
		env["QMIGRATION_MYSQL_START_POS"] = task.CDCStartPositionValue[i+1:]
	}
	b, _ := json.MarshalIndent(plan, "", "  ")
	return &domain.RuntimeSpec{Engine: "native-oceanbase-binlog", Format: "json", Filename: "native-oceanbase-binlog.json", Content: string(b), Command: []string{"qmigration-mysql-cdc"}, Env: env}, nil
}

// NativeTiDBCDCAdapter manages a dedicated TiCDC changefeed and consumes its
// single-partition Canal-JSON Kafka topic using QMigration's own Kafka wire
// client. TiCDC remains the TiDB-native log capture component; QMigration owns
// changefeed lifecycle, decoding, durable offsets, target apply and restart.
type NativeTiDBCDCAdapter struct{}

func NewNativeTiDBCDC() *NativeTiDBCDCAdapter { return &NativeTiDBCDCAdapter{} }
func (*NativeTiDBCDCAdapter) Name() string    { return "native-tidb-ticdc" }
func (*NativeTiDBCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-tidb-cdc")
	return domain.EngineInfo{Name: "native-tidb-ticdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Built-in TiCDC OpenAPI lifecycle + native Kafka Canal-JSON consumer with durable TSO/offset checkpoints"}
}
func (*NativeTiDBCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceTiDB {
		return nil, fmt.Errorf("native-tidb-ticdc requires TiDB source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "TIDB_TSO" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-tidb-ticdc requires captured TIDB_TSO start position")
	}
	if _, err := ticdc.ParseEndpoint(src.CDCURL); err != nil {
		return nil, err
	}
	if _, err := ticdc.ParsePosition(task.CDCStartPositionValue); err != nil {
		return nil, err
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-tidb-ticdc requires planned tables")
	}
	plan := map[string]any{"position_type": "TIDB_TSO", "position_value": task.CDCStartPositionValue, "changefeed_name_scope": "task-id + CDC direction", "tables": tableNames, "kafka_partitions": 1}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_TICDC_URL":            src.CDCURL,
		"QMIGRATION_TICDC_START_POSITION": task.CDCStartPositionValue,
		"QMIGRATION_TICDC_TABLES":         strings.Join(tableNames, ","),
	}
	return &domain.RuntimeSpec{Engine: "native-tidb-ticdc", Format: "json", Filename: "native-tidb-ticdc.json", Content: string(b), Command: []string{"qmigration-tidb-cdc"}, Env: env}, nil
}

// NativePostgresCDCAdapter runs QMigration's built-in PostgreSQL logical
// replication reader. The worker owns the process lifecycle while the reader
// advances the slot only after the control plane atomically applies a source
// transaction.
type NativePostgresCDCAdapter struct{}

func NewNativePostgresCDC() *NativePostgresCDCAdapter { return &NativePostgresCDCAdapter{} }
func (*NativePostgresCDCAdapter) Name() string        { return "native-postgres-cdc" }
func (*NativePostgresCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-postgres-cdc")
	return domain.EngineInfo{Name: "native-postgres-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Built-in pgoutput logical replication reader with apply-before-ACK semantics"}
}
func (*NativePostgresCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if !src.Type.IsPostgreSQLFamily() {
		return nil, fmt.Errorf("native-postgres-cdc requires PostgreSQL-family source, got %s", src.Type)
	}
	if task.CDCStartPositionType != "LSN" || task.CDCStartPositionValue == "" {
		return nil, errors.New("native-postgres-cdc requires a captured LSN start position")
	}
	if task.CDCStartResource == "" {
		return nil, errors.New("native-postgres-cdc requires a logical replication slot checkpoint")
	}
	publication := strings.ToLower(strings.ReplaceAll("qmigration_pub_"+task.ID, "-", "_"))
	if len(publication) > 63 {
		publication = publication[:63]
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-postgres-cdc requires planned tables")
	}
	plan := map[string]any{"slot": task.CDCStartResource, "publication": publication, "start_lsn": task.CDCStartPositionValue, "tables": tableNames}
	buf, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_PG_HOST":               src.Host,
		"QMIGRATION_PG_PORT":               fmt.Sprint(src.Port),
		"QMIGRATION_PG_USER":               src.Username,
		"PGPASSWORD":                       src.Password,
		"QMIGRATION_PG_DATABASE":           src.Database,
		"QMIGRATION_PG_SLOT":               task.CDCStartResource,
		"QMIGRATION_PG_PUBLICATION":        publication,
		"QMIGRATION_PG_PUBLICATION_TABLES": strings.Join(tableNames, ","),
		"QMIGRATION_PG_START_LSN":          task.CDCStartPositionValue,
	}
	return &domain.RuntimeSpec{Engine: "native-postgres-cdc", Format: "json", Filename: "native-postgres-cdc.json", Content: string(buf), Command: []string{"qmigration-postgres-cdc"}, Env: env}, nil
}

// NativeOpenGaussCDCAdapter consumes openGauss mppdb_decoding through the
// documented SQL logical-decoding functions. QMigration peeks complete
// transactions and advances the slot only after target commit/checkpoint.
type NativeOpenGaussCDCAdapter struct{}

func NewNativeOpenGaussCDC() *NativeOpenGaussCDCAdapter { return &NativeOpenGaussCDCAdapter{} }
func (*NativeOpenGaussCDCAdapter) Name() string         { return "native-opengauss-logical-cdc" }
func (*NativeOpenGaussCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-opengauss-cdc")
	return domain.EngineInfo{Name: "native-opengauss-logical-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Experimental openGauss mppdb_decoding SQL logical CDC with OPENGAUSS_LSN apply-before-slot-advance semantics"}
}
func (*NativeOpenGaussCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, dst domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceOpenGauss {
		return nil, fmt.Errorf("native-opengauss-logical-cdc requires openGauss source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "OPENGAUSS_LSN" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-opengauss-logical-cdc requires captured OPENGAUSS_LSN start position")
	}
	if strings.TrimSpace(task.CDCStartResource) == "" {
		return nil, errors.New("native-opengauss-logical-cdc requires a logical replication slot checkpoint")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-opengauss-logical-cdc requires planned tables")
	}
	plan := map[string]any{"slot": task.CDCStartResource, "start_lsn": task.CDCStartPositionValue, "tables": tableNames, "decoder": "mppdb_decoding/json", "ack": "pg_logical_slot_get_changes(up_to_lsn) after target commit"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_OPENGAUSS_LOGICAL_CDC": "1",
		"QMIGRATION_OPENGAUSS_HOST":                     src.Host, "QMIGRATION_OPENGAUSS_PORT": fmt.Sprint(src.Port), "QMIGRATION_OPENGAUSS_USER": src.Username,
		"OPENGAUSS_PASSWORD": src.Password, "QMIGRATION_OPENGAUSS_DATABASE": src.Database, "QMIGRATION_OPENGAUSS_SLOT": task.CDCStartResource,
		"QMIGRATION_OPENGAUSS_START_LSN": task.CDCStartPositionValue, "QMIGRATION_OPENGAUSS_TABLES": strings.Join(tableNames, ","),
		"QMIGRATION_OPENGAUSS_TLS_MODE": string(src.TLSMode), "QMIGRATION_OPENGAUSS_TLS_SERVER_NAME": src.TLSServerName,
		"QMIGRATION_OPENGAUSS_TLS_CA": src.TLSCACert, "QMIGRATION_OPENGAUSS_TLS_CLIENT_CERT": src.TLSClientCert, "QMIGRATION_OPENGAUSS_TLS_CLIENT_KEY": src.TLSClientKey,
	}
	if strings.EqualFold(strings.TrimSpace(task.CDCDDLMode), "SAME_FAMILY") {
		if dst.Type != domain.DataSourceOpenGauss {
			return nil, fmt.Errorf("openGauss CDC DDL SAME_FAMILY requires openGauss target, got %s", dst.Type)
		}
		if strings.TrimSpace(os.Getenv("QMIGRATION_OPENGAUSS_DDL_SIDECAR_URL")) == "" {
			return nil, errors.New("openGauss CDC DDL SAME_FAMILY requires QMIGRATION_OPENGAUSS_DDL_SIDECAR_URL")
		}
		env["QMIGRATION_EXPERIMENTAL_OPENGAUSS_DDL_CDC"] = "1"
	}
	return &domain.RuntimeSpec{Engine: "native-opengauss-logical-cdc", Format: "json", Filename: "native-opengauss-logical-cdc.json", Content: string(b), Command: []string{"qmigration-opengauss-cdc"}, Env: env}, nil
}

// NativeKingbaseCDCAdapter uses KingbaseES' sys_* logical slot functions with
// its pgoutput-compatible replication stream. Position type stays explicitly
// KINGBASE_LSN so it is never conflated with PostgreSQL qualification.
type NativeKingbaseCDCAdapter struct{}

func NewNativeKingbaseCDC() *NativeKingbaseCDCAdapter { return &NativeKingbaseCDCAdapter{} }
func (*NativeKingbaseCDCAdapter) Name() string        { return "native-kingbase-kboutput-cdc" }
func (*NativeKingbaseCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-postgres-cdc")
	return domain.EngineInfo{Name: "native-kingbase-kboutput-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Experimental KingbaseES sys_* logical slot + kboutput stream with KINGBASE_LSN apply-before-ACK semantics"}
}
func (*NativeKingbaseCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, dst domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceKingbase {
		return nil, fmt.Errorf("native-kingbase-kboutput-cdc requires KingbaseES source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "KINGBASE_LSN" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-kingbase-kboutput-cdc requires captured KINGBASE_LSN start position")
	}
	if strings.TrimSpace(task.CDCStartResource) == "" {
		return nil, errors.New("native-kingbase-kboutput-cdc requires a logical replication slot checkpoint")
	}
	publication := strings.ToLower(strings.ReplaceAll("qmigration_pub_"+task.ID, "-", "_"))
	if len(publication) > 63 {
		publication = publication[:63]
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-kingbase-kboutput-cdc requires planned tables")
	}
	plan := map[string]any{"slot": task.CDCStartResource, "publication": publication, "start_lsn": task.CDCStartPositionValue, "tables": tableNames, "slot_api": "sys_create_logical_replication_slot", "decoder": "kboutput (pgoutput-compatible wire conformance required)"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_KINGBASE_LOGICAL_CDC": "1", "QMIGRATION_PG_SOURCE_TYPE": string(domain.DataSourceKingbase),
		"QMIGRATION_PG_HOST": src.Host, "QMIGRATION_PG_PORT": fmt.Sprint(src.Port), "QMIGRATION_PG_USER": src.Username, "PGPASSWORD": src.Password,
		"QMIGRATION_PG_DATABASE": src.Database, "QMIGRATION_PG_SLOT": task.CDCStartResource, "QMIGRATION_PG_PUBLICATION": publication,
		"QMIGRATION_PG_PUBLICATION_TABLES": strings.Join(tableNames, ","), "QMIGRATION_PG_START_LSN": task.CDCStartPositionValue,
		"QMIGRATION_PG_TLS_MODE": string(src.TLSMode), "QMIGRATION_PG_TLS_SERVER_NAME": src.TLSServerName, "QMIGRATION_PG_TLS_CA": src.TLSCACert,
		"QMIGRATION_PG_TLS_CLIENT_CERT": src.TLSClientCert, "QMIGRATION_PG_TLS_CLIENT_KEY": src.TLSClientKey,
	}
	if strings.EqualFold(strings.TrimSpace(task.CDCDDLMode), "SAME_FAMILY") {
		if dst.Type != domain.DataSourceKingbase {
			return nil, fmt.Errorf("Kingbase CDC DDL SAME_FAMILY requires Kingbase target, got %s", dst.Type)
		}
		if strings.TrimSpace(os.Getenv("QMIGRATION_KINGBASE_DDL_SIDECAR_URL")) == "" {
			return nil, errors.New("Kingbase CDC DDL SAME_FAMILY requires QMIGRATION_KINGBASE_DDL_SIDECAR_URL")
		}
		env["QMIGRATION_EXPERIMENTAL_KINGBASE_DDL_CDC"] = "1"
	}
	return &domain.RuntimeSpec{Engine: "native-kingbase-kboutput-cdc", Format: "json", Filename: "native-kingbase-kboutput-cdc.json", Content: string(b), Command: []string{"qmigration-postgres-cdc"}, Env: env}, nil
}

// NativeGaussDBCDCAdapter consumes Huawei GaussDB logical decoding through
// SQL replication-slot functions.  The reader peeks complete transactions,
// lets the control plane commit the target, then advances the source slot with
// pg_logical_slot_get_changes(up_to_lsn), preserving apply-before-ACK order.
func gaussDBMultiPrimaryConfig(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "[") {
		var v []map[string]any
		if err := json.Unmarshal([]byte(raw), &v); err != nil || len(v) < 2 {
			return "", errors.New("GaussDB multi-primary cdc_url JSON must contain at least two primary descriptors")
		}
		return raw, nil
	}
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "gaussdbmulti") {
		return "", errors.New("GaussDB GAUSSDB_CSN_VECTOR requires cdc_url as JSON array or gaussdbmulti://...?config_b64=<base64url-json>")
	}
	q := u.Query()
	if v := strings.TrimSpace(q.Get("config")); v != "" {
		return gaussDBMultiPrimaryConfig(v)
	}
	if v := strings.TrimSpace(q.Get("config_b64")); v != "" {
		b, err := base64.RawURLEncoding.DecodeString(v)
		if err != nil {
			return "", fmt.Errorf("invalid GaussDB multi-primary config_b64: %w", err)
		}
		return gaussDBMultiPrimaryConfig(string(b))
	}
	return "", errors.New("GaussDB multi-primary cdc_url is missing config/config_b64")
}

type NativeGaussDBCDCAdapter struct{}

func NewNativeGaussDBCDC() *NativeGaussDBCDCAdapter { return &NativeGaussDBCDCAdapter{} }
func (*NativeGaussDBCDCAdapter) Name() string       { return "native-gaussdb-logical-cdc" }
func (*NativeGaussDBCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-gaussdb-cdc")
	return domain.EngineInfo{Name: "native-gaussdb-logical-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Experimental GaussDB byte-safe SQL binary logical-decoding reader using replication slots with apply-before-slot-advance semantics"}
}
func (*NativeGaussDBCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, dst domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceGaussDB {
		return nil, fmt.Errorf("native-gaussdb-logical-cdc requires GaussDB source, got %s", src.Type)
	}
	positionType := strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType))
	if (positionType != "GAUSSDB_LSN" && positionType != "GAUSSDB_CSN_VECTOR") || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-gaussdb-logical-cdc requires captured GAUSSDB_LSN or GAUSSDB_CSN_VECTOR start position")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-gaussdb-logical-cdc requires planned tables")
	}
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_GAUSSDB_NATIVE": "1", "QMIGRATION_EXPERIMENTAL_GAUSSDB_LOGICAL_CDC": "1",
		"QMIGRATION_GAUSSDB_HOST": src.Host, "QMIGRATION_GAUSSDB_PORT": fmt.Sprint(src.Port), "QMIGRATION_GAUSSDB_USER": src.Username,
		"GAUSSDB_PASSWORD": src.Password, "QMIGRATION_GAUSSDB_DATABASE": src.Database, "QMIGRATION_GAUSSDB_TABLES": strings.Join(tableNames, ","),
		"QMIGRATION_GAUSSDB_TLS_MODE": string(src.TLSMode), "QMIGRATION_GAUSSDB_TLS_SERVER_NAME": src.TLSServerName,
		"QMIGRATION_GAUSSDB_TLS_CA": src.TLSCACert, "QMIGRATION_GAUSSDB_TLS_CLIENT_CERT": src.TLSClientCert, "QMIGRATION_GAUSSDB_TLS_CLIENT_KEY": src.TLSClientKey,
	}
	plan := map[string]any{"position_type": positionType, "position_value": task.CDCStartPositionValue, "tables": tableNames}
	if positionType == "GAUSSDB_CSN_VECTOR" {
		cfg, err := gaussDBMultiPrimaryConfig(src.CDCURL)
		if err != nil {
			return nil, err
		}
		env["QMIGRATION_EXPERIMENTAL_GAUSSDB_MULTI_PRIMARY"] = "1"
		env["QMIGRATION_GAUSSDB_PRIMARIES_JSON"] = cfg
		env["QMIGRATION_GAUSSDB_START_VECTOR"] = task.CDCStartPositionValue
		plan["ordering"] = "global CSN plus all-primary resolved watermarks"
		plan["checkpoint"] = "GAUSSDB_CSN_VECTOR"
	} else {
		if strings.TrimSpace(task.CDCStartResource) == "" {
			return nil, errors.New("native-gaussdb-logical-cdc requires a logical replication slot checkpoint")
		}
		env["QMIGRATION_GAUSSDB_SLOT"] = task.CDCStartResource
		env["QMIGRATION_GAUSSDB_START_LSN"] = task.CDCStartPositionValue
		plan["slot"] = task.CDCStartResource
		plan["decoder"] = "mppdb_decoding/binary"
	}
	if strings.EqualFold(strings.TrimSpace(task.CDCDDLMode), "SAME_FAMILY") {
		if dst.Type != domain.DataSourceGaussDB {
			return nil, fmt.Errorf("GaussDB CDC DDL SAME_FAMILY requires a GaussDB target, got %s", dst.Type)
		}
		for _, t := range tables {
			if !strings.EqualFold(strings.TrimSpace(t.SourceSchema), strings.TrimSpace(t.TargetSchema)) || !strings.EqualFold(strings.TrimSpace(t.SourceTable), strings.TrimSpace(t.TargetTable)) {
				return nil, fmt.Errorf("GaussDB CDC DDL requires identity mapping, got %s.%s -> %s.%s", t.SourceSchema, t.SourceTable, t.TargetSchema, t.TargetTable)
			}
		}
		env["QMIGRATION_GAUSSDB_DDL_REPLAY"] = "1"
		plan["ddl"] = "safe selected-table hybrid DDL/DML reconstruction with binary DML cardinality proof"
	}
	b, _ := json.MarshalIndent(plan, "", "  ")
	return &domain.RuntimeSpec{Engine: "native-gaussdb-logical-cdc", Format: "json", Filename: "native-gaussdb-logical-cdc.json", Content: string(b), Command: []string{"qmigration-gaussdb-cdc"}, Env: env}, nil
}

// NativeGBase8aCDCAdapter reads committed GBase 8a source transactions from a
// datasource-local provider. The provider must attest complete row images,
// transaction atomicity, schema fences and a stable capture lineage; QMigration
// retains ownership of target apply, durable checkpoint and source ACK ordering.
type NativeGBase8aCDCAdapter struct{}

func NewNativeGBase8aCDC() *NativeGBase8aCDCAdapter { return &NativeGBase8aCDCAdapter{} }
func (*NativeGBase8aCDCAdapter) Name() string       { return "native-gbase8a-provider-cdc" }
func (*NativeGBase8aCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-gbase-cdc")
	return domain.EngineInfo{Name: "native-gbase8a-provider-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "GBase 8a datasource-local CDC proof provider with durable GBASE8A_CDC_SEQ checkpoints"}
}
func (*NativeGBase8aCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceGBase {
		return nil, fmt.Errorf("native-gbase8a-provider-cdc requires GBase 8a source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "GBASE8A_CDC_SEQ" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-gbase8a-provider-cdc requires captured GBASE8A_CDC_SEQ start position")
	}
	if strings.TrimSpace(src.CDCURL) == "" {
		return nil, errors.New("native-gbase8a-provider-cdc requires datasource cdc_url")
	}
	selections := make([]gbase8acdc.TableSelection, 0, len(tables))
	for _, t := range tables {
		pks := append([]string(nil), t.PrimaryKeys...)
		if len(pks) == 0 && strings.TrimSpace(t.PrimaryKey) != "" {
			pks = []string{t.PrimaryKey}
		}
		sel, err := gbase8acdc.BuildTableSelection(t.SourceSchema, t.SourceTable, t.Columns, pks)
		if err != nil {
			return nil, fmt.Errorf("GBase 8a CDC planned schema %s.%s: %w", t.SourceSchema, t.SourceTable, err)
		}
		selections = append(selections, sel)
	}
	if len(selections) == 0 {
		return nil, errors.New("native-gbase8a-provider-cdc requires planned tables")
	}
	selectionJSON, _ := json.Marshal(selections)
	plan := map[string]any{"position_type": "GBASE8A_CDC_SEQ", "position_value": task.CDCStartPositionValue, "provider": src.CDCURL, "tables": selections, "proofs": []string{"capture-lineage", "schema-fence", "committed-transaction-atomicity", "complete-row-images", "monotonic-sequence"}, "ack": "target/spool durable apply before provider ACK"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{"QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE": "1", "QMIGRATION_EXPERIMENTAL_GBASE8A_SOURCE_CDC": "1", "QMIGRATION_GBASE8A_CDC_URL": src.CDCURL, "QMIGRATION_GBASE8A_CDC_DATABASE": src.Database, "QMIGRATION_GBASE8A_CDC_START_POSITION": task.CDCStartPositionValue, "QMIGRATION_GBASE8A_CDC_SELECTIONS_JSON": string(selectionJSON)}
	return &domain.RuntimeSpec{Engine: "native-gbase8a-provider-cdc", Format: "json", Filename: "native-gbase8a-provider-cdc.json", Content: string(b), Command: []string{"qmigration-gbase-cdc"}, Env: env}, nil
}

// NativeGBase8sCDCAdapter reads GBase 8s syscdcv1 Change Data Capture through
// a datasource-local CSDK smart-LOB provider. QMigration owns transaction
// assembly and durable restart=<open-begin>;commit=<applied-commit> semantics.
type NativeGBase8sCDCAdapter struct{}

func NewNativeGBase8sCDC() *NativeGBase8sCDCAdapter { return &NativeGBase8sCDCAdapter{} }
func (*NativeGBase8sCDCAdapter) Name() string       { return "native-gbase8s-csdk-cdc" }
func (*NativeGBase8sCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-gbase8s-cdc")
	return domain.EngineInfo{Name: "native-gbase8s-csdk-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Experimental GBase 8s syscdcv1/CSDK CDC provider with durable restart/commit sequence checkpoints"}
}
func (*NativeGBase8sCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceGBase8s {
		return nil, fmt.Errorf("native-gbase8s-csdk-cdc requires GBase 8s source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "GBASE8S_CDC_SEQ" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-gbase8s-csdk-cdc requires captured GBASE8S_CDC_SEQ start position")
	}
	if strings.TrimSpace(src.CDCURL) == "" {
		return nil, errors.New("native-gbase8s-csdk-cdc requires datasource cdc_url")
	}
	selections := make([]gbase8scdc.TableSelection, 0, len(tables))
	for _, t := range tables {
		pk := append([]string(nil), t.PrimaryKeys...)
		if len(pk) == 0 && strings.TrimSpace(t.PrimaryKey) != "" {
			pk = []string{t.PrimaryKey}
		}
		sel, err := gbase8scdc.BuildTableSelection(t.SourceSchema, t.SourceTable, t.Columns, pk)
		if err != nil {
			return nil, fmt.Errorf("GBase 8s CDC planned schema %s.%s: %w", t.SourceSchema, t.SourceTable, err)
		}
		selections = append(selections, sel)
	}
	if len(selections) == 0 {
		return nil, errors.New("native-gbase8s-csdk-cdc requires planned tables")
	}
	selectionJSON, _ := json.Marshal(selections)
	plan := map[string]any{"position_type": "GBASE8S_CDC_SEQ", "position_value": task.CDCStartPositionValue, "provider": src.CDCURL, "tables": selections, "ack": "target/spool durable apply before QMigration commit watermark advance"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{"QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE": "1", "QMIGRATION_EXPERIMENTAL_GBASE8S_CDC": "1", "QMIGRATION_GBASE8S_CDC_URL": src.CDCURL, "QMIGRATION_GBASE8S_CDC_DATABASE": src.Database, "QMIGRATION_GBASE8S_CDC_START_POSITION": task.CDCStartPositionValue, "QMIGRATION_GBASE8S_CDC_SELECTIONS_JSON": string(selectionJSON)}
	return &domain.RuntimeSpec{Engine: "native-gbase8s-csdk-cdc", Format: "json", Filename: "native-gbase8s-csdk-cdc.json", Content: string(b), Command: []string{"qmigration-gbase8s-cdc"}, Env: env}, nil
}

// NativeSQLServerCDCAdapter runs QMigration's own SQL Server CDC/LSN polling
// reader over the native TDS connector. It does not invoke Debezium, SSIS or
// any third-party migration runtime.
type NativeSQLServerCDCAdapter struct{}

func NewNativeSQLServerCDC() *NativeSQLServerCDCAdapter { return &NativeSQLServerCDCAdapter{} }
func (*NativeSQLServerCDCAdapter) Name() string         { return "native-sqlserver-cdc" }
func (*NativeSQLServerCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-sqlserver-cdc")
	return domain.EngineInfo{Name: "native-sqlserver-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Built-in SQL Server CDC change-table reader over QMigration native TDS with apply-before-LSN-advance semantics"}
}
func (*NativeSQLServerCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceSQLServer {
		return nil, fmt.Errorf("native-sqlserver-cdc requires SQL Server source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "SQLSERVER_LSN" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-sqlserver-cdc requires captured SQLSERVER_LSN start position")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-sqlserver-cdc requires planned tables")
	}
	plan := map[string]any{"position_type": "SQLSERVER_LSN", "position_value": task.CDCStartPositionValue, "tables": tableNames}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE": "1", "QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC": "1",
		"QMIGRATION_SQLSERVER_HOST": src.Host, "QMIGRATION_SQLSERVER_PORT": fmt.Sprint(src.Port), "QMIGRATION_SQLSERVER_USER": src.Username, "QMIGRATION_SQLSERVER_PASSWORD": src.Password, "QMIGRATION_SQLSERVER_DATABASE": src.Database,
		"QMIGRATION_SQLSERVER_START_LSN": task.CDCStartPositionValue, "QMIGRATION_SQLSERVER_TABLES": strings.Join(tableNames, ","), "QMIGRATION_SQLSERVER_TLS_MODE": string(src.TLSMode), "QMIGRATION_SQLSERVER_TLS_SERVER_NAME": src.TLSServerName, "QMIGRATION_SQLSERVER_TLS_CA": src.TLSCACert, "QMIGRATION_SQLSERVER_TLS_CLIENT_CERT": src.TLSClientCert, "QMIGRATION_SQLSERVER_TLS_CLIENT_KEY": src.TLSClientKey,
	}
	return &domain.RuntimeSpec{Engine: "native-sqlserver-cdc", Format: "json", Filename: "native-sqlserver-cdc.json", Content: string(b), Command: []string{"qmigration-sqlserver-cdc"}, Env: env}, nil
}

// NativeDB2CDCAdapter runs QMigration's db2ReadLog CDC reader through the
// source-side QMigration DB2 Log Agent. The Worker itself remains pure Go and
// does not link IBM client libraries.
type NativeDB2CDCAdapter struct{}

func NewNativeDB2CDC() *NativeDB2CDCAdapter { return &NativeDB2CDCAdapter{} }
func (*NativeDB2CDCAdapter) Name() string   { return "native-db2-readlog" }
func (*NativeDB2CDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-db2-cdc")
	return domain.EngineInfo{Name: "native-db2-readlog", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "QMigration DB2 source CDC using IBM db2ReadLog behind the QMigration DB2 Log Agent, DB2_LRI durable checkpoints and apply-before-ACK"}
}
func (*NativeDB2CDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceDB2 {
		return nil, fmt.Errorf("native-db2-readlog requires DB2 source, got %s", src.Type)
	}
	positionType := strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType))
	if (positionType != "DB2_LRI" && positionType != "DB2_PURESCALE_VECTOR") || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-db2-readlog requires captured DB2_LRI or DB2_PURESCALE_VECTOR start position")
	}
	if strings.TrimSpace(src.CDCURL) == "" {
		return nil, errors.New("native-db2-readlog requires cdc_url pointing to QMigration DB2 Log Agent")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-db2-readlog requires planned tables")
	}
	plan := map[string]any{"position_type": positionType, "position_value": task.CDCStartPositionValue, "agent": src.CDCURL, "tables": tableNames, "provider": "IBM db2ReadLog / pureScale proof provider via QMigration DB2 Log Agent"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_DB2_NATIVE": "1", "QMIGRATION_EXPERIMENTAL_DB2_LOG_CDC": "1",
		"QMIGRATION_DB2_HOST": src.Host, "QMIGRATION_DB2_PORT": fmt.Sprint(src.Port), "QMIGRATION_DB2_USER": src.Username, "QMIGRATION_DB2_PASSWORD": src.Password, "QMIGRATION_DB2_DATABASE": src.Database, "QMIGRATION_DB2_SCHEMA": src.Schema,
		"QMIGRATION_DB2_START_LRI": task.CDCStartPositionValue, "QMIGRATION_DB2_TABLES": strings.Join(tableNames, ","), "QMIGRATION_DB2_LOG_URL": src.CDCURL,
		"QMIGRATION_DB2_TLS_MODE": string(src.TLSMode), "QMIGRATION_DB2_TLS_SERVER_NAME": src.TLSServerName, "QMIGRATION_DB2_TLS_CA": src.TLSCACert, "QMIGRATION_DB2_TLS_CLIENT_CERT": src.TLSClientCert, "QMIGRATION_DB2_TLS_CLIENT_KEY": src.TLSClientKey,
	}
	if positionType == "DB2_PURESCALE_VECTOR" {
		env["QMIGRATION_EXPERIMENTAL_DB2_PURESCALE"] = "1"
		env["QMIGRATION_DB2_START_VECTOR"] = task.CDCStartPositionValue
		env["QMIGRATION_DB2_START_LRI"] = ""
		plan["ordering"] = "provider-proved pureScale global sequence plus all-stream resolved watermarks"
		b, _ = json.MarshalIndent(plan, "", "  ")
	}
	return &domain.RuntimeSpec{Engine: "native-db2-readlog", Format: "json", Filename: "native-db2-readlog.json", Content: string(b), Command: []string{"qmigration-db2-cdc"}, Env: env}, nil
}

// NativeDamengCDCAdapter runs QMigration's DM8 archived-log DBMS_LOGMNR reader.
// LogMiner supplies transaction/ROWID/commit-LSN ordering while the connector
// reconstructs complete row images through DM flashback AS OF SCN reads.
type NativeDamengCDCAdapter struct{}

func NewNativeDamengCDC() *NativeDamengCDCAdapter { return &NativeDamengCDCAdapter{} }
func (*NativeDamengCDCAdapter) Name() string      { return "native-dameng-logmnr" }
func (*NativeDamengCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-dameng-cdc")
	return domain.EngineInfo{Name: "native-dameng-logmnr", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Experimental DM8 DBMS_LOGMNR archived-log CDC using DM_LSN checkpoints, ROWID transaction indexing and AS OF SCN full-row reconstruction"}
}
func (*NativeDamengCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceDameng {
		return nil, fmt.Errorf("native-dameng-logmnr requires Dameng source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "DM_LSN" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-dameng-logmnr requires captured DM_LSN start position")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-dameng-logmnr requires planned tables")
	}
	plan := map[string]any{"position_type": "DM_LSN", "position_value": task.CDCStartPositionValue, "tables": tableNames, "resource": "DBMS_LOGMNR", "row_image": "flashback AS OF SCN"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_DAMENG_NATIVE":  "1",
		"QMIGRATION_EXPERIMENTAL_DAMENG_LOG_CDC": "1",
		"QMIGRATION_DAMENG_HOST":                 src.Host,
		"QMIGRATION_DAMENG_PORT":                 fmt.Sprint(src.Port),
		"QMIGRATION_DAMENG_USER":                 src.Username,
		"QMIGRATION_DAMENG_PASSWORD":             src.Password,
		"QMIGRATION_DAMENG_DATABASE":             src.Database,
		"QMIGRATION_DAMENG_SCHEMA":               src.Schema,
		"QMIGRATION_DAMENG_DSN":                  src.JDBCURL,
		"QMIGRATION_DAMENG_SQL_DRIVER":           src.DriverClass,
		"QMIGRATION_DAMENG_START_LSN":            task.CDCStartPositionValue,
		"QMIGRATION_DAMENG_TABLES":               strings.Join(tableNames, ","),
		"QMIGRATION_DAMENG_TLS_MODE":             string(src.TLSMode),
	}
	return &domain.RuntimeSpec{Engine: "native-dameng-logmnr", Format: "json", Filename: "native-dameng-logmnr.json", Content: string(b), Command: []string{"qmigration-dameng-cdc"}, Env: env}, nil
}

// NativeOracleCDCAdapter runs QMigration's Oracle LogMiner/SCN reader over the
// native TNS/TTC connector. It is experimental until qualified on supported
// Oracle releases, but it uses the same apply-before-ACK CDC runtime contract.
type NativeOracleCDCAdapter struct{}

func NewNativeOracleCDC() *NativeOracleCDCAdapter { return &NativeOracleCDCAdapter{} }
func (*NativeOracleCDCAdapter) Name() string      { return "native-oracle-cdc" }
func (*NativeOracleCDCAdapter) Info(_ context.Context) domain.EngineInfo {
	p, ok := lookPath("qmigration-oracle-cdc")
	return domain.EngineInfo{Name: "native-oracle-cdc", Available: ok, Binary: p, Modes: []string{"FULL_AND_INCREMENTAL", "INCREMENTAL"}, Note: "Built-in Oracle DBMS_LOGMNR/SCN reader over QMigration native TTC with flashback full-row reconstruction and apply-before-SCN-advance semantics"}
}
func (*NativeOracleCDCAdapter) Render(_ context.Context, task *domain.MigrationTask, src, _ domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if src.Type != domain.DataSourceOracle {
		return nil, fmt.Errorf("native-oracle-cdc requires Oracle source, got %s", src.Type)
	}
	if strings.ToUpper(strings.TrimSpace(task.CDCStartPositionType)) != "ORACLE_SCN" || strings.TrimSpace(task.CDCStartPositionValue) == "" {
		return nil, errors.New("native-oracle-cdc requires captured ORACLE_SCN start position")
	}
	tableNames := make([]string, 0, len(tables))
	for _, t := range tables {
		tableNames = append(tableNames, qualified(t.SourceSchema, t.SourceTable))
	}
	if len(tableNames) == 0 {
		return nil, errors.New("native-oracle-cdc requires planned tables")
	}
	plan := map[string]any{"position_type": "ORACLE_SCN", "position_value": task.CDCStartPositionValue, "tables": tableNames, "resource": "DBMS_LOGMNR"}
	b, _ := json.MarshalIndent(plan, "", "  ")
	env := map[string]string{
		"QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE":       "1",
		"QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC": "1",
		"QMIGRATION_ORACLE_HOST":                      src.Host,
		"QMIGRATION_ORACLE_PORT":                      fmt.Sprint(src.Port),
		"QMIGRATION_ORACLE_USER":                      src.Username,
		"QMIGRATION_ORACLE_PASSWORD":                  src.Password,
		"QMIGRATION_ORACLE_SERVICE":                   src.Database,
		"QMIGRATION_ORACLE_START_SCN":                 task.CDCStartPositionValue,
		"QMIGRATION_ORACLE_TABLES":                    strings.Join(tableNames, ","),
		"QMIGRATION_ORACLE_TLS_MODE":                  string(src.TLSMode),
		"QMIGRATION_ORACLE_TLS_SERVER_NAME":           src.TLSServerName,
		"QMIGRATION_ORACLE_TLS_CA":                    src.TLSCACert,
		"QMIGRATION_ORACLE_TLS_CLIENT_CERT":           src.TLSClientCert,
		"QMIGRATION_ORACLE_TLS_CLIENT_KEY":            src.TLSClientKey,
	}
	return &domain.RuntimeSpec{Engine: "native-oracle-cdc", Format: "json", Filename: "native-oracle-cdc.json", Content: string(b), Command: []string{"qmigration-oracle-cdc"}, Env: env}, nil
}
