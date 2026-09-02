package domain

import "time"

type DataSourceType string

const (
	DataSourceMySQL             DataSourceType = "mysql"
	DataSourceMariaDB           DataSourceType = "mariadb"
	DataSourcePolarDBX          DataSourceType = "polardbx"
	DataSourceTiDB              DataSourceType = "tidb"
	DataSourceOceanBase         DataSourceType = "oceanbase_mysql"
	DataSourcePolarDBMySQL      DataSourceType = "polardb_mysql"
	DataSourcePostgreSQL        DataSourceType = "postgresql"
	DataSourcePolarDBPostgreSQL DataSourceType = "polardb_postgresql"
	DataSourceOracle            DataSourceType = "oracle"
	DataSourceSQLServer         DataSourceType = "sqlserver"
	DataSourceDB2               DataSourceType = "db2"
	DataSourceOpenGauss         DataSourceType = "opengauss"
	DataSourceKingbase          DataSourceType = "kingbase"
	DataSourceDameng            DataSourceType = "dameng"
	DataSourceGaussDB           DataSourceType = "gaussdb"
	DataSourceGBase             DataSourceType = "gbase"
	DataSourceGBase8s           DataSourceType = "gbase8s"
)

func (t DataSourceType) IsMySQLFamily() bool {
	switch t {
	case DataSourceMySQL, DataSourceMariaDB, DataSourcePolarDBX, DataSourceTiDB, DataSourceOceanBase, DataSourcePolarDBMySQL:
		return true
	default:
		return false
	}
}

func (t DataSourceType) IsPostgreSQLFamily() bool {
	return t == DataSourcePostgreSQL || t == DataSourcePolarDBPostgreSQL
}

// IsPostgreSQLWireCompatible is broader than IsPostgreSQLFamily: these
// products speak the PostgreSQL frontend/backend protocol and can therefore
// reuse QMigration's native full-load connector. It deliberately does not imply
// pgoutput compatibility; CDC capability is advertised separately by the SPI.
func (t DataSourceType) IsPostgreSQLWireCompatible() bool {
	return t.IsPostgreSQLFamily() || t == DataSourceOpenGauss || t == DataSourceKingbase || t == DataSourceGaussDB
}

func (t DataSourceType) IsExternalJDBC() bool {
	// V0.15 no longer routes any built-in datasource through an external JDBC
	// migration runtime. GBase 8a entered the qualification-gated native data
	// plane in RC17. The method is retained for API/backward compatibility.
	return false
}

type TLSMode string

const (
	TLSModeDisable   TLSMode = "DISABLE"
	TLSModePreferred TLSMode = "PREFERRED"
	TLSModeRequired  TLSMode = "REQUIRED"
)

type DataSource struct {
	ID                     string         `json:"id"`
	Name                   string         `json:"name"`
	Type                   DataSourceType `json:"type"`
	Host                   string         `json:"host"`
	Port                   int            `json:"port"`
	Username               string         `json:"username"`
	Password               string         `json:"-"`
	PasswordCiphertext     string         `json:"-"`
	Database               string         `json:"database,omitempty"`
	Schema                 string         `json:"schema,omitempty"`
	JDBCURL                string         `json:"jdbc_url,omitempty"`
	DriverClass            string         `json:"driver_class,omitempty"`
	CDCURL                 string         `json:"cdc_url,omitempty"`
	TLSMode                TLSMode        `json:"tls_mode,omitempty"`
	TLSServerName          string         `json:"tls_server_name,omitempty"`
	TLSCACert              string         `json:"tls_ca_cert,omitempty"`
	TLSClientCert          string         `json:"tls_client_cert,omitempty"`
	TLSClientKey           string         `json:"-"`
	TLSClientKeyCiphertext string         `json:"-"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

// DataSourceCredential is only returned by the internal worker-claim API.
// Public datasource APIs always use DataSource and never serialize Password.
type DataSourceCredential struct {
	Type          DataSourceType `json:"type"`
	Host          string         `json:"host"`
	Port          int            `json:"port"`
	Username      string         `json:"username"`
	Password      string         `json:"password"`
	Database      string         `json:"database,omitempty"`
	Schema        string         `json:"schema,omitempty"`
	JDBCURL       string         `json:"jdbc_url,omitempty"`
	DriverClass   string         `json:"driver_class,omitempty"`
	CDCURL        string         `json:"cdc_url,omitempty"`
	TLSMode       TLSMode        `json:"tls_mode,omitempty"`
	TLSServerName string         `json:"tls_server_name,omitempty"`
	TLSCACert     string         `json:"tls_ca_cert,omitempty"`
	TLSClientCert string         `json:"tls_client_cert,omitempty"`
	TLSClientKey  string         `json:"tls_client_key,omitempty"`
}

func Credential(d DataSource) DataSourceCredential {
	return DataSourceCredential{Type: d.Type, Host: d.Host, Port: d.Port, Username: d.Username, Password: d.Password, Database: d.Database, Schema: d.Schema, JDBCURL: d.JDBCURL, DriverClass: d.DriverClass, CDCURL: d.CDCURL, TLSMode: d.TLSMode, TLSServerName: d.TLSServerName, TLSCACert: d.TLSCACert, TLSClientCert: d.TLSClientCert, TLSClientKey: d.TLSClientKey}
}

type MigrationStatus string

const (
	StatusCreated           MigrationStatus = "CREATED"
	StatusPrechecking       MigrationStatus = "PRECHECKING"
	StatusPrecheckSuccess   MigrationStatus = "PRECHECK_SUCCESS"
	StatusPreparing         MigrationStatus = "PREPARING"
	StatusCDCInitializing   MigrationStatus = "CDC_INITIALIZING"
	StatusFullMigrating     MigrationStatus = "FULL_MIGRATING"
	StatusFullFinished      MigrationStatus = "FULL_FINISHED"
	StatusCDCCatchingUp     MigrationStatus = "CDC_CATCHING_UP"
	StatusValidating        MigrationStatus = "VALIDATING"
	StatusReadyCutover      MigrationStatus = "READY_FOR_CUTOVER"
	StatusCutoverRunning    MigrationStatus = "CUTOVER_RUNNING"
	StatusFinished          MigrationStatus = "FINISHED"
	StatusPaused            MigrationStatus = "PAUSED"
	StatusFailed            MigrationStatus = "FAILED"
	StatusCancelled         MigrationStatus = "CANCELLED"
	StatusRollbackPreparing MigrationStatus = "ROLLBACK_PREPARING"
	StatusRollbackSyncing   MigrationStatus = "ROLLBACK_SYNCING"
	StatusRollbackReady     MigrationStatus = "ROLLBACK_READY"
	StatusRollbackRunning   MigrationStatus = "ROLLBACK_RUNNING"
	StatusRolledBack        MigrationStatus = "ROLLED_BACK"
)

type MigrationMode string

const (
	ModeFull               MigrationMode = "FULL"
	ModeIncremental        MigrationMode = "INCREMENTAL"
	ModeFullAndIncremental MigrationMode = "FULL_AND_INCREMENTAL"
)

type ColumnMapping struct {
	SourceColumn string `json:"source_column"`
	TargetColumn string `json:"target_column"`
}

type TableMapping struct {
	SourceSchema  string          `json:"source_schema"`
	SourceTable   string          `json:"source_table"`
	TargetSchema  string          `json:"target_schema,omitempty"`
	TargetTable   string          `json:"target_table,omitempty"`
	Columns       []ColumnMapping `json:"columns,omitempty"`
	SplitStrategy string          `json:"split_strategy,omitempty"` // AUTO / PRIMARY_KEY_RANGE / UNIQUE_KEY_RANGE / HASH / PARTITION / CUSTOM_SQL
	Partitions    []string        `json:"partitions,omitempty"`
	HashBuckets   int             `json:"hash_buckets,omitempty"`
	CustomWhere   string          `json:"custom_where,omitempty"`
}

// RateLimitWindow overrides task throttling inside a local clock window.
// Windows may cross midnight (for example 18:00-08:00).
type RateLimitWindow struct {
	Start                string `json:"start"`
	End                  string `json:"end"`
	ReadLimitMBps        int64  `json:"read_limit_mbps,omitempty"`
	WriteLimitMBps       int64  `json:"write_limit_mbps,omitempty"`
	TargetThroughputMBps int64  `json:"target_throughput_mbps,omitempty"`
	RowsPerSecond        int64  `json:"rows_per_second,omitempty"`
	QPS                  int    `json:"qps,omitempty"`
	Parallelism          int    `json:"parallelism,omitempty"`
}

// TransformAction is a safe, declarative QMigration value transformation.
// Rules never execute user SQL or arbitrary code; they operate on connector-
// neutral row images inside the unified Full Load pipeline.
type TransformAction string

const (
	TransformTrim            TransformAction = "TRIM"
	TransformLower           TransformAction = "LOWER"
	TransformUpper           TransformAction = "UPPER"
	TransformEmptyToNull     TransformAction = "EMPTY_TO_NULL"
	TransformNullToValue     TransformAction = "NULL_TO_VALUE"
	TransformReplaceLiteral  TransformAction = "REPLACE_LITERAL"
	TransformZeroDateToNull  TransformAction = "ZERO_DATE_TO_NULL"
	TransformZeroDateToValue TransformAction = "ZERO_DATE_TO_VALUE"
	TransformJSONCompact     TransformAction = "JSON_COMPACT"
)

// TransformRule targets one source column. Empty schema/table values are
// wildcards, allowing a task-wide policy while still supporting table-specific
// overrides. Rules are applied in declaration order.
type TransformRule struct {
	SourceSchema string          `json:"source_schema,omitempty"`
	SourceTable  string          `json:"source_table,omitempty"`
	Column       string          `json:"column"`
	Action       TransformAction `json:"action"`
	Match        string          `json:"match,omitempty"`
	Value        string          `json:"value,omitempty"`
}

type MigrationTask struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	SourceID         string          `json:"source_datasource_id"`
	TargetID         string          `json:"target_datasource_id"`
	Mode             MigrationMode   `json:"mode"`
	Status           MigrationStatus `json:"status"`
	PausedFromStatus MigrationStatus `json:"paused_from_status,omitempty"`
	// Engine fields are retained for metadata/API compatibility. V0.15+ always
	// normalizes them to "qmigration"; users do not select third-party runtimes.
	FullEngine                     string            `json:"full_engine"`
	CDCEngine                      string            `json:"cdc_engine,omitempty"`
	RollbackCDCEngine              string            `json:"rollback_cdc_engine,omitempty"`
	CDCDDLMode                     string            `json:"cdc_ddl_mode,omitempty"`
	CDCConflictMode                string            `json:"cdc_conflict_mode,omitempty"`
	CDCConflictColumn              string            `json:"cdc_conflict_column,omitempty"`
	SequenceSyncedAt               time.Time         `json:"sequence_synced_at,omitempty"`
	Tables                         []TableMapping    `json:"tables,omitempty"`
	ChunkRows                      int64             `json:"chunk_rows"`
	BatchRows                      int               `json:"batch_rows"`
	Parallelism                    int               `json:"parallelism"`
	MaxRetries                     int               `json:"max_retries"`
	AutoCreateTable                bool              `json:"auto_create_table"`
	ValidationEnabled              bool              `json:"validation_enabled"`
	ValidationMode                 string            `json:"validation_mode,omitempty"`
	PostLoadDDLMode                string            `json:"post_load_ddl_mode,omitempty"`
	ReadLimitMBps                  int64             `json:"read_limit_mbps,omitempty"`
	WriteLimitMBps                 int64             `json:"write_limit_mbps,omitempty"`
	TargetThroughputMBps           int64             `json:"target_throughput_mbps,omitempty"`
	AutoThroughputEnabled          bool              `json:"auto_throughput_enabled,omitempty"`
	CompletionSLASeconds           int64             `json:"completion_sla_seconds,omitempty"`
	SLAStartedAt                   time.Time         `json:"sla_started_at,omitempty"`
	ControllerTargetBytesSec       int64             `json:"controller_target_bytes_sec,omitempty"`
	ThroughputControllerReason     string            `json:"throughput_controller_reason,omitempty"`
	AdaptiveHotspotSplits          int64             `json:"adaptive_hotspot_splits,omitempty"`
	AdaptiveRunningYields          int64             `json:"adaptive_running_yields,omitempty"`
	AdaptiveTopologyDrains         int64             `json:"adaptive_topology_drains,omitempty"`
	AdaptiveTopologyDegradedYields int64             `json:"adaptive_topology_degraded_yields,omitempty"`
	AdaptiveFaultDomainYields      int64             `json:"adaptive_fault_domain_yields,omitempty"`
	ControllerAutoProbePct         int               `json:"controller_auto_probe_pct,omitempty"`
	ControllerSLAHeadroomPct       int               `json:"controller_sla_headroom_pct,omitempty"`
	ControllerLearningSamples      int64             `json:"controller_learning_samples,omitempty"`
	RowsLimitPerSec                int64             `json:"rows_limit_per_sec,omitempty"`
	QPSLimit                       int               `json:"qps_limit,omitempty"`
	RateLimitTimezone              string            `json:"rate_limit_timezone,omitempty"`
	RateLimitWindows               []RateLimitWindow `json:"rate_limit_windows,omitempty"`
	TransformRules                 []TransformRule   `json:"transform_rules,omitempty"`
	WorkerSelector                 map[string]string `json:"worker_selector,omitempty"`
	WorkerAffinity                 string            `json:"worker_affinity,omitempty"`
	EffectiveParallelism           int               `json:"effective_parallelism,omitempty"`
	FlowControlLevel               string            `json:"flow_control_level,omitempty"`
	FlowControlReason              string            `json:"flow_control_reason,omitempty"`
	CDCSpoolGrowthBytesSec         int64             `json:"cdc_spool_growth_bytes_sec,omitempty"`
	CDCSpoolCriticalETASeconds     int64             `json:"cdc_spool_critical_eta_seconds,omitempty"`
	Progress                       float64           `json:"progress"`
	TotalChunks                    int               `json:"total_chunks"`
	FinishedChunks                 int               `json:"finished_chunks"`
	RowsMigrated                   int64             `json:"rows_migrated"`
	BytesMigrated                  int64             `json:"bytes_migrated"`
	SpeedBytesSec                  int64             `json:"speed_bytes_sec"`
	SpeedRowsSec                   int64             `json:"speed_rows_sec"`
	ETASeconds                     int64             `json:"eta_seconds"`
	SLAP95ETASeconds               int64             `json:"sla_p95_eta_seconds,omitempty"`
	SLAP99ETASeconds               int64             `json:"sla_p99_eta_seconds,omitempty"`
	SLARiskLevel                   string            `json:"sla_risk_level,omitempty"`
	SLARiskReason                  string            `json:"sla_risk_reason,omitempty"`
	CDCLagMS                       int64             `json:"cdc_lag_ms"`
	CDCStartTimestampMS            int64             `json:"cdc_start_timestamp_ms,omitempty"`
	CDCStartPositionType           string            `json:"cdc_start_position_type,omitempty"`
	CDCStartPositionValue          string            `json:"cdc_start_position_value,omitempty"`
	CDCStartResource               string            `json:"cdc_start_resource,omitempty"`
	ValidationBarrierPositionType  string            `json:"validation_barrier_position_type,omitempty"`
	ValidationBarrierPositionValue string            `json:"validation_barrier_position_value,omitempty"`
	ValidationBarrierResource      string            `json:"validation_barrier_resource,omitempty"`
	ValidationBarrierCapturedAt    time.Time         `json:"validation_barrier_captured_at,omitempty"`
	LastError                      string            `json:"last_error,omitempty"`
	CreatedAt                      time.Time         `json:"created_at"`
	UpdatedAt                      time.Time         `json:"updated_at"`
}

type Worker struct {
	ID                 string            `json:"id"`
	Hostname           string            `json:"hostname"`
	CPU                int               `json:"cpu"`
	MemoryMB           int               `json:"memory_mb"`
	Status             string            `json:"status"`
	RunningJobs        int               `json:"running_jobs"`
	CPUUsagePct        float64           `json:"cpu_usage_pct,omitempty"`
	MemoryUsagePct     float64           `json:"memory_usage_pct,omitempty"`
	NetworkRxBps       int64             `json:"network_rx_bps,omitempty"`
	NetworkTxBps       int64             `json:"network_tx_bps,omitempty"`
	Capabilities       []string          `json:"capabilities"`
	Labels             map[string]string `json:"labels,omitempty"`
	LastHeartbeat      time.Time         `json:"last_heartbeat"`
	SchedulerLoadScore float64           `json:"scheduler_load_score,omitempty"`
}

type SchemaInfo struct {
	Name string `json:"name"`
}

type TableInfo struct {
	Schema     string `json:"schema"`
	Name       string `json:"name"`
	Rows       int64  `json:"rows"`
	DataLength int64  `json:"data_length"`
}

type SchemaObjectType string

const (
	SchemaObjectView      SchemaObjectType = "VIEW"
	SchemaObjectSequence  SchemaObjectType = "SEQUENCE"
	SchemaObjectTrigger   SchemaObjectType = "TRIGGER"
	SchemaObjectFunction  SchemaObjectType = "FUNCTION"
	SchemaObjectProcedure SchemaObjectType = "PROCEDURE"
)

// SchemaObject represents non-table database objects that may need conversion
// during heterogeneous migrations. DDL is best-effort native source DDL.
type SchemaObject struct {
	Schema            string           `json:"schema"`
	Name              string           `json:"name"`
	Type              SchemaObjectType `json:"type"`
	RelatedTo         string           `json:"related_to,omitempty"`
	DDL               string           `json:"ddl,omitempty"`
	Definition        string           `json:"definition,omitempty"`
	Dependencies      []string         `json:"dependencies,omitempty"`
	DependenciesKnown bool             `json:"dependencies_known,omitempty"`
	BindingKnown      bool             `json:"binding_known,omitempty"`
}

type SchemaObjectPlanAction string

const (
	SchemaObjectApplySafe    SchemaObjectPlanAction = "APPLY_SAFE"
	SchemaObjectSyncSequence SchemaObjectPlanAction = "SYNC_SEQUENCE"
	SchemaObjectSkipExisting SchemaObjectPlanAction = "SKIP_EXISTING"
	SchemaObjectManual       SchemaObjectPlanAction = "MANUAL"
)

type SchemaObjectPlanItem struct {
	Source                    SchemaObject           `json:"source"`
	TargetSchema              string                 `json:"target_schema"`
	TargetName                string                 `json:"target_name"`
	TargetRelatedTo           string                 `json:"target_related_to,omitempty"`
	TargetExists              bool                   `json:"target_exists"`
	Action                    SchemaObjectPlanAction `json:"action"`
	Reason                    string                 `json:"reason"`
	TargetDDL                 string                 `json:"target_ddl,omitempty"`
	TranslationEvidenceSHA256 string                 `json:"translation_evidence_sha256,omitempty"`
}

type SchemaObjectPlan struct {
	TaskID      string                 `json:"task_id"`
	SafeActions int                    `json:"safe_actions"`
	Manual      int                    `json:"manual"`
	Skipped     int                    `json:"skipped"`
	Items       []SchemaObjectPlanItem `json:"items"`
	PlannedAt   time.Time              `json:"planned_at"`
}

type SchemaObjectApplyRequest struct {
	Confirm bool               `json:"confirm"`
	Types   []SchemaObjectType `json:"types,omitempty"`
}

type SchemaObjectApplyItemResult struct {
	Type         SchemaObjectType `json:"type"`
	SourceSchema string           `json:"source_schema"`
	Name         string           `json:"name"`
	TargetSchema string           `json:"target_schema"`
	Status       string           `json:"status"`
	Message      string           `json:"message,omitempty"`
}

type SchemaObjectApplyResult struct {
	TaskID  string                        `json:"task_id"`
	Applied int                           `json:"applied"`
	Skipped int                           `json:"skipped"`
	Failed  int                           `json:"failed"`
	Items   []SchemaObjectApplyItemResult `json:"items"`
}

type ColumnInfo struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	ColumnType string `json:"column_type"`
	Nullable   bool   `json:"nullable"`
	Extra      string `json:"extra,omitempty"`
	Ordinal    int    `json:"ordinal"`
	PrimaryKey bool   `json:"primary_key"`
}

type IndexInfo struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
	Primary bool     `json:"primary"`
}

type ForeignKeyInfo struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns"`
	ReferencedSchema  string   `json:"referenced_schema"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns"`
}

type TopologyPlacement struct {
	ID      string            `json:"id"`
	Kind    string            `json:"kind"`
	Labels  map[string]string `json:"labels,omitempty"`
	Address string            `json:"address,omitempty"`
	Weight  int64             `json:"weight,omitempty"`
}

type DatabaseRuntimeLoad struct {
	Connections        int64   `json:"connections,omitempty"`
	RunningQueries     int64   `json:"running_queries,omitempty"`
	MaxConnections     int64   `json:"max_connections,omitempty"`
	ConnectionUsagePct float64 `json:"connection_usage_pct,omitempty"`
}

type TableMetadata struct {
	Schema            string           `json:"schema"`
	Name              string           `json:"name"`
	Columns           []ColumnInfo     `json:"columns"`
	Indexes           []IndexInfo      `json:"indexes,omitempty"`
	ForeignKeys       []ForeignKeyInfo `json:"foreign_keys,omitempty"`
	PrimaryKeys       []string         `json:"primary_keys,omitempty"`
	PrimaryKey        string           `json:"primary_key,omitempty"`
	PrimaryKeyType    string           `json:"primary_key_type,omitempty"`
	PrimaryKeyNumeric bool             `json:"primary_key_numeric"`
	MinPK             int64            `json:"min_pk"`
	MaxPK             int64            `json:"max_pk"`
	EstimatedRows     int64            `json:"estimated_rows"`
	DataLength        int64            `json:"data_length"`
	HasRows           bool             `json:"has_rows"`
}

type TableTopologyPerformance struct {
	BytesPerSec            int64     `json:"bytes_per_sec,omitempty"`
	RowsPerSec             int64     `json:"rows_per_sec,omitempty"`
	RecommendedChunkRows   int64     `json:"recommended_chunk_rows,omitempty"`
	Samples                int64     `json:"samples,omitempty"`
	DurationSamplesMS      []int64   `json:"duration_samples_ms,omitempty"`
	P95DurationMS          int64     `json:"p95_duration_ms,omitempty"`
	P99DurationMS          int64     `json:"p99_duration_ms,omitempty"`
	Health                 string    `json:"health,omitempty"`
	SlowStreak             int       `json:"slow_streak,omitempty"`
	GoodStreak             int       `json:"good_streak,omitempty"`
	RecoveryConcurrencyCap int       `json:"recovery_concurrency_cap,omitempty"`
	HealthChangedAt        time.Time `json:"health_changed_at,omitempty"`
	LastProbeAt            time.Time `json:"last_probe_at,omitempty"`
}

type MigrationTable struct {
	ID                   string                              `json:"id"`
	TaskID               string                              `json:"task_id"`
	Engine               string                              `json:"engine,omitempty"`
	SourceSchema         string                              `json:"source_schema"`
	SourceTable          string                              `json:"source_table"`
	TargetSchema         string                              `json:"target_schema"`
	TargetTable          string                              `json:"target_table"`
	PrimaryKeys          []string                            `json:"primary_keys,omitempty"`
	TargetPrimaryKeys    []string                            `json:"target_primary_keys,omitempty"`
	PrimaryKey           string                              `json:"primary_key"`
	TargetPrimaryKey     string                              `json:"target_primary_key,omitempty"`
	PrimaryKeyType       string                              `json:"primary_key_type"`
	Columns              []ColumnInfo                        `json:"columns"`
	TargetColumns        []ColumnInfo                        `json:"target_columns,omitempty"`
	Indexes              []IndexInfo                         `json:"indexes,omitempty"`
	ForeignKeys          []ForeignKeyInfo                    `json:"foreign_keys,omitempty"`
	Partitions           []string                            `json:"partitions,omitempty"`
	SplitStrategy        string                              `json:"split_strategy,omitempty"`
	CustomWhere          string                              `json:"custom_where,omitempty"`
	HashBuckets          int                                 `json:"hash_buckets,omitempty"`
	Topology             []TopologyPlacement                 `json:"topology,omitempty"`
	EstimatedRows        int64                               `json:"estimated_rows"`
	DataLength           int64                               `json:"data_length"`
	MinPK                int64                               `json:"min_pk"`
	MaxPK                int64                               `json:"max_pk"`
	TotalChunks          int                                 `json:"total_chunks"`
	FinishedChunks       int                                 `json:"finished_chunks"`
	RowsMigrated         int64                               `json:"rows_migrated"`
	BytesMigrated        int64                               `json:"bytes_migrated"`
	ProfileBytesPerSec   int64                               `json:"profile_bytes_per_sec,omitempty"`
	ProfileRowsPerSec    int64                               `json:"profile_rows_per_sec,omitempty"`
	RecommendedChunkRows int64                               `json:"recommended_chunk_rows,omitempty"`
	PerformanceSamples   int64                               `json:"performance_samples,omitempty"`
	TopologyPerformance  map[string]TableTopologyPerformance `json:"topology_performance,omitempty"`
	Status               string                              `json:"status"`
}

type ChunkStatus string

const (
	ChunkPending ChunkStatus = "PENDING"
	ChunkRunning ChunkStatus = "RUNNING"
	ChunkSuccess ChunkStatus = "SUCCESS"
	ChunkFailed  ChunkStatus = "FAILED"
	ChunkStale   ChunkStatus = "STALE"
)

type MigrationChunk struct {
	ID                string            `json:"id"`
	TaskID            string            `json:"task_id"`
	TableID           string            `json:"table_id"`
	ChunkNo           int               `json:"chunk_no"`
	SplitType         string            `json:"split_type"`
	PrimaryKey        string            `json:"primary_key"`
	Start             int64             `json:"start"`
	End               int64             `json:"end"`
	StartCursorJSON   string            `json:"start_cursor_json,omitempty"`
	EndCursorJSON     string            `json:"end_cursor_json,omitempty"`
	CursorJSON        string            `json:"cursor_json,omitempty"`
	Status            ChunkStatus       `json:"status"`
	WorkerID          string            `json:"worker_id,omitempty"`
	LeaseUntil        time.Time         `json:"lease_until,omitempty"`
	RowsRead          int64             `json:"rows_read"`
	RowsWritten       int64             `json:"rows_written"`
	BytesRead         int64             `json:"bytes_read"`
	BytesWritten      int64             `json:"bytes_written"`
	RetryCount        int               `json:"retry_count"`
	LastError         string            `json:"last_error,omitempty"`
	LastReadMS        int64             `json:"last_read_ms,omitempty"`
	LastWriteMS       int64             `json:"last_write_ms,omitempty"`
	LastBatchRows     int               `json:"last_batch_rows,omitempty"`
	BackpressureLevel string            `json:"backpressure_level,omitempty"`
	PlacementHint     map[string]string `json:"placement_hint,omitempty"`
	FaultDomain       map[string]string `json:"fault_domain,omitempty"`
	TopologyID        string            `json:"topology_id,omitempty"`
	TopologyKind      string            `json:"topology_kind,omitempty"`
	PartitionName     string            `json:"partition_name,omitempty"`
	HashBucket        int               `json:"hash_bucket,omitempty"`
	HashBuckets       int               `json:"hash_buckets,omitempty"`
	CustomWhere       string            `json:"custom_where,omitempty"`
	StartedAt         time.Time         `json:"started_at,omitempty"`
	FinishedAt        time.Time         `json:"finished_at,omitempty"`
}

type ChunkJob struct {
	Chunk             MigrationChunk       `json:"chunk"`
	Table             MigrationTable       `json:"table"`
	Engine            string               `json:"engine,omitempty"`
	Source            DataSourceCredential `json:"source"`
	Target            DataSourceCredential `json:"target"`
	BatchRows         int                  `json:"batch_rows"`
	ReadLimitMBps     int64                `json:"read_limit_mbps,omitempty"`
	WriteLimitMBps    int64                `json:"write_limit_mbps,omitempty"`
	ReadBytesPerSec   int64                `json:"read_bytes_per_sec,omitempty"`
	WriteBytesPerSec  int64                `json:"write_bytes_per_sec,omitempty"`
	TargetBytesPerSec int64                `json:"target_bytes_per_sec,omitempty"`
	RowsPerSecond     int64                `json:"rows_per_second,omitempty"`
	QPS               int                  `json:"qps,omitempty"`
	TransformRules    []TransformRule      `json:"transform_rules,omitempty"`
	WorkSteal         bool                 `json:"work_steal,omitempty"`
	WorkStealReason   string               `json:"work_steal_reason,omitempty"`
}

type ChunkResult struct {
	RowsRead     int64  `json:"rows_read"`
	RowsWritten  int64  `json:"rows_written"`
	BytesRead    int64  `json:"bytes_read"`
	BytesWritten int64  `json:"bytes_written"`
	DurationMS   int64  `json:"duration_ms"`
	CursorJSON   string `json:"cursor_json,omitempty"`
	Yielded      bool   `json:"yielded,omitempty"`
	YieldReason  string `json:"yield_reason,omitempty"`
	Error        string `json:"error,omitempty"`
}

type ChunkProgress struct {
	CursorJSON        string `json:"cursor_json,omitempty"`
	RowsRead          int64  `json:"rows_read,omitempty"`
	RowsWritten       int64  `json:"rows_written,omitempty"`
	BytesRead         int64  `json:"bytes_read,omitempty"`
	BytesWritten      int64  `json:"bytes_written,omitempty"`
	LastReadMS        int64  `json:"last_read_ms,omitempty"`
	LastWriteMS       int64  `json:"last_write_ms,omitempty"`
	LastBatchRows     int    `json:"last_batch_rows,omitempty"`
	BackpressureLevel string `json:"backpressure_level,omitempty"`
}

// ChunkControl is returned on every lease renewal. It closes the feedback loop
// between observed source/target latency and the worker's next batch without
// requiring the control plane to revoke a running chunk.
type ChunkControl struct {
	PauseMS           int    `json:"pause_ms,omitempty"`
	MaxBatchRows      int    `json:"max_batch_rows,omitempty"`
	TargetBatchRows   int    `json:"target_batch_rows,omitempty"`
	ReadBytesPerSec   int64  `json:"read_bytes_per_sec,omitempty"`
	WriteBytesPerSec  int64  `json:"write_bytes_per_sec,omitempty"`
	TargetBytesPerSec int64  `json:"target_bytes_per_sec,omitempty"`
	YieldAfterBatch   bool   `json:"yield_after_batch,omitempty"`
	YieldReason       string `json:"yield_reason,omitempty"`
	Level             string `json:"level,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

type PrecheckLevel string

const (
	PrecheckPass    PrecheckLevel = "PASS"
	PrecheckWarning PrecheckLevel = "WARNING"
	PrecheckFailed  PrecheckLevel = "FAILED"
)

type PrecheckItem struct {
	Name    string        `json:"name"`
	Level   PrecheckLevel `json:"level"`
	Message string        `json:"message"`
}

type ValidationStatus string

const (
	ValidationPending  ValidationStatus = "PENDING"
	ValidationRunning  ValidationStatus = "RUNNING"
	ValidationSuccess  ValidationStatus = "SUCCESS"
	ValidationMismatch ValidationStatus = "MISMATCH"
	ValidationError    ValidationStatus = "ERROR"
)

type ValidationResult struct {
	ID             string           `json:"id"`
	TaskID         string           `json:"task_id"`
	TableID        string           `json:"table_id"`
	ChunkID        string           `json:"chunk_id"`
	Status         ValidationStatus `json:"status"`
	SourceRows     int64            `json:"source_rows"`
	TargetRows     int64            `json:"target_rows"`
	SourceChecksum string           `json:"source_checksum"`
	TargetChecksum string           `json:"target_checksum"`
	LastError      string           `json:"last_error,omitempty"`
	StartedAt      time.Time        `json:"started_at"`
	FinishedAt     time.Time        `json:"finished_at"`
}

// ValidationArchive is an immutable audit proof created after a migration reaches
// a terminal state. Detailed per-chunk validation attempts may later be compacted,
// but this summary and its evidence digests are retained permanently.
type ValidationArchive struct {
	TaskID                        string                   `json:"task_id"`
	TerminalStatus                MigrationStatus          `json:"terminal_status"`
	ValidationMode                string                   `json:"validation_mode"`
	ValidationBarrierPositionType string                   `json:"validation_barrier_position_type,omitempty"`
	ValidationBarrierPosition     string                   `json:"validation_barrier_position_value,omitempty"`
	ValidationBarrierResource     string                   `json:"validation_barrier_resource,omitempty"`
	TotalTables                   int                      `json:"total_tables"`
	TotalChunks                   int                      `json:"total_chunks"`
	CoveredChunks                 int                      `json:"covered_chunks"`
	SuccessChunks                 int                      `json:"success_chunks"`
	MismatchChunks                int                      `json:"mismatch_chunks"`
	ErrorChunks                   int                      `json:"error_chunks"`
	MissingChunks                 int                      `json:"missing_chunks"`
	EvidenceDigest                string                   `json:"evidence_digest"`
	Tables                        []ValidationTableArchive `json:"tables"`
	ArchivedAt                    time.Time                `json:"archived_at"`
}

// ValidationTableArchive summarizes the latest validation evidence for all
// successful full-load chunks of a table at archive time.
// ValidationReportArchiveRecord permanently registers the external immutable
// delivery location for a sealed validation report. The record is append-only
// per task+evidence digest and is never pruned by metadata maintenance.
type ValidationReportArchiveRecord struct {
	TaskID                     string    `json:"task_id"`
	EvidenceDigest             string    `json:"evidence_digest"`
	URI                        string    `json:"uri"`
	Bucket                     string    `json:"bucket,omitempty"`
	Prefix                     string    `json:"prefix,omitempty"`
	ManifestSHA256             string    `json:"manifest_sha256"`
	PublicSignatureAlgorithm   string    `json:"public_signature_algorithm,omitempty"`
	PublicSignatureKeyID       string    `json:"public_signature_key_id,omitempty"`
	PublicKeyEd25519           string    `json:"public_key_ed25519,omitempty"`
	PublicKeyFingerprintSHA256 string    `json:"public_key_fingerprint_sha256,omitempty"`
	ObjectLockMode             string    `json:"object_lock_mode,omitempty"`
	RetainUntil                string    `json:"retain_until,omitempty"`
	LegalHold                  bool      `json:"legal_hold,omitempty"`
	CommittedAt                time.Time `json:"committed_at"`
}

type ValidationTableArchive struct {
	TableID              string    `json:"table_id"`
	SourceSchema         string    `json:"source_schema"`
	SourceTable          string    `json:"source_table"`
	TargetSchema         string    `json:"target_schema"`
	TargetTable          string    `json:"target_table"`
	EvidenceScope        string    `json:"evidence_scope"` // CHUNK_SET / TABLE_UNION / MIXED
	ChecksumKind         string    `json:"checksum_kind"`  // CHUNK_SET_SHA256 / TABLE_UNION / EVIDENCE_SHA256
	TotalChunks          int       `json:"total_chunks"`
	CoveredChunks        int       `json:"covered_chunks"`
	SuccessChunks        int       `json:"success_chunks"`
	MismatchChunks       int       `json:"mismatch_chunks"`
	ErrorChunks          int       `json:"error_chunks"`
	MissingChunks        int       `json:"missing_chunks"`
	SourceRows           int64     `json:"source_rows"`
	TargetRows           int64     `json:"target_rows"`
	SourceChecksum       string    `json:"source_checksum,omitempty"`
	TargetChecksum       string    `json:"target_checksum,omitempty"`
	SourceChecksumDigest string    `json:"source_checksum_digest,omitempty"`
	TargetChecksumDigest string    `json:"target_checksum_digest,omitempty"`
	EvidenceDigest       string    `json:"evidence_digest"`
	FirstStartedAt       time.Time `json:"first_started_at,omitempty"`
	LastFinishedAt       time.Time `json:"last_finished_at,omitempty"`
}

type Alert struct {
	ID           string    `json:"id"`
	Severity     string    `json:"severity"`
	Title        string    `json:"title"`
	Message      string    `json:"message"`
	TaskID       string    `json:"task_id,omitempty"`
	Acknowledged bool      `json:"acknowledged"`
	CreatedAt    time.Time `json:"created_at"`
}

type AuditEvent struct {
	ID           string    `json:"id"`
	Actor        string    `json:"actor"`
	Action       string    `json:"action"`
	ResourceType string    `json:"resource_type"`
	ResourceID   string    `json:"resource_id,omitempty"`
	Detail       string    `json:"detail,omitempty"`
	RemoteAddr   string    `json:"remote_addr,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type TaskLog struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	WorkerID  string    `json:"worker_id,omitempty"`
	TableID   string    `json:"table_id,omitempty"`
	ChunkID   string    `json:"chunk_id,omitempty"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type UniversalDataType string

const (
	UniversalString    UniversalDataType = "STRING"
	UniversalInteger   UniversalDataType = "INTEGER"
	UniversalBigInt    UniversalDataType = "BIGINT"
	UniversalDecimal   UniversalDataType = "DECIMAL"
	UniversalFloat     UniversalDataType = "FLOAT"
	UniversalDouble    UniversalDataType = "DOUBLE"
	UniversalDate      UniversalDataType = "DATE"
	UniversalTime      UniversalDataType = "TIME"
	UniversalTimestamp UniversalDataType = "TIMESTAMP"
	UniversalBoolean   UniversalDataType = "BOOLEAN"
	UniversalBinary    UniversalDataType = "BINARY"
	UniversalText      UniversalDataType = "TEXT"
	UniversalJSON      UniversalDataType = "JSON"
	UniversalUUID      UniversalDataType = "UUID"
	UniversalUnknown   UniversalDataType = "UNKNOWN"
)

type UniversalColumn struct {
	Name       string            `json:"name"`
	Type       UniversalDataType `json:"type"`
	SourceType string            `json:"source_type"`
	Length     int64             `json:"length,omitempty"`
	Precision  int               `json:"precision,omitempty"`
	Scale      int               `json:"scale,omitempty"`
	Nullable   bool              `json:"nullable"`
	PrimaryKey bool              `json:"primary_key,omitempty"`
}

type UniversalTable struct {
	Database    string            `json:"database,omitempty"`
	Schema      string            `json:"schema"`
	Name        string            `json:"name"`
	Columns     []UniversalColumn `json:"columns"`
	Indexes     []IndexInfo       `json:"indexes,omitempty"`
	Constraints []ForeignKeyInfo  `json:"constraints,omitempty"`
}

type CDCOperation string

const (
	CDCInsert     CDCOperation = "INSERT"
	CDCUpdate     CDCOperation = "UPDATE"
	CDCDelete     CDCOperation = "DELETE"
	CDCTruncate   CDCOperation = "TRUNCATE"
	CDCDDL        CDCOperation = "DDL"
	CDCCheckpoint CDCOperation = "CHECKPOINT"
)

// CDCField uses an explicit encoding so event bridges can safely transport
// binary data without guessing whether a JSON string is text or bytes.
type CDCField struct {
	Column   string `json:"column"`
	Null     bool   `json:"null,omitempty"`
	Value    string `json:"value,omitempty"`
	Encoding string `json:"encoding,omitempty"` // text (default) or base64
}

type CDCEvent struct {
	ID                string       `json:"id,omitempty"`
	Operation         CDCOperation `json:"operation"`
	SourceSchema      string       `json:"source_schema"`
	SourceTable       string       `json:"source_table"`
	SQL               string       `json:"sql,omitempty"`
	Before            []CDCField   `json:"before,omitempty"`
	After             []CDCField   `json:"after,omitempty"`
	PositionType      string       `json:"position_type,omitempty"`
	PositionValue     string       `json:"position_value,omitempty"`
	Resource          string       `json:"resource,omitempty"`
	SourceTimestampMS int64        `json:"source_timestamp_ms,omitempty"`
}

type CDCApplyRequest struct {
	Direction string     `json:"direction,omitempty"`
	Events    []CDCEvent `json:"events"`
}

type CDCApplyResult struct {
	Applied          int    `json:"applied"`
	LastEventID      string `json:"last_event_id,omitempty"`
	PositionType     string `json:"position_type,omitempty"`
	PositionValue    string `json:"position_value,omitempty"`
	Duplicate        bool   `json:"duplicate,omitempty"`
	Staged           bool   `json:"staged,omitempty"`
	SpoolSequence    int64  `json:"spool_sequence,omitempty"`
	SkippedConflicts int    `json:"skipped_conflicts,omitempty"`
}

type CDCDeadLetterStatus string

const (
	CDCDeadLetterOpen            CDCDeadLetterStatus = "OPEN"
	CDCDeadLetterCommitUncertain CDCDeadLetterStatus = "COMMIT_UNCERTAIN"
	CDCDeadLetterReplayRequired  CDCDeadLetterStatus = "REPLAY_REQUIRED"
	CDCDeadLetterResolved        CDCDeadLetterStatus = "RESOLVED"
)

// CDCDeadLetter keeps a failed managed CDC transaction and its durable source
// position. The source reader must still leave the source checkpoint unacked;
// this record is an operator-visible recovery aid, not a replacement for WAL/
// binlog retention.
type CDCDeadLetter struct {
	ID               string              `json:"id"`
	TaskID           string              `json:"task_id"`
	Direction        string              `json:"direction"`
	PositionType     string              `json:"position_type,omitempty"`
	PositionValue    string              `json:"position_value,omitempty"`
	Resource         string              `json:"resource,omitempty"`
	Events           []CDCEvent          `json:"events,omitempty"`
	EventsCiphertext string              `json:"-"`
	LastError        string              `json:"last_error"`
	RetryCount       int                 `json:"retry_count"`
	Status           CDCDeadLetterStatus `json:"status"`
	CreatedAt        time.Time           `json:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at"`
	ResolvedAt       time.Time           `json:"resolved_at,omitempty"`
}

type CDCConflictDecision string

const (
	CDCConflictTargetKept    CDCConflictDecision = "TARGET_KEPT"
	CDCConflictSourceApplied CDCConflictDecision = "SOURCE_APPLIED"
)

type CDCConflictRecord struct {
	ID             string              `json:"id"`
	TaskID         string              `json:"task_id"`
	Direction      string              `json:"direction"`
	SourceSchema   string              `json:"source_schema"`
	SourceTable    string              `json:"source_table"`
	KeyFingerprint string              `json:"key_fingerprint"`
	Policy         string              `json:"policy"`
	Decision       CDCConflictDecision `json:"decision"`
	SourceVersion  string              `json:"source_version,omitempty"`
	TargetVersion  string              `json:"target_version,omitempty"`
	PositionType   string              `json:"position_type,omitempty"`
	PositionValue  string              `json:"position_value,omitempty"`
	CreatedAt      time.Time           `json:"created_at"`
}

type CDCPosition struct {
	ID                string    `json:"id"`
	TaskID            string    `json:"task_id"`
	Direction         string    `json:"direction,omitempty"` // forward or reverse
	DatabaseType      string    `json:"database_type,omitempty"`
	PositionType      string    `json:"position_type,omitempty"`
	PositionValue     string    `json:"position_value,omitempty"`
	Resource          string    `json:"resource,omitempty"`
	SourceTimestampMS int64     `json:"source_timestamp_ms,omitempty"`
	ApplyTimestampMS  int64     `json:"apply_timestamp_ms,omitempty"`
	LagMS             int64     `json:"lag_ms"`
	EventsTotal       int64     `json:"events_total,omitempty"`
	EventsPending     int64     `json:"events_pending,omitempty"`
	RecordedAt        time.Time `json:"recorded_at"`
}

// CDCSpoolRecord is a durable transaction staged by QMigration before the
// target apply gate opens. Events are encrypted at rest by the secure
// repository wrapper. Sequence is repository-assigned and preserves source
// transaction order for deterministic draining after the snapshot finishes.
type CDCSpoolStatus string

const (
	CDCSpoolPending CDCSpoolStatus = "PENDING"
	CDCSpoolApplied CDCSpoolStatus = "APPLIED"
)

type CDCSpoolRecord struct {
	ID                string         `json:"id"`
	TaskID            string         `json:"task_id"`
	Direction         string         `json:"direction"`
	Sequence          int64          `json:"sequence"`
	PositionType      string         `json:"position_type,omitempty"`
	PositionValue     string         `json:"position_value,omitempty"`
	Resource          string         `json:"resource,omitempty"`
	SourceTimestampMS int64          `json:"source_timestamp_ms,omitempty"`
	Events            []CDCEvent     `json:"events,omitempty"`
	EventCount        int            `json:"event_count"`
	PayloadBytes      int64          `json:"payload_bytes"`
	EventsCiphertext  string         `json:"-"`
	Status            CDCSpoolStatus `json:"status"`
	CreatedAt         time.Time      `json:"created_at"`
	AppliedAt         time.Time      `json:"applied_at,omitempty"`
}

type CDCSpoolStats struct {
	PendingTransactions  int64   `json:"pending_transactions"`
	PendingEvents        int64   `json:"pending_events"`
	PendingBytes         int64   `json:"pending_bytes"`
	FirstPosition        string  `json:"first_position,omitempty"`
	LastPosition         string  `json:"last_position,omitempty"`
	StorageBackend       string  `json:"storage_backend,omitempty"`
	StorageLevel         string  `json:"storage_level,omitempty"`
	StorageCapacityBytes int64   `json:"storage_capacity_bytes,omitempty"`
	StorageUsedBytes     int64   `json:"storage_used_bytes,omitempty"`
	StorageFreeBytes     int64   `json:"storage_free_bytes,omitempty"`
	StorageUsedPct       float64 `json:"storage_used_pct,omitempty"`
}

// EngineInfo describes QMigration Unified Engine capabilities exposed for diagnostics.
type EngineInfo struct {
	Name      string   `json:"name"`
	Available bool     `json:"available"`
	Binary    string   `json:"binary,omitempty"`
	Modes     []string `json:"modes,omitempty"`
	Note      string   `json:"note,omitempty"`
}

// EngineRenderRequest asks QMigration to render its internal runtime diagnostics/configuration.
type EngineRenderRequest struct {
	Engine string `json:"engine"`
	TaskID string `json:"task_id"`
}

// RuntimeSpec describes an internally supervised QMigration runtime. FULL jobs do not use commands; native CDC readers may run as QMigration-owned subprocesses.
type RuntimeSpec struct {
	Engine   string            `json:"engine"`
	Format   string            `json:"format"`
	Filename string            `json:"filename"`
	Content  string            `json:"content"`
	Command  []string          `json:"command,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
}

// EngineJob is a durable long-running QMigration-owned runtime process, primarily used
// for managed CDC. Unlike MigrationChunk it is not part of full-load progress.
type EngineJobStatus string

const (
	EngineJobPending       EngineJobStatus = "PENDING"
	EngineJobRunning       EngineJobStatus = "RUNNING"
	EngineJobStopRequested EngineJobStatus = "STOP_REQUESTED"
	EngineJobStopped       EngineJobStatus = "STOPPED"
	EngineJobFailed        EngineJobStatus = "FAILED"
)

type EngineJob struct {
	ID         string          `json:"id"`
	TaskID     string          `json:"task_id"`
	Kind       string          `json:"kind"`      // CDC
	Direction  string          `json:"direction"` // forward / reverse
	Engine     string          `json:"engine"`
	Status     EngineJobStatus `json:"status"`
	WorkerID   string          `json:"worker_id,omitempty"`
	LeaseUntil time.Time       `json:"lease_until,omitempty"`
	RetryCount int             `json:"retry_count"`
	LastError  string          `json:"last_error,omitempty"`
	StartedAt  time.Time       `json:"started_at,omitempty"`
	UpdatedAt  time.Time       `json:"updated_at"`
	FinishedAt time.Time       `json:"finished_at,omitempty"`
}

type EngineJobClaim struct {
	Job           EngineJob   `json:"job"`
	RuntimeConfig RuntimeSpec `json:"runtime_config"`
}

type EngineJobResult struct {
	Error      string `json:"error,omitempty"`
	OutputTail string `json:"output_tail,omitempty"`
}

// CompatibilityLevel classifies migration assessment findings.
type CompatibilityLevel string

const (
	CompatibilityCompatible  CompatibilityLevel = "COMPATIBLE"
	CompatibilityWarning     CompatibilityLevel = "WARNING"
	CompatibilityUnsupported CompatibilityLevel = "UNSUPPORTED"
)

type CompatibilityFinding struct {
	ObjectType   string             `json:"object_type"`
	SourceObject string             `json:"source_object"`
	TargetObject string             `json:"target_object,omitempty"`
	Level        CompatibilityLevel `json:"level"`
	Code         string             `json:"code"`
	Message      string             `json:"message"`
}

type CompatibilityAssessment struct {
	TaskID      string                 `json:"task_id"`
	Compatible  int                    `json:"compatible"`
	Warnings    int                    `json:"warnings"`
	Unsupported int                    `json:"unsupported"`
	CanStart    bool                   `json:"can_start"`
	Findings    []CompatibilityFinding `json:"findings"`
	AssessedAt  time.Time              `json:"assessed_at"`
}

type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	LastLoginAt  time.Time `json:"last_login_at,omitempty"`
}
