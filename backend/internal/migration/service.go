package migration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/engine"
	"qmigration/backend/internal/faultinject"
	"qmigration/backend/internal/perfmodel"
	"qmigration/backend/internal/repository"
	schemapkg "qmigration/backend/internal/schema"
	"qmigration/backend/internal/schematranslate"
	"qmigration/backend/internal/transform"
	validationpkg "qmigration/backend/internal/validation"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Service struct {
	repo          repository.Repository
	connectors    *connector.Registry
	engines       *engine.Registry
	mu            sync.Mutex
	runners       map[string]context.CancelFunc
	progressMu    sync.Mutex
	pressureMu    sync.Mutex
	validationMu  sync.Mutex
	cdcSpoolMu    sync.Mutex
	cdcSpoolLocks map[string]*sync.Mutex
	instanceID    string
	pressure      map[string]taskPressureSample
}

func NewService(repo repository.Repository, connectors *connector.Registry, external ...*engine.Registry) *Service {
	var er *engine.Registry
	if len(external) > 0 {
		er = external[0]
	}
	return &Service{repo: repo, connectors: connectors, engines: er, runners: map[string]context.CancelFunc{}, pressure: map[string]taskPressureSample{}, cdcSpoolLocks: map[string]*sync.Mutex{}, instanceID: newID("srv")}
}

func newID(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

const unifiedEngineName = "qmigration"

// normalizeEngineName keeps old persisted tasks importable while ensuring new
// execution never selects a third-party runtime. DataX/SeaTunnel/Flink/etc.
// names are treated as historical metadata only.
func normalizeEngineName(_ string) string { return unifiedEngineName }
func (s *Service) logTask(ctx context.Context, taskID, workerID, tableID, chunkID, level, message string) {
	if taskID == "" || strings.TrimSpace(message) == "" {
		return
	}
	_ = s.repo.CreateTaskLog(ctx, &domain.TaskLog{ID: newID("log"), TaskID: taskID, WorkerID: workerID, TableID: tableID, ChunkID: chunkID, Level: strings.ToUpper(level), Message: message, CreatedAt: time.Now()})
}
func (s *Service) Logs(ctx context.Context, taskID string, limit int) ([]domain.TaskLog, error) {
	return s.repo.ListTaskLogs(ctx, taskID, limit)
}
func (s *Service) Create(ctx context.Context, m *domain.MigrationTask) error {
	now := time.Now()
	m.ID = newID("mig")
	m.Status = domain.StatusCreated
	m.CreatedAt = now
	m.UpdatedAt = now
	// Engine selection is intentionally not user-configurable anymore. QMigration
	// is the engine; source protocol and table strategy are selected internally.
	m.FullEngine = unifiedEngineName
	if m.Mode == domain.ModeFullAndIncremental || m.Mode == domain.ModeIncremental {
		m.CDCEngine = unifiedEngineName
		m.RollbackCDCEngine = unifiedEngineName
	} else {
		m.CDCEngine = ""
		m.RollbackCDCEngine = ""
	}
	if m.ChunkRows <= 0 {
		m.ChunkRows = 100000
	}
	if m.BatchRows <= 0 {
		m.BatchRows = 500
	}
	if m.Parallelism <= 0 {
		m.Parallelism = 4
	}
	if m.EffectiveParallelism <= 0 || m.EffectiveParallelism > m.Parallelism {
		m.EffectiveParallelism = m.Parallelism
	}
	if strings.TrimSpace(m.FlowControlLevel) == "" {
		m.FlowControlLevel = "NORMAL"
	}
	if m.MaxRetries <= 0 {
		m.MaxRetries = 3
	}
	if strings.TrimSpace(m.WorkerAffinity) == "" {
		m.WorkerAffinity = "PREFERRED"
	}
	m.WorkerAffinity = strings.ToUpper(strings.TrimSpace(m.WorkerAffinity))
	if m.WorkerAffinity != "PREFERRED" && m.WorkerAffinity != "REQUIRED" {
		return fmt.Errorf("unsupported worker_affinity %q", m.WorkerAffinity)
	}
	for key, value := range m.WorkerSelector {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return errors.New("worker_selector keys and values must be non-empty")
		}
	}
	if m.RowsLimitPerSec < 0 || m.QPSLimit < 0 || m.ReadLimitMBps < 0 || m.WriteLimitMBps < 0 || m.TargetThroughputMBps < 0 || m.CompletionSLASeconds < 0 {
		return errors.New("rate limits and completion_sla_seconds cannot be negative")
	}
	if m.TargetThroughputMBps > 0 && (m.AutoThroughputEnabled || m.CompletionSLASeconds > 0) {
		return errors.New("target_throughput_mbps cannot be combined with auto_throughput_enabled or completion_sla_seconds")
	}
	if m.AutoThroughputEnabled && m.CompletionSLASeconds > 0 {
		return errors.New("auto_throughput_enabled and completion_sla_seconds are mutually exclusive")
	}
	const maxSafeTargetThroughputMBps int64 = (1<<63 - 1) / (1 << 20)
	if m.TargetThroughputMBps > maxSafeTargetThroughputMBps {
		return fmt.Errorf("target_throughput_mbps is too large: %d", m.TargetThroughputMBps)
	}
	if strings.TrimSpace(m.RateLimitTimezone) == "" {
		m.RateLimitTimezone = "Local"
	}
	if !strings.EqualFold(m.RateLimitTimezone, "Local") {
		if _, err := time.LoadLocation(m.RateLimitTimezone); err != nil {
			return fmt.Errorf("invalid rate_limit_timezone %q: %w", m.RateLimitTimezone, err)
		}
	}
	for i, w := range m.RateLimitWindows {
		if _, ok := parseClockMinutes(w.Start); !ok {
			return fmt.Errorf("rate_limit_windows[%d].start must be HH:MM", i)
		}
		if _, ok := parseClockMinutes(w.End); !ok {
			return fmt.Errorf("rate_limit_windows[%d].end must be HH:MM", i)
		}
		if w.ReadLimitMBps < 0 || w.WriteLimitMBps < 0 || w.TargetThroughputMBps < 0 || w.RowsPerSecond < 0 || w.QPS < 0 || w.Parallelism < 0 {
			return fmt.Errorf("rate_limit_windows[%d] values cannot be negative", i)
		}
		if w.TargetThroughputMBps > maxSafeTargetThroughputMBps {
			return fmt.Errorf("rate_limit_windows[%d].target_throughput_mbps is too large: %d", i, w.TargetThroughputMBps)
		}
	}
	if err := transform.ValidateRules(m.TransformRules); err != nil {
		return err
	}
	for i := range m.Tables {
		st := normalizedSplitStrategy(m.Tables[i].SplitStrategy)
		m.Tables[i].SplitStrategy = st
		if st != "AUTO" && st != "PRIMARY_KEY_RANGE" && st != "UNIQUE_KEY_RANGE" && st != "HASH" && st != "PARTITION" && st != "CUSTOM_SQL" {
			return fmt.Errorf("table %s.%s unsupported split_strategy %q", m.Tables[i].SourceSchema, m.Tables[i].SourceTable, st)
		}
		if st == "CUSTOM_SQL" {
			if err := validateCustomWhere(m.Tables[i].CustomWhere); err != nil {
				return fmt.Errorf("table mapping %d: %w", i, err)
			}
		}
		if m.Tables[i].HashBuckets < 0 || m.Tables[i].HashBuckets > 256 {
			return fmt.Errorf("table mapping %d hash_buckets must be 0..256", i)
		}
	}
	if m.ValidationMode == "" {
		m.ValidationMode = "CHUNK_CHECKSUM"
	}
	if m.PostLoadDDLMode == "" {
		m.PostLoadDDLMode = "INDEXES"
	}
	m.PostLoadDDLMode = strings.ToUpper(strings.TrimSpace(m.PostLoadDDLMode))
	if m.CDCDDLMode == "" {
		m.CDCDDLMode = "REJECT"
	}
	m.CDCDDLMode = strings.ToUpper(strings.TrimSpace(m.CDCDDLMode))
	if m.CDCConflictMode == "" {
		m.CDCConflictMode = "SOURCE_WINS"
	}
	m.CDCConflictMode = strings.ToUpper(strings.TrimSpace(m.CDCConflictMode))
	switch m.CDCConflictMode {
	case "SOURCE_WINS":
	case "LAST_WRITE_WINS":
		if strings.TrimSpace(m.CDCConflictColumn) == "" {
			return errors.New("cdc_conflict_column is required for LAST_WRITE_WINS")
		}
	default:
		return fmt.Errorf("unsupported cdc_conflict_mode %q", m.CDCConflictMode)
	}
	if err := s.repo.CreateMigration(ctx, m); err != nil {
		return err
	}
	s.logTask(ctx, m.ID, "", "", "", "INFO", "migration task created")
	return nil
}
func (s *Service) List(ctx context.Context) ([]domain.MigrationTask, error) {
	return s.repo.ListMigrations(ctx)
}
func (s *Service) Get(ctx context.Context, id string) (*domain.MigrationTask, error) {
	return s.repo.GetMigration(ctx, id)
}
func (s *Service) Tables(ctx context.Context, id string) ([]domain.MigrationTable, error) {
	return s.repo.ListMigrationTables(ctx, id)
}
func (s *Service) Chunks(ctx context.Context, id string) ([]domain.MigrationChunk, error) {
	return s.repo.ListChunks(ctx, id)
}

func (s *Service) Start(ctx context.Context, id string) error {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if m.Status != domain.StatusCreated && m.Status != domain.StatusPaused {
		return fmt.Errorf("task cannot start from %s", m.Status)
	}
	if m.Status == domain.StatusPaused {
		resume := m.PausedFromStatus
		if resume == "" {
			resume = s.inferResumeStatus(ctx, m)
		}
		if err := Transition(m, resume); err != nil {
			return err
		}
		m.PausedFromStatus = ""
		m.UpdatedAt = time.Now()
		if err := s.repo.UpdateMigration(ctx, m); err != nil {
			return err
		}
		switch resume {
		case domain.StatusCDCInitializing:
			return s.ensureManagedCDCJob(ctx, m, "forward")
		case domain.StatusCDCCatchingUp:
			return s.ensureManagedCDCJob(ctx, m, "forward")
		case domain.StatusRollbackSyncing:
			return s.ensureManagedCDCJob(ctx, m, "reverse")
		case domain.StatusValidating:
			go s.validateTask(m.ID)
		}
		return nil
	}
	if err := s.validateExecutionPlan(ctx, m); err != nil {
		return err
	}
	// The execution engine is fixed to QMigration. Protocol/table strategy is
	// selected internally and is never delegated to a third-party runtime.
	m.FullEngine = unifiedEngineName
	if m.Mode != domain.ModeFull {
		m.CDCEngine = unifiedEngineName
	}
	if err := Transition(m, domain.StatusPrechecking); err != nil {
		return err
	}
	m.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, m); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	if old := s.runners[id]; old != nil {
		old()
	}
	s.runners[id] = cancel
	s.mu.Unlock()
	go s.prepare(runCtx, id)
	return nil
}
func (s *Service) inferResumeStatus(ctx context.Context, m *domain.MigrationTask) domain.MigrationStatus {
	if strings.HasPrefix(string(m.PausedFromStatus), "ROLLBACK_") {
		return m.PausedFromStatus
	}
	if m.Mode == domain.ModeIncremental {
		return domain.StatusCDCCatchingUp
	}
	if m.TotalChunks > 0 && m.FinishedChunks < m.TotalChunks {
		return domain.StatusFullMigrating
	}
	if m.ValidationEnabled {
		results, _ := s.repo.ListValidationResults(ctx, m.ID)
		if len(results) == 0 {
			return domain.StatusValidating
		}
	}
	if m.Mode == domain.ModeFullAndIncremental {
		return domain.StatusCDCCatchingUp
	}
	return domain.StatusFullMigrating
}

func (s *Service) Pause(ctx context.Context, id string) error {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	from := m.Status
	if err := Transition(m, domain.StatusPaused); err != nil {
		return err
	}
	m.PausedFromStatus = from
	m.UpdatedAt = time.Now()
	s.mu.Lock()
	if c := s.runners[id]; c != nil {
		c()
		delete(s.runners, id)
	}
	s.mu.Unlock()
	if err := s.repo.UpdateMigration(ctx, m); err != nil {
		return err
	}
	// A pure CDC or rollback CDC task should stop the managed process while paused.
	// During FULL_MIGRATING of a full+incremental task the CDC capture is deliberately
	// left running so changes remain buffered while full chunks are paused.
	switch from {
	case domain.StatusCDCInitializing, domain.StatusCDCCatchingUp:
		_ = s.requestStopEngineJobs(ctx, id, "forward")
	case domain.StatusRollbackSyncing:
		_ = s.requestStopEngineJobs(ctx, id, "reverse")
	}
	return nil
}
func (s *Service) Cancel(ctx context.Context, id string) error {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if m.Status == domain.StatusFinished {
		return errors.New("finished task cannot be cancelled")
	}
	m.Status = domain.StatusCancelled
	m.UpdatedAt = time.Now()
	s.mu.Lock()
	if c := s.runners[id]; c != nil {
		c()
		delete(s.runners, id)
	}
	s.mu.Unlock()
	return s.repo.UpdateMigration(ctx, m)
}

func stableMigrationKey(meta *domain.TableMetadata) ([]string, *domain.IndexInfo) {
	if meta == nil {
		return nil, nil
	}
	keys := append([]string(nil), meta.PrimaryKeys...)
	if len(keys) == 0 && meta.PrimaryKey != "" {
		keys = []string{meta.PrimaryKey}
	}
	if len(keys) > 0 {
		return keys, nil
	}
	cols := map[string]domain.ColumnInfo{}
	for _, col := range meta.Columns {
		cols[strings.ToLower(col.Name)] = col
	}
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		if !idx.Unique || idx.Primary || len(idx.Columns) == 0 {
			continue
		}
		stable := true
		for _, name := range idx.Columns {
			col, ok := cols[strings.ToLower(name)]
			if !ok || col.Nullable || strings.Contains(strings.ToUpper(col.Extra), "GENERATED") {
				stable = false
				break
			}
		}
		if stable {
			return append([]string(nil), idx.Columns...), idx
		}
	}
	return nil, nil
}

func mappedKeyNames(keys []string, mappings []domain.ColumnMapping) []string {
	rename := map[string]string{}
	for _, m := range mappings {
		if m.SourceColumn != "" && m.TargetColumn != "" {
			rename[strings.ToLower(m.SourceColumn)] = m.TargetColumn
		}
	}
	out := make([]string, len(keys))
	for i, key := range keys {
		out[i] = key
		if n := rename[strings.ToLower(key)]; n != "" {
			out[i] = n
		}
	}
	return out
}

func matchingUniqueIndex(indexes []domain.IndexInfo, keys []string) bool {
	for _, idx := range indexes {
		if !idx.Unique || len(idx.Columns) != len(keys) {
			continue
		}
		match := true
		for i := range keys {
			if !strings.EqualFold(idx.Columns[i], keys[i]) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func (s *Service) requireFullPair(source, target domain.DataSource) error {
	if err := s.connectors.Require(source.Type, connector.CapabilityFullRead); err != nil {
		return fmt.Errorf("source %s: %w", source.Type, err)
	}
	if err := s.connectors.Require(target.Type, connector.CapabilityFullWrite); err != nil {
		return fmt.Errorf("target %s: %w", target.Type, err)
	}
	return nil
}

func (s *Service) requireCDCPair(source, target domain.DataSource) error {
	if err := s.connectors.Require(source.Type, connector.CapabilityCDCRead); err != nil {
		return fmt.Errorf("CDC source %s: %w", source.Type, err)
	}
	if err := s.connectors.Require(target.Type, connector.CapabilityCDCApply); err != nil {
		return fmt.Errorf("CDC target %s: %w", target.Type, err)
	}
	return nil
}

func (s *Service) chooseTableFullEngine(_ *domain.MigrationTask, source, target domain.DataSource, _ *domain.TableMetadata) (string, error) {
	if err := s.requireFullPair(source, target); err != nil {
		return "", fmt.Errorf("QMigration Unified Engine cannot execute full load %s -> %s: %w", source.Type, target.Type, err)
	}
	return unifiedEngineName, nil
}

func validateCDCEngineSource(engineName string, ds domain.DataSource, role string) error {
	legacy := strings.TrimSpace(engineName)
	if legacy == "native-mysql-cdc" && !ds.Type.IsMySQLFamily() {
		return fmt.Errorf("%s legacy native-mysql-cdc requires a MySQL-family source, got %s", role, ds.Type)
	}
	if legacy == "native-postgres-cdc" && !ds.Type.IsPostgreSQLFamily() {
		return fmt.Errorf("%s legacy native-postgres-cdc requires a PostgreSQL-family source, got %s", role, ds.Type)
	}
	// Native CDC availability is capability-driven by the Connector registry.
	// requireCDCPair performs the authoritative check so newly implemented
	// vendors (SQL Server today, Oracle later) do not require another type switch.
	return nil
}

func (s *Service) validateExecutionPlan(ctx context.Context, m *domain.MigrationTask) error {
	source, err := s.repo.GetDataSource(ctx, m.SourceID)
	if err != nil {
		return fmt.Errorf("source datasource: %w", err)
	}
	target, err := s.repo.GetDataSource(ctx, m.TargetID)
	if err != nil {
		return fmt.Errorf("target datasource: %w", err)
	}
	if m.Mode != domain.ModeIncremental {
		if err := s.requireFullPair(*source, *target); err != nil {
			return err
		}
	}
	if m.Mode != domain.ModeFull {
		if err := validateCDCEngineSource(m.CDCEngine, *source, "forward"); err != nil {
			return err
		}
		if err := s.requireCDCPair(*source, *target); err != nil {
			return err
		}
		if strings.TrimSpace(m.RollbackCDCEngine) != "" {
			if err := validateCDCEngineSource(m.RollbackCDCEngine, *target, "rollback"); err != nil {
				return err
			}
			if err := s.connectors.Require(target.Type, connector.CapabilityCDCRead); err != nil {
				return fmt.Errorf("rollback CDC source %s: %w", target.Type, err)
			}
		}
	}
	switch strings.ToUpper(strings.TrimSpace(m.PostLoadDDLMode)) {
	case "", "NONE", "INDEXES", "INDEXES_AND_FOREIGN_KEYS":
	default:
		return fmt.Errorf("unsupported post_load_ddl_mode %q", m.PostLoadDDLMode)
	}
	switch strings.ToUpper(strings.TrimSpace(m.CDCDDLMode)) {
	case "", "REJECT":
	case "SAME_FAMILY":
		sameFamily := (source.Type.IsMySQLFamily() && target.Type.IsMySQLFamily()) || (source.Type.IsPostgreSQLFamily() && target.Type.IsPostgreSQLFamily()) || (source.Type == domain.DataSourceOracle && target.Type == domain.DataSourceOracle) || (source.Type == domain.DataSourceSQLServer && target.Type == domain.DataSourceSQLServer) || (source.Type == domain.DataSourceGaussDB && target.Type == domain.DataSourceGaussDB)
		if !sameFamily {
			return fmt.Errorf("cdc_ddl_mode SAME_FAMILY requires same-family source/target, got %s -> %s", source.Type, target.Type)
		}
	default:
		return fmt.Errorf("unsupported cdc_ddl_mode %q", m.CDCDDLMode)
	}
	return nil
}

func (s *Service) prepare(ctx context.Context, id string) {
	defer func() { s.mu.Lock(); delete(s.runners, id); s.mu.Unlock() }()
	if err := s.prepareTask(ctx, id); err != nil {
		s.failTask(id, err)
	}
}
func (s *Service) prepareTask(ctx context.Context, id string) error {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	sourceDS, err := s.repo.GetDataSource(ctx, m.SourceID)
	if err != nil {
		return fmt.Errorf("source datasource: %w", err)
	}
	targetDS, err := s.repo.GetDataSource(ctx, m.TargetID)
	if err != nil {
		return fmt.Errorf("target datasource: %w", err)
	}
	src, err := s.connectors.New(*sourceDS)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := s.connectors.New(*targetDS)
	if err != nil {
		return err
	}
	defer dst.Close()
	if err := src.TestConnection(ctx); err != nil {
		return fmt.Errorf("source precheck: %w", err)
	}
	if err := dst.TestConnection(ctx); err != nil {
		return fmt.Errorf("target precheck: %w", err)
	}
	if sourceDS.Type.IsMySQLFamily() && m.Mode != domain.ModeFull {
		// Real MySQL-family connectors expose binlog configuration checks. Test or
		// future protocol connectors may not; capability validation still occurs
		// when the unified CDC reader opens the stream.
		if inspector, ok := src.(connector.MigrationPrecheckConnector); ok {
			for _, item := range inspector.MigrationPrechecks(ctx, true) {
				if item.Level == domain.PrecheckFailed {
					return fmt.Errorf("QMigration MySQL CDC precheck %s failed: %s", item.Name, item.Message)
				}
				if item.Name == "mysql_binlog_row_image" && item.Level != domain.PrecheckPass {
					return fmt.Errorf("QMigration MySQL CDC requires binlog_row_image=FULL: %s", item.Message)
				}
			}
		}
	}
	if err := s.setStatus(id, domain.StatusPrecheckSuccess); err != nil {
		return err
	}
	if err := s.setStatus(id, domain.StatusPreparing); err != nil {
		return err
	}
	m, err = s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if m.Mode == domain.ModeFullAndIncremental || m.Mode == domain.ModeIncremental {
		var pos *domain.CDCPosition
		if selectedSource, ok := src.(connector.CDCSelectionPositionSource); ok {
			pos, err = selectedSource.CurrentCDCPositionForSelection(ctx, m.Tables)
			if err != nil {
				return fmt.Errorf("capture selected-table CDC start position: %w", err)
			}
		} else if mysqlSource, ok := src.(connector.MySQLBinlogSource); ok && sourceDS.Type.IsMySQLFamily() && sourceDS.Type != domain.DataSourceTiDB && sourceDS.Type != domain.DataSourceOceanBase {
			pos, err = mysqlSource.CurrentCDCPosition(ctx)
			if err != nil {
				return fmt.Errorf("capture QMigration MySQL CDC start position: %w", err)
			}
		} else if checkpoint, ok := src.(connector.CDCCheckpointSource); ok && sourceDS.Type.IsPostgreSQLFamily() {
			slot := strings.ToLower(strings.ReplaceAll("qmigration_"+m.ID, "-", "_"))
			if len(slot) > 63 {
				slot = slot[:63]
			}
			pos, err = checkpoint.CreateCDCCheckpoint(ctx, slot)
			if err != nil {
				return fmt.Errorf("create PostgreSQL CDC replication slot: %w", err)
			}
		} else if cdcSrc, ok := src.(connector.CDCSource); ok {
			pos, err = cdcSrc.CurrentCDCPosition(ctx)
			if err != nil {
				return fmt.Errorf("capture CDC start position: %w", err)
			}
		} else {
			return fmt.Errorf("source connector %s does not implement the QMigration CDC source contract", sourceDS.Type)
		}
		pos.ID = newID("cdc")
		pos.TaskID = id
		pos.Direction = "forward"
		pos.RecordedAt = time.Now()
		if err = s.repo.CreateCDCPosition(ctx, pos); err != nil {
			return err
		}
		m.CDCStartTimestampMS = pos.SourceTimestampMS
		m.CDCStartPositionType = pos.PositionType
		m.CDCStartPositionValue = pos.PositionValue
		m.CDCStartResource = pos.Resource
		m.UpdatedAt = time.Now()
		if err = s.repo.UpdateMigration(ctx, m); err != nil {
			return err
		}
	}
	if m.Mode == domain.ModeIncremental {
		if err := s.prepareIncrementalTables(ctx, m, src, *sourceDS, *targetDS); err != nil {
			return err
		}
		if err := s.setStatus(id, domain.StatusCDCInitializing); err != nil {
			return err
		}
		m, _ = s.repo.GetMigration(ctx, id)
		return s.ensureManagedCDCJob(ctx, m, "forward")
	}
	mappings, err := resolveMappings(ctx, src, *sourceDS, *targetDS, m.Tables)
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return errors.New("no source tables selected")
	}
	if m.Mode == domain.ModeFullAndIncremental {
		if err := validateSelectedCDC(ctx, src, mappings); err != nil {
			return err
		}
	}
	totalChunks := 0
	for _, mapping := range mappings {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		meta, err := src.GetTableMetadata(ctx, mapping.SourceSchema, mapping.SourceTable)
		if err != nil && errors.Is(err, connector.ErrMetadataUnavailable) {
			return fmt.Errorf("QMigration connector for %s does not expose table metadata; QMigration requires a native connector implementation", sourceDS.Type)
		}
		if err != nil {
			return fmt.Errorf("read metadata %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
		}
		if len(meta.Columns) == 0 {
			return fmt.Errorf("source table %s.%s has no columns", mapping.SourceSchema, mapping.SourceTable)
		}
		selectedEngine, err := s.chooseTableFullEngine(m, *sourceDS, *targetDS, meta)
		if err != nil {
			return fmt.Errorf("select full engine for %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
		}
		migrationKeys, migrationUniqueIndex := stableMigrationKey(meta)
		if selectedEngine == unifiedEngineName && len(migrationKeys) == 0 {
			return fmt.Errorf("table %s.%s has neither a primary key nor a UNIQUE NOT NULL key; native resumable migration requires a stable migration key", mapping.SourceSchema, mapping.SourceTable)
		} else if m.ValidationEnabled && len(migrationKeys) == 0 {
			return fmt.Errorf("table %s.%s has no stable primary/unique key; built-in deterministic validation requires a stable key, so disable validation or choose explicit validation tooling", mapping.SourceSchema, mapping.SourceTable)
		}
		sourceColumns := filterGenerated(meta.Columns)
		targetColumns, actualTargetPK, err := mapTargetColumns(sourceColumns, meta.PrimaryKey, mapping.Columns)
		actualTargetPKs := primaryKeyNames(targetColumns)
		sourcePKs := append([]string(nil), migrationKeys...)
		targetPKs := mappedKeyNames(sourcePKs, mapping.Columns)
		targetPK := ""
		if len(targetPKs) > 0 {
			targetPK = targetPKs[0]
		}
		if err != nil {
			return fmt.Errorf("column mapping %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
		}
		mappedIndexes, mappedFKs := mapTableSchemaArtifacts(meta, mapping, mappings)
		targetMeta, err := dst.GetTableMetadata(ctx, mapping.TargetSchema, mapping.TargetTable)
		targetMetadataExternal := false
		targetCreated := false
		if err != nil && !targetMetadataExternal {
			return fmt.Errorf("target metadata %s.%s: %w", mapping.TargetSchema, mapping.TargetTable, err)
		}
		if !targetMetadataExternal && (targetMeta == nil || len(targetMeta.Columns) == 0) {
			if !m.AutoCreateTable {
				return fmt.Errorf("target table %s.%s does not exist; enable auto_create_table", mapping.TargetSchema, mapping.TargetTable)
			}
			createColumns := targetColumns
			if sourceDS.Type != targetDS.Type && ((sourceDS.Type.IsMySQLFamily() && targetDS.Type.IsPostgreSQLFamily()) || (sourceDS.Type.IsPostgreSQLFamily() && targetDS.Type.IsMySQLFamily())) {
				converted, warns := schemapkg.ConvertColumns(targetColumns, targetDS.Type)
				createColumns = converted
				for _, warn := range warns {
					s.logTask(ctx, id, "", "", "", "WARN", mapping.SourceSchema+"."+mapping.SourceTable+": "+warn)
				}
			}
			sc, ok := dst.(connector.SchemaConnector)
			if !ok {
				return errors.New("target connector does not support schema creation")
			}
			if err := sc.EnsureSchema(ctx, mapping.TargetSchema); err != nil {
				return fmt.Errorf("ensure target schema: %w", err)
			}
			if len(actualTargetPKs) > 1 {
				composite, ok := dst.(connector.CompositeSchemaConnector)
				if !ok {
					return fmt.Errorf("target connector cannot create composite primary key %v", targetPKs)
				}
				if err := composite.CreateTableWithPrimaryKeys(ctx, mapping.TargetSchema, mapping.TargetTable, createColumns, actualTargetPKs); err != nil {
					return fmt.Errorf("create target table: %w", err)
				}
			} else if err := sc.CreateTable(ctx, mapping.TargetSchema, mapping.TargetTable, createColumns, actualTargetPK); err != nil {
				return fmt.Errorf("create target table: %w", err)
			}
			targetCreated = true
			targetMeta, err = dst.GetTableMetadata(ctx, mapping.TargetSchema, mapping.TargetTable)
			if err != nil {
				return err
			}
		}
		if !targetMetadataExternal && selectedEngine == unifiedEngineName && migrationUniqueIndex != nil && !matchingUniqueIndex(targetMeta.Indexes, targetPKs) {
			if !targetCreated {
				return fmt.Errorf("target table %s.%s must have a UNIQUE index on migration key %v before native migration", mapping.TargetSchema, mapping.TargetTable, targetPKs)
			}
			ddl, ok := dst.(connector.PostLoadSchemaConnector)
			if !ok {
				return fmt.Errorf("target connector cannot create UNIQUE migration key %v", targetPKs)
			}
			idx := domain.IndexInfo{Name: migrationUniqueIndex.Name, Columns: append([]string(nil), targetPKs...), Unique: true}
			if idx.Name == "" {
				idx.Name = "uq_qmigration_key"
			}
			if err := ddl.CreateIndex(ctx, mapping.TargetSchema, mapping.TargetTable, idx); err != nil {
				return fmt.Errorf("create target UNIQUE migration key %v: %w", targetPKs, err)
			}
			targetMeta, err = dst.GetTableMetadata(ctx, mapping.TargetSchema, mapping.TargetTable)
			if err != nil {
				return err
			}
		}
		if !targetMetadataExternal {
			if err := validateTargetColumns(targetColumns, targetMeta); err != nil {
				return fmt.Errorf("target table %s.%s: %w", mapping.TargetSchema, mapping.TargetTable, err)
			}
			targetColumns = orderedTargetColumns(targetColumns, targetMeta)
		}
		topology := s.discoverTableTopology(ctx, src, mapping.SourceSchema, mapping.SourceTable)
		t := &domain.MigrationTable{ID: newID("tbl"), TaskID: id, Engine: selectedEngine, SourceSchema: mapping.SourceSchema, SourceTable: mapping.SourceTable, TargetSchema: mapping.TargetSchema, TargetTable: mapping.TargetTable, PrimaryKeys: sourcePKs, TargetPrimaryKeys: targetPKs, Topology: topology, PrimaryKey: func() string {
			if len(sourcePKs) > 0 {
				return sourcePKs[0]
			}
			return ""
		}(), TargetPrimaryKey: targetPK, PrimaryKeyType: meta.PrimaryKeyType, Columns: sourceColumns, TargetColumns: targetColumns, Indexes: mappedIndexes, ForeignKeys: mappedFKs, EstimatedRows: meta.EstimatedRows, DataLength: meta.DataLength, MinPK: meta.MinPK, MaxPK: meta.MaxPK, SplitStrategy: normalizedSplitStrategy(mapping.SplitStrategy), CustomWhere: strings.TrimSpace(mapping.CustomWhere), HashBuckets: mapping.HashBuckets, Partitions: append([]string(nil), mapping.Partitions...), Status: "READY"}
		plannedChunkRows := m.ChunkRows
		if envDefaultOn("QMIGRATION_HISTORICAL_PROFILE_REUSE") {
			if prev, e := s.repo.FindMigrationTableProfile(ctx, m.SourceID, m.TargetID, mapping.SourceSchema, mapping.SourceTable); e == nil && prev.PerformanceSamples > 0 {
				t.ProfileBytesPerSec = prev.ProfileBytesPerSec
				t.ProfileRowsPerSec = prev.ProfileRowsPerSec
				t.RecommendedChunkRows = prev.RecommendedChunkRows
				t.PerformanceSamples = prev.PerformanceSamples
				if prev.TopologyPerformance != nil {
					t.TopologyPerformance = make(map[string]domain.TableTopologyPerformance, len(prev.TopologyPerformance))
					for k, v := range prev.TopologyPerformance {
						t.TopologyPerformance[k] = v
					}
				}
				if prev.RecommendedChunkRows > 0 {
					plannedChunkRows = perfmodel.BoundHistoricalRows(prev.RecommendedChunkRows, m.ChunkRows, .25, 4)
					s.logTask(ctx, id, "", t.ID, "", "INFO", fmt.Sprintf("historical table profile reused table=%s.%s samples=%d recommended_chunk_rows=%d planned_chunk_rows=%d", mapping.SourceSchema, mapping.SourceTable, prev.PerformanceSamples, prev.RecommendedChunkRows, plannedChunkRows))
				}
			}
		}

		if !meta.HasRows {
			t.Status = "EMPTY"
			if err := s.repo.CreateMigrationTable(ctx, t); err != nil {
				return err
			}
			continue
		}
		var chunks []domain.MigrationChunk
		{
			strategy := normalizedSplitStrategy(mapping.SplitStrategy)
			if strategy != "AUTO" && strategy != "PRIMARY_KEY_RANGE" && strategy != "UNIQUE_KEY_RANGE" && strategy != "HASH" && strategy != "PARTITION" && strategy != "CUSTOM_SQL" {
				return fmt.Errorf("table %s.%s has unsupported split_strategy %q", mapping.SourceSchema, mapping.SourceTable, mapping.SplitStrategy)
			}
			planKeyset := func(splitType string, hashBuckets int, customWhere string) error {
				key := ""
				if len(sourcePKs) > 0 {
					key = sourcePKs[0]
				}
				partitions := DesiredKeysetPartitions(meta.EstimatedRows, plannedChunkRows, m.Parallelism)
				var boundaries [][]connector.Value
				if hashBuckets == 0 && partitions > 1 && splitType != "CUSTOM_SQL_KEYSET" {
					if splitter, ok := src.(connector.KeysetBoundaryConnector); ok {
						boundaries, _ = splitter.PlanKeysetBoundaries(ctx, connector.KeysetBoundaryRequest{Schema: mapping.SourceSchema, Table: mapping.SourceTable, Keys: sourcePKs, Columns: sourceColumns, Partitions: partitions})
					}
				}
				var e error
				if hashBuckets > 0 {
					chunks = make([]domain.MigrationChunk, 0, hashBuckets)
					for bucket := 0; bucket < hashBuckets; bucket++ {
						chunks = append(chunks, domain.MigrationChunk{SplitType: "HASH_KEYSET", HashBucket: bucket, HashBuckets: hashBuckets})
					}
				} else {
					chunks, e = PlanBoundedKeyset(boundaries)
					if e != nil {
						return e
					}
					for i := range chunks {
						chunks[i].SplitType = splitType
					}
				}
				for i := range chunks {
					chunks[i].ID = newID("chk")
					chunks[i].TaskID = id
					chunks[i].TableID = t.ID
					chunks[i].ChunkNo = i + 1
					chunks[i].PrimaryKey = key
					chunks[i].CustomWhere = customWhere
					chunks[i].Status = domain.ChunkPending
				}
				return nil
			}
			numericPK := len(sourcePKs) == 1 && meta.PrimaryKeyNumeric && strings.EqualFold(sourcePKs[0], meta.PrimaryKey)
			switch strategy {
			case "PRIMARY_KEY_RANGE":
				if !numericPK {
					return fmt.Errorf("table %s.%s PRIMARY_KEY_RANGE requires a numeric primary key", mapping.SourceSchema, mapping.SourceTable)
				}
				chunks, err = PlanIntegerRangeByRows(meta.MinPK, meta.MaxPK, meta.EstimatedRows, plannedChunkRows)
				if err != nil {
					return err
				}
				for i := range chunks {
					chunks[i].ID = newID("chk")
					chunks[i].TaskID = id
					chunks[i].TableID = t.ID
					chunks[i].PrimaryKey = meta.PrimaryKey
					chunks[i].Status = domain.ChunkPending
				}
			case "UNIQUE_KEY_RANGE":
				if err = planKeyset("PRIMARY_KEY_KEYSET", 0, ""); err != nil {
					return fmt.Errorf("plan unique keyset %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
				}
			case "HASH":
				b := mapping.HashBuckets
				if b <= 0 {
					b = m.Parallelism * 2
				}
				if b < 1 {
					b = 1
				}
				if b > 256 {
					b = 256
				}
				t.HashBuckets = b
				if err = planKeyset("HASH_KEYSET", b, ""); err != nil {
					return fmt.Errorf("plan hash split %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
				}
			case "PARTITION":
				parts := append([]string(nil), mapping.Partitions...)
				if len(parts) == 0 {
					pc, ok := src.(connector.PartitionConnector)
					if !ok {
						return fmt.Errorf("table %s.%s source connector does not expose partitions", mapping.SourceSchema, mapping.SourceTable)
					}
					parts, err = pc.ListTablePartitions(ctx, mapping.SourceSchema, mapping.SourceTable)
					if err != nil {
						return fmt.Errorf("list partitions: %w", err)
					}
				}
				if len(parts) == 0 {
					return fmt.Errorf("table %s.%s is not partitioned", mapping.SourceSchema, mapping.SourceTable)
				}
				t.Partitions = parts
				chunks = make([]domain.MigrationChunk, 0, len(parts))
				for i, part := range parts {
					c := domain.MigrationChunk{ID: newID("chk"), TaskID: id, TableID: t.ID, ChunkNo: i + 1, PrimaryKey: t.PrimaryKey, PartitionName: part, Status: domain.ChunkPending}
					if numericPK {
						c.SplitType = "PARTITION_RANGE"
						c.Start = meta.MinPK
						c.End = meta.MaxPK
					} else {
						c.SplitType = "PARTITION_KEYSET"
					}
					chunks = append(chunks, c)
				}
			case "CUSTOM_SQL":
				if err := validateCustomWhere(mapping.CustomWhere); err != nil {
					return fmt.Errorf("table %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
				}
				t.CustomWhere = strings.TrimSpace(mapping.CustomWhere)
				if numericPK {
					chunks = []domain.MigrationChunk{{ID: newID("chk"), TaskID: id, TableID: t.ID, ChunkNo: 1, SplitType: "CUSTOM_SQL_RANGE", PrimaryKey: meta.PrimaryKey, Start: meta.MinPK, End: meta.MaxPK, CustomWhere: t.CustomWhere, Status: domain.ChunkPending}}
				} else {
					if err = planKeyset("CUSTOM_SQL_KEYSET", 0, t.CustomWhere); err != nil {
						return err
					}
				}
			case "AUTO":
				if numericPK {
					chunks, err = PlanIntegerRangeByRows(meta.MinPK, meta.MaxPK, meta.EstimatedRows, plannedChunkRows)
					if err != nil {
						return err
					}
					for i := range chunks {
						chunks[i].ID = newID("chk")
						chunks[i].TaskID = id
						chunks[i].TableID = t.ID
						chunks[i].PrimaryKey = meta.PrimaryKey
						chunks[i].Status = domain.ChunkPending
					}
				} else {
					if err = planKeyset("PRIMARY_KEY_KEYSET", 0, ""); err != nil {
						return fmt.Errorf("plan bounded keyset %s.%s: %w", mapping.SourceSchema, mapping.SourceTable, err)
					}
				}
			}
		}
		assignTopologyHints(chunks, topology)
		t.TotalChunks = len(chunks)
		totalChunks += len(chunks)
		if err := s.repo.CreateMigrationTable(ctx, t); err != nil {
			return err
		}
		if err := s.repo.CreateChunks(ctx, chunks); err != nil {
			return err
		}
	}
	m, err = s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	m.TotalChunks = totalChunks
	m.UpdatedAt = time.Now()
	if m.Mode == domain.ModeFullAndIncremental {
		if err := Transition(m, domain.StatusCDCInitializing); err != nil {
			return err
		}
		if err := s.repo.UpdateMigration(ctx, m); err != nil {
			return err
		}
		return s.ensureManagedCDCJob(ctx, m, "forward")
	}
	if totalChunks == 0 {
		m.Progress = 100
		if err := Transition(m, domain.StatusFullMigrating); err != nil {
			return err
		}
		if err := Transition(m, domain.StatusFullFinished); err != nil {
			return err
		}
		if m.ValidationEnabled {
			_ = Transition(m, domain.StatusValidating)
			if err := s.repo.UpdateMigration(ctx, m); err != nil {
				return err
			}
			go s.validateTask(id)
			return nil
		}
		_ = Transition(m, domain.StatusFinished)
		return s.repo.UpdateMigration(ctx, m)
	}
	if err := Transition(m, domain.StatusFullMigrating); err != nil {
		return err
	}
	return s.repo.UpdateMigration(ctx, m)
}

func normalizedSplitStrategy(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	if v == "" {
		return "AUTO"
	}
	switch v {
	case "AUTO", "PRIMARY_KEY_RANGE", "UNIQUE_KEY_RANGE", "HASH", "PARTITION", "CUSTOM_SQL":
		return v
	default:
		return v
	}
}

func validateCustomWhere(expr string) error {
	x := strings.TrimSpace(expr)
	if x == "" {
		return errors.New("CUSTOM_SQL split requires custom_where")
	}
	lower := strings.ToLower(x)
	for _, bad := range []string{";", "--", "/*", "*/", " union ", " into outfile", " copy ", " drop ", " alter ", " truncate ", " insert ", " update ", " delete "} {
		if strings.Contains(lower, bad) {
			return fmt.Errorf("custom_where contains unsafe token %q", strings.TrimSpace(bad))
		}
	}
	return nil
}

func copyPlacement(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func validateSelectedCDC(ctx context.Context, src connector.Connector, mappings []domain.TableMapping) error {
	if v, ok := src.(connector.CDCSelectionValidator); ok {
		if err := v.ValidateCDCSelection(ctx, mappings); err != nil {
			return fmt.Errorf("source CDC table selection: %w", err)
		}
	}
	return nil
}

func (s *Service) prepareIncrementalTables(ctx context.Context, task *domain.MigrationTask, src connector.Connector, sourceDS, targetDS domain.DataSource) error {
	existing, err := s.repo.ListMigrationTables(ctx, task.ID)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return nil
	}
	mappings, err := resolveMappings(ctx, src, sourceDS, targetDS, task.Tables)
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return errors.New("no source tables selected")
	}
	if err := validateSelectedCDC(ctx, src, mappings); err != nil {
		return err
	}
	for _, mapping := range mappings {
		t := &domain.MigrationTable{ID: newID("tbl"), TaskID: task.ID, SourceSchema: mapping.SourceSchema, SourceTable: mapping.SourceTable, TargetSchema: mapping.TargetSchema, TargetTable: mapping.TargetTable, Status: "CDC_READY"}
		meta, e := src.GetTableMetadata(ctx, mapping.SourceSchema, mapping.SourceTable)
		if e == nil && meta != nil {
			t.PrimaryKeys = append([]string(nil), meta.PrimaryKeys...)
			if len(t.PrimaryKeys) == 0 && meta.PrimaryKey != "" {
				t.PrimaryKeys = []string{meta.PrimaryKey}
			}
			t.PrimaryKey = meta.PrimaryKey
			t.TargetPrimaryKey = meta.PrimaryKey
			t.PrimaryKeyType = meta.PrimaryKeyType
			t.Columns = filterGenerated(meta.Columns)
			t.EstimatedRows = meta.EstimatedRows
			t.DataLength = meta.DataLength
			var targetPK string
			t.TargetColumns, targetPK, e = mapTargetColumns(t.Columns, meta.PrimaryKey, mapping.Columns)
			if e != nil {
				return e
			}
			t.TargetPrimaryKey = targetPK
			t.TargetPrimaryKeys = primaryKeyNames(t.TargetColumns)
		} else if e != nil && !errors.Is(e, connector.ErrMetadataUnavailable) {
			return e
		}
		if e := s.repo.CreateMigrationTable(ctx, t); e != nil {
			return e
		}
	}
	return nil
}

func objectName(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

func defaultSchema(ds domain.DataSource) string {
	if ds.Schema != "" {
		return ds.Schema
	}
	if ds.Type.IsPostgreSQLFamily() {
		return "public"
	}
	return ds.Database
}

func resolveMappings(ctx context.Context, src connector.Connector, sourceDS, targetDS domain.DataSource, configured []domain.TableMapping) ([]domain.TableMapping, error) {
	sourceSchema := defaultSchema(sourceDS)
	targetSchema := defaultSchema(targetDS)
	if len(configured) > 0 {
		out := make([]domain.TableMapping, 0, len(configured))
		for _, x := range configured {
			if x.SourceSchema == "" {
				x.SourceSchema = sourceSchema
			}
			if x.SourceTable == "" {
				return nil, errors.New("source_table is required")
			}
			if x.TargetSchema == "" {
				x.TargetSchema = targetSchema
				if x.TargetSchema == "" {
					x.TargetSchema = x.SourceSchema
				}
			}
			if x.TargetTable == "" {
				x.TargetTable = x.SourceTable
			}
			if x.SourceSchema == "" || x.TargetSchema == "" {
				return nil, errors.New("source/target schema is required")
			}
			out = append(out, x)
		}
		return out, nil
	}
	if sourceSchema == "" {
		return nil, errors.New("source datasource default schema/database is required when tables are not explicitly configured")
	}
	tables, err := src.ListTables(ctx, sourceSchema)
	if err != nil {
		return nil, err
	}
	if targetSchema == "" {
		targetSchema = sourceSchema
	}
	out := make([]domain.TableMapping, 0, len(tables))
	for _, t := range tables {
		out = append(out, domain.TableMapping{SourceSchema: t.Schema, SourceTable: t.Name, TargetSchema: targetSchema, TargetTable: t.Name})
	}
	return out, nil
}
func filterGenerated(cols []domain.ColumnInfo) []domain.ColumnInfo {
	out := make([]domain.ColumnInfo, 0, len(cols))
	for _, c := range cols {
		if strings.Contains(strings.ToUpper(c.Extra), "GENERATED") {
			continue
		}
		out = append(out, c)
	}
	return out
}
func primaryKeyNames(columns []domain.ColumnInfo) []string {
	out := []string{}
	for _, col := range columns {
		if col.PrimaryKey {
			out = append(out, col.Name)
		}
	}
	return out
}

func mapTargetColumns(src []domain.ColumnInfo, sourcePK string, maps []domain.ColumnMapping) ([]domain.ColumnInfo, string, error) {
	rename := map[string]string{}
	for _, m := range maps {
		if m.SourceColumn == "" || m.TargetColumn == "" {
			return nil, "", errors.New("source_column and target_column are required")
		}
		rename[strings.ToLower(m.SourceColumn)] = m.TargetColumn
	}
	out := make([]domain.ColumnInfo, len(src))
	targetPK := sourcePK
	found := map[string]bool{}
	for i, c := range src {
		found[strings.ToLower(c.Name)] = true
		if n := rename[strings.ToLower(c.Name)]; n != "" {
			if c.Name == sourcePK {
				targetPK = n
			}
			c.Name = n
		}
		out[i] = c
	}
	for k := range rename {
		if !found[k] {
			return nil, "", fmt.Errorf("mapped source column %s does not exist", k)
		}
	}
	seen := map[string]bool{}
	for _, c := range out {
		k := strings.ToLower(c.Name)
		if seen[k] {
			return nil, "", fmt.Errorf("duplicate target column %s", c.Name)
		}
		seen[k] = true
	}
	return out, targetPK, nil
}
func orderedTargetColumns(expected []domain.ColumnInfo, dst *domain.TableMetadata) []domain.ColumnInfo {
	byName := map[string]domain.ColumnInfo{}
	for _, c := range dst.Columns {
		byName[strings.ToLower(c.Name)] = c
	}
	out := make([]domain.ColumnInfo, 0, len(expected))
	for _, e := range expected {
		if c, ok := byName[strings.ToLower(e.Name)]; ok {
			out = append(out, c)
		} else {
			out = append(out, e)
		}
	}
	return out
}
func validateTargetColumns(expected []domain.ColumnInfo, dst *domain.TableMetadata) error {
	if dst == nil || len(dst.Columns) == 0 {
		return errors.New("target table does not exist or metadata is empty")
	}
	set := map[string]bool{}
	for _, c := range dst.Columns {
		set[strings.ToLower(c.Name)] = true
	}
	for _, c := range expected {
		if !set[strings.ToLower(c.Name)] {
			return fmt.Errorf("missing column %s", c.Name)
		}
	}
	return nil
}

func mapTableSchemaArtifacts(meta *domain.TableMetadata, current domain.TableMapping, all []domain.TableMapping) ([]domain.IndexInfo, []domain.ForeignKeyInfo) {
	indexes := make([]domain.IndexInfo, 0, len(meta.Indexes))
	for _, idx := range meta.Indexes {
		copyIdx := idx
		copyIdx.Columns = mappedColumnsForAssessment(idx.Columns, current.Columns)
		indexes = append(indexes, copyIdx)
	}
	fks := make([]domain.ForeignKeyInfo, 0, len(meta.ForeignKeys))
	for _, fk := range meta.ForeignKeys {
		copyFK := fk
		copyFK.Columns = mappedColumnsForAssessment(fk.Columns, current.Columns)
		for _, ref := range all {
			if strings.EqualFold(ref.SourceSchema, fk.ReferencedSchema) && strings.EqualFold(ref.SourceTable, fk.ReferencedTable) {
				copyFK.ReferencedSchema = ref.TargetSchema
				copyFK.ReferencedTable = ref.TargetTable
				copyFK.ReferencedColumns = mappedColumnsForAssessment(fk.ReferencedColumns, ref.Columns)
				break
			}
		}
		if strings.EqualFold(copyFK.ReferencedSchema, current.SourceSchema) {
			copyFK.ReferencedSchema = current.TargetSchema
		}
		fks = append(fks, copyFK)
	}
	return indexes, fks
}

func (s *Service) setStatus(id string, to domain.MigrationStatus) error {
	m, err := s.repo.GetMigration(context.Background(), id)
	if err != nil {
		return err
	}
	from := m.Status
	if err := Transition(m, to); err != nil {
		return err
	}
	m.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(context.Background(), m); err != nil {
		return err
	}
	s.logTask(context.Background(), id, "", "", "", "INFO", fmt.Sprintf("status %s -> %s", from, to))
	return nil
}
func (s *Service) failTask(id string, cause error) {
	m, err := s.repo.GetMigration(context.Background(), id)
	if err != nil {
		return
	}
	if m.Status == domain.StatusCancelled || m.Status == domain.StatusPaused || m.Status == domain.StatusFinished {
		return
	}
	if CanTransition(m.Status, domain.StatusFailed) {
		m.Status = domain.StatusFailed
	} else {
		m.Status = domain.StatusFailed
	}
	m.LastError = cause.Error()
	m.UpdatedAt = time.Now()
	_ = s.repo.UpdateMigration(context.Background(), m)
	_ = s.repo.CreateAlert(context.Background(), &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Migration failed", Message: cause.Error(), TaskID: id, CreatedAt: time.Now()})
	s.logTask(context.Background(), id, "", "", "", "ERROR", "migration failed: "+cause.Error())
}

const chunkLease = 5 * time.Minute

type taskPressureSample struct {
	At                time.Time
	Level             string
	Reason            string
	SpoolPendingBytes int64
	SpoolGrowthBPS    int64
}

func topologyPlacementHint(p domain.TopologyPlacement) map[string]string {
	if len(p.Labels) == 0 {
		return nil
	}
	keys := []string{"polardbx_group", "tidb_store_id", "ob_zone"}
	for _, key := range keys {
		if value := strings.TrimSpace(p.Labels[key]); value != "" {
			return map[string]string{key: value}
		}
	}
	return nil
}

func assignTopologyHints(chunks []domain.MigrationChunk, topology []domain.TopologyPlacement) {
	if len(chunks) == 0 || len(topology) == 0 {
		return
	}
	usable := make([]domain.TopologyPlacement, 0, len(topology))
	for _, p := range topology {
		if len(topologyPlacementHint(p)) > 0 {
			usable = append(usable, p)
		}
	}
	if len(usable) == 0 {
		return
	}
	for i := range chunks {
		p := usable[i%len(usable)]
		if part := strings.TrimSpace(chunks[i].PartitionName); part != "" {
			for _, candidate := range usable {
				if strings.EqualFold(strings.TrimSpace(candidate.Labels["partition_name"]), part) {
					p = candidate
					break
				}
			}
		}
		chunks[i].PlacementHint = topologyPlacementHint(p)
		chunks[i].FaultDomain = repository.CanonicalFaultDomain(p.Labels)
		chunks[i].TopologyID = p.ID
		chunks[i].TopologyKind = p.Kind
	}
}

func (s *Service) discoverTableTopology(ctx context.Context, src connector.Connector, schema, table string) []domain.TopologyPlacement {
	discoverer, ok := src.(connector.TableTopologyConnector)
	if !ok {
		return nil
	}
	topology, err := discoverer.DiscoverTableTopology(ctx, schema, table)
	if err != nil {
		return nil // topology is advisory; discovery failure must never block migration
	}
	if len(topology) > 256 {
		topology = topology[:256]
	}
	return topology
}

func loadPressureLevel(name string, load domain.DatabaseRuntimeLoad) (string, string) {
	criticalConn := floatEnv("QMIGRATION_DB_CONNECTION_CRITICAL_PCT", 90)
	warnConn := floatEnv("QMIGRATION_DB_CONNECTION_WARN_PCT", 75)
	criticalRunning := int64(intEnv("QMIGRATION_DB_RUNNING_QUERY_CRITICAL", 128))
	warnRunning := int64(intEnv("QMIGRATION_DB_RUNNING_QUERY_WARN", 64))
	if load.ConnectionUsagePct >= criticalConn && load.ConnectionUsagePct > 0 {
		return "CRITICAL", fmt.Sprintf("%s connections %.1f%% >= %.1f%%", name, load.ConnectionUsagePct, criticalConn)
	}
	if load.RunningQueries >= criticalRunning && load.RunningQueries > 0 {
		return "CRITICAL", fmt.Sprintf("%s running queries %d >= %d", name, load.RunningQueries, criticalRunning)
	}
	if load.ConnectionUsagePct >= warnConn && load.ConnectionUsagePct > 0 {
		return "WARN", fmt.Sprintf("%s connections %.1f%% >= %.1f%%", name, load.ConnectionUsagePct, warnConn)
	}
	if load.RunningQueries >= warnRunning && load.RunningQueries > 0 {
		return "WARN", fmt.Sprintf("%s running queries %d >= %d", name, load.RunningQueries, warnRunning)
	}
	return "NORMAL", ""
}

func pressureRank(level string) int {
	switch strings.ToUpper(level) {
	case "CRITICAL":
		return 2
	case "WARN":
		return 1
	default:
		return 0
	}
}

func positiveInt64Env(name string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// sampleCDCSpoolPressure makes Full Load yield capacity before CDC durability
// becomes the bottleneck. It considers absolute backlog, storage watermarks and
// backlog growth. The growth forecast is intentionally conservative: once the
// projected time to the CRITICAL backlog boundary is short, Full parallelism
// is reduced before the spool filesystem/object store is near exhaustion.
func (s *Service) sampleCDCSpoolPressure(ctx context.Context, task *domain.MigrationTask, previous taskPressureSample) (level, reason string, pendingBytes, growthBPS, criticalETASeconds int64) {
	if task == nil || task.Status != domain.StatusFullMigrating || task.Mode != domain.ModeFullAndIncremental {
		return "NORMAL", "", 0, 0, 0
	}
	stats, err := s.repo.CDCSpoolStats(ctx, task.ID, "forward")
	if err != nil {
		return "NORMAL", "", 0, 0, 0
	}
	pendingBytes = stats.PendingBytes
	maxPending := positiveInt64Env("QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES", 64<<30)
	warnBytes := positiveInt64Env("QMIGRATION_CDC_SPOOL_BACKLOG_WARN_BYTES", maxPending/2)
	criticalBytes := positiveInt64Env("QMIGRATION_CDC_SPOOL_BACKLOG_CRITICAL_BYTES", maxPending*4/5)
	if warnBytes >= criticalBytes {
		warnBytes = criticalBytes / 2
	}
	if !previous.At.IsZero() && pendingBytes > previous.SpoolPendingBytes {
		dt := time.Since(previous.At).Seconds()
		if dt >= 0.5 {
			growthBPS = int64(float64(pendingBytes-previous.SpoolPendingBytes) / dt)
		}
	}
	level = "NORMAL"
	reason = ""
	if strings.EqualFold(stats.StorageLevel, "CRITICAL") || pendingBytes >= criticalBytes {
		level = "CRITICAL"
		reason = fmt.Sprintf("CDC spool backlog critical pending=%dB critical=%dB storage=%s", pendingBytes, criticalBytes, stats.StorageLevel)
	} else if strings.EqualFold(stats.StorageLevel, "WARN") || pendingBytes >= warnBytes {
		level = "WARN"
		reason = fmt.Sprintf("CDC spool backlog warning pending=%dB warn=%dB storage=%s", pendingBytes, warnBytes, stats.StorageLevel)
	}
	if growthBPS > 0 && pendingBytes < criticalBytes {
		headroomSeconds := (criticalBytes - pendingBytes) / growthBPS
		criticalETASeconds = headroomSeconds
		criticalETA := int64(intEnv("QMIGRATION_CDC_SPOOL_PREDICT_CRITICAL_SECONDS", 300))
		warnETA := int64(intEnv("QMIGRATION_CDC_SPOOL_PREDICT_WARN_SECONDS", 900))
		predLevel := "NORMAL"
		if headroomSeconds <= criticalETA {
			predLevel = "CRITICAL"
		} else if headroomSeconds <= warnETA {
			predLevel = "WARN"
		}
		if pressureRank(predLevel) > pressureRank(level) {
			level = predLevel
			reason = fmt.Sprintf("CDC spool backlog growing %dB/s; projected critical headroom=%ds pending=%dB critical=%dB", growthBPS, headroomSeconds, pendingBytes, criticalBytes)
		}
	}
	return level, reason, pendingBytes, growthBPS, criticalETASeconds
}

func (s *Service) sampleTaskDatabasePressure(ctx context.Context, task *domain.MigrationTask) (string, string) {
	if s.connectors == nil || task == nil {
		return "NORMAL", ""
	}
	level, reason := "NORMAL", ""
	for _, side := range []struct {
		name string
		id   string
	}{{"source", task.SourceID}, {"target", task.TargetID}} {
		ds, err := s.repo.GetDataSource(ctx, side.id)
		if err != nil {
			continue
		}
		c, err := s.connectors.New(*ds)
		if err != nil {
			continue
		}
		sampler, ok := c.(connector.RuntimeLoadConnector)
		if !ok {
			_ = c.Close()
			continue
		}
		load, err := sampler.SampleRuntimeLoad(ctx)
		_ = c.Close()
		if err != nil {
			continue
		}
		candidate, candidateReason := loadPressureLevel(side.name, load)
		if pressureRank(candidate) > pressureRank(level) {
			level, reason = candidate, candidateReason
		}
	}
	return level, reason
}

func (s *Service) reconcileTaskFlowControl(ctx context.Context, task *domain.MigrationTask, observedLevel, observedReason string) (string, string) {
	if task == nil {
		return "NORMAL", ""
	}
	s.pressureMu.Lock()
	defer s.pressureMu.Unlock()
	interval := time.Duration(intEnv("QMIGRATION_DB_PRESSURE_SAMPLE_SECONDS", 5)) * time.Second
	previous := s.pressure[task.ID]
	if !previous.At.IsZero() && time.Since(previous.At) < interval && pressureRank(observedLevel) <= pressureRank(previous.Level) {
		return previous.Level, previous.Reason
	}
	level, reason := s.sampleTaskDatabasePressure(ctx, task)
	spoolLevel, spoolReason, spoolBytes, spoolGrowth, spoolCriticalETA := s.sampleCDCSpoolPressure(ctx, task, previous)
	if pressureRank(spoolLevel) > pressureRank(level) {
		level, reason = spoolLevel, spoolReason
	}
	if pressureRank(observedLevel) > pressureRank(level) {
		level, reason = observedLevel, observedReason
	}
	if task.AutoThroughputEnabled && task.SpeedBytesSec > 0 {
		pct := int64(autoProbePct(task))
		switch strings.ToUpper(strings.TrimSpace(level)) {
		case "WARN":
			if pct > 90 {
				pct = 90
			}
		case "CRITICAL":
			if pct > 70 {
				pct = 70
			}
		}
		task.ControllerTargetBytesSec = capControllerTarget(task, task.SpeedBytesSec*pct/100, time.Now())
		task.ThroughputControllerReason = fmt.Sprintf("AUTO speed=%dB/s probe=%d%% pressure=%s target=%dB/s", task.SpeedBytesSec, autoProbePct(task), level, task.ControllerTargetBytesSec)
	}
	parallelismCap := resolveTaskParallelismCap(task, time.Now())
	current := task.EffectiveParallelism
	if current <= 0 || current > parallelismCap {
		current = parallelismCap
	}
	if current <= 0 {
		current = 1
	}
	desired := current
	switch level {
	case "CRITICAL":
		desired = current / 2
		if desired < 1 {
			desired = 1
		}
	case "WARN":
		desired = current - 1
		if desired < 1 {
			desired = 1
		}
	default:
		// Under an AUTO/SLA controller, avoid increasing concurrency once the
		// measured task throughput is already close to the requested global
		// rate. When behind target, add up to two workers per control interval
		// so large migrations converge faster without bypassing pressure gates.
		needMore := task.ControllerTargetBytesSec <= 0 || task.SpeedBytesSec <= 0 || task.SpeedBytesSec*100 < task.ControllerTargetBytesSec*95
		step := 1
		if task.ControllerTargetBytesSec > 0 && task.SpeedBytesSec > 0 && task.SpeedBytesSec*100 < task.ControllerTargetBytesSec*80 {
			step = 2
		}
		if needMore && desired < parallelismCap {
			desired += step
			if desired > parallelismCap {
				desired = parallelismCap
			}
		}
	}
	previousLevel := strings.ToUpper(strings.TrimSpace(task.FlowControlLevel))
	if task.EffectiveParallelism != desired || task.FlowControlLevel != level || task.FlowControlReason != reason || task.CDCSpoolGrowthBytesSec != spoolGrowth || task.CDCSpoolCriticalETASeconds != spoolCriticalETA {
		task.EffectiveParallelism = desired
		task.FlowControlLevel = level
		task.FlowControlReason = reason
		task.CDCSpoolGrowthBytesSec = spoolGrowth
		task.CDCSpoolCriticalETASeconds = spoolCriticalETA
		task.UpdatedAt = time.Now()
		_ = s.repo.UpdateMigration(ctx, task)
		if level == "CRITICAL" && previousLevel != "CRITICAL" {
			_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Migration pressure critical", Message: reason, TaskID: task.ID, CreatedAt: time.Now()})
			s.logTask(ctx, task.ID, "", "", "", "WARN", fmt.Sprintf("flow control CRITICAL; effective_parallelism=%d reason=%s", desired, reason))
		} else if level == "WARN" && previousLevel == "NORMAL" {
			s.logTask(ctx, task.ID, "", "", "", "WARN", fmt.Sprintf("flow control WARN; effective_parallelism=%d reason=%s", desired, reason))
		} else if level == "NORMAL" && previousLevel != "" && previousLevel != "NORMAL" {
			s.logTask(ctx, task.ID, "", "", "", "INFO", fmt.Sprintf("flow control recovered; effective_parallelism=%d", desired))
		}
	}
	s.pressure[task.ID] = taskPressureSample{At: time.Now(), Level: level, Reason: reason, SpoolPendingBytes: spoolBytes, SpoolGrowthBPS: spoolGrowth}
	return level, reason
}

func mergeTaskPressure(control domain.ChunkControl, taskLevel, taskReason string, batch int) domain.ChunkControl {
	if pressureRank(taskLevel) <= pressureRank(control.Level) {
		return control
	}
	if batch <= 0 {
		batch = 500
	}
	control.Level = taskLevel
	control.Reason = taskReason
	if taskLevel == "CRITICAL" {
		control.PauseMS = intEnv("QMIGRATION_BACKPRESSURE_CRITICAL_PAUSE_MS", 1000)
		control.MaxBatchRows = batch / 2
		if control.MaxBatchRows < 50 {
			control.MaxBatchRows = 50
		}
	} else {
		control.PauseMS = intEnv("QMIGRATION_BACKPRESSURE_WARN_PAUSE_MS", 250)
		control.MaxBatchRows = batch * 3 / 4
		if control.MaxBatchRows < 100 {
			control.MaxBatchRows = 100
		}
	}
	return control
}

func floatEnv(name string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || n <= 0 || n > 100 {
		return fallback
	}
	return n
}

func labelsContain(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func workerOverloaded(w *domain.Worker) bool {
	if w == nil {
		return true
	}
	if w.Status != "" && !strings.EqualFold(w.Status, "ONLINE") {
		return true
	}
	maxCPU := floatEnv("QMIGRATION_WORKER_MAX_CPU_PCT", 95)
	maxMemory := floatEnv("QMIGRATION_WORKER_MAX_MEMORY_PCT", 95)
	return (w.CPUUsagePct > 0 && w.CPUUsagePct >= maxCPU) || (w.MemoryUsagePct > 0 && w.MemoryUsagePct >= maxMemory)
}

func workerSchedulingScore(w *domain.Worker) float64 {
	if w == nil {
		return 1e9
	}
	cores := w.CPU
	if cores <= 0 {
		cores = 1
	}
	jobsPct := float64(w.RunningJobs) * 100 / float64(cores)
	netCapacityMbps := int64(intEnv("QMIGRATION_WORKER_NETWORK_CAPACITY_MBPS", 1000))
	netCapacityBPS := float64(netCapacityMbps) * 1000 * 1000 / 8
	netPct := 0.0
	if netCapacityBPS > 0 {
		net := w.NetworkRxBps
		if w.NetworkTxBps > net {
			net = w.NetworkTxBps
		}
		netPct = float64(net) * 100 / netCapacityBPS
	}
	if netPct > 200 {
		netPct = 200
	}
	return w.CPUUsagePct*.45 + w.MemoryUsagePct*.25 + jobsPct*.20 + netPct*.10
}

func workerFreshOnline(w domain.Worker, now time.Time) bool {
	if w.Status != "" && !strings.EqualFold(w.Status, "ONLINE") {
		return false
	}
	if !w.LastHeartbeat.IsZero() && now.Sub(w.LastHeartbeat) > 20*time.Second {
		return false
	}
	return !workerOverloaded(&w)
}

func (s *Service) betterWorkerAvailable(ctx context.Context, current *domain.Worker) (bool, string) {
	if current == nil || current.RunningJobs <= 0 {
		return false, ""
	}
	workers, err := s.repo.ListWorkers(ctx)
	if err != nil {
		return false, ""
	}
	now := time.Now()
	currentScore := workerSchedulingScore(current)
	margin := float64(intEnv("QMIGRATION_WORKER_REBALANCE_SCORE_MARGIN", 15))
	bestScore, bestID, bestJobs := currentScore, "", current.RunningJobs
	for i := range workers {
		candidate := &workers[i]
		if candidate.ID == current.ID || !workerFreshOnline(*candidate, now) {
			continue
		}
		score := workerSchedulingScore(candidate)
		if candidate.RunningJobs < bestJobs && score < bestScore {
			bestScore, bestID, bestJobs = score, candidate.ID, candidate.RunningJobs
		}
	}
	if bestID != "" && currentScore >= bestScore+margin {
		return true, fmt.Sprintf("worker load rebalance current=%.1f best=%s/%.1f", currentScore, bestID, bestScore)
	}
	return false, ""
}

func envDefaultOn(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if v == "" {
		return true
	}
	return v != "0" && v != "false" && v != "off" && v != "no"
}

func desiredTaskWorkers(task *domain.MigrationTask) int {
	if task == nil {
		return 1
	}
	n := task.EffectiveParallelism
	if n <= 0 {
		n = task.Parallelism
	}
	if n <= 0 {
		n = 1
	}
	return n
}

func numericResumeCursor(raw string) (int64, bool) {
	var v struct {
		AfterPK int64 `json:"after_pk"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(raw)), &v) != nil {
		return 0, false
	}
	return v.AfterPK, true
}

func numericRangeSplit(splitType string) bool {
	switch strings.ToUpper(strings.TrimSpace(splitType)) {
	case "PRIMARY_KEY_RANGE", "PK_RANGE", "PK_RANGE_ADAPTIVE", "PK_RANGE_REBALANCED":
		return true
	default:
		return false
	}
}

func runningChunkYieldCursorSafe(chunk *domain.MigrationChunk, progress domain.ChunkProgress) bool {
	if chunk == nil || strings.TrimSpace(progress.CursorJSON) == "" {
		return false
	}
	if strings.Contains(strings.ToUpper(chunk.SplitType), "KEYSET") {
		cursor, err := decodeKeysetBound(progress.CursorJSON)
		return err == nil && len(cursor) > 0 && strings.TrimSpace(progress.CursorJSON) != strings.TrimSpace(chunk.EndCursorJSON)
	}
	after, ok := numericResumeCursor(progress.CursorJSON)
	return ok && numericRangeSplit(chunk.SplitType) && after >= chunk.Start && after < chunk.End
}

func (s *Service) topologyDegradedShedReason(ctx context.Context, chunk *domain.MigrationChunk, progress domain.ChunkProgress) string {
	if chunk == nil || !envDefaultOn("QMIGRATION_TOPOLOGY_DEGRADED_RUNNING_SHED") || strings.TrimSpace(chunk.TopologyID) == "" || !runningChunkYieldCursorSafe(chunk, progress) {
		return ""
	}
	table, err := s.repo.GetMigrationTable(ctx, chunk.TableID)
	if err != nil || repository.TopologyProfileHealth(table, chunk.TopologyID) != "DEGRADED" {
		return ""
	}
	cap := repository.TopologyEffectiveConcurrencyCap(table, chunk.TopologyID)
	if cap < 1 {
		cap = 1
	}
	running, err := repository.ListRunningTopologyChunks(ctx, s.repo, chunk.TaskID, chunk.TopologyID)
	if err != nil {
		return ""
	}
	if len(running) <= cap {
		return ""
	}
	// Keep a deterministic survivor set so concurrent renewals cannot drain every
	// chunk from the degraded topology. Oldest work keeps its lease; newer excess
	// work yields only after a durable batch boundary.
	sort.Slice(running, func(i, j int) bool {
		ti, tj := running[i].StartedAt, running[j].StartedAt
		if ti.IsZero() != tj.IsZero() {
			return !ti.IsZero()
		}
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if running[i].ChunkNo != running[j].ChunkNo {
			return running[i].ChunkNo < running[j].ChunkNo
		}
		return running[i].ID < running[j].ID
	})
	for i := 0; i < cap && i < len(running); i++ {
		if running[i].ID == chunk.ID {
			return ""
		}
	}
	return fmt.Sprintf("topology degraded shed topology=%s running=%d cap=%d", chunk.TopologyID, len(running), cap)
}

func (s *Service) faultDomainRunningShedReason(ctx context.Context, chunk *domain.MigrationChunk, progress domain.ChunkProgress) string {
	if chunk == nil || !envDefaultOn("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_RUNNING_SHED") || !repository.FaultDomainProtectionEnabled() || !runningChunkYieldCursorSafe(chunk, progress) {
		return ""
	}
	table, err := s.repo.GetMigrationTable(ctx, chunk.TableID)
	if err != nil {
		return ""
	}
	risk, scope := repository.TopologyFaultDomainPeerState(table, chunk.TopologyID)
	cap := repository.FaultDomainConcurrencyCap(risk)
	if cap <= 0 || scope == "" {
		return ""
	}
	domainValue := strings.TrimSpace(chunk.FaultDomain[scope])
	if domainValue == "" {
		domainValue = strings.TrimSpace(repository.TopologyFaultDomain(table, chunk.TopologyID)[scope])
	}
	if domainValue == "" {
		return ""
	}
	chunks, err := repository.ListRunningFaultDomainChunks(ctx, s.repo, chunk.TaskID, scope, domainValue)
	if err != nil {
		return ""
	}
	type survivor struct {
		chunk      domain.MigrationChunk
		healthRank int
	}
	running := make([]survivor, 0, cap+1)
	for i := range chunks {
		ch := chunks[i]
		healthRank := 0
		if strings.TrimSpace(ch.TopologyID) != "" {
			if peerTable, e := s.repo.GetMigrationTable(ctx, ch.TableID); e == nil {
				healthRank = perfmodel.TopologyHealthRank(repository.TopologyProfileHealth(peerTable, ch.TopologyID))
			}
		}
		running = append(running, survivor{chunk: ch, healthRank: healthRank})
	}
	if len(running) <= cap {
		return ""
	}
	// Prefer healthier work inside the fault domain, then keep the oldest work.
	// This avoids a CIRCUIT_OPEN/DEGRADED survivor consuming the domain cap while
	// a healthy peer is unnecessarily yielded. The ordering is deterministic so
	// concurrent renewals converge to the same survivor set.
	sort.Slice(running, func(i, j int) bool {
		if running[i].healthRank != running[j].healthRank {
			return running[i].healthRank < running[j].healthRank
		}
		ti, tj := running[i].chunk.StartedAt, running[j].chunk.StartedAt
		if ti.IsZero() != tj.IsZero() {
			return !ti.IsZero()
		}
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if running[i].chunk.ChunkNo != running[j].chunk.ChunkNo {
			return running[i].chunk.ChunkNo < running[j].chunk.ChunkNo
		}
		return running[i].chunk.ID < running[j].chunk.ID
	})
	for i := 0; i < cap && i < len(running); i++ {
		if running[i].chunk.ID == chunk.ID {
			return ""
		}
	}
	return fmt.Sprintf("fault-domain shed scope=%s domain=%s risk=%d running=%d cap=%d", scope, domainValue, risk, len(running), cap)
}

func appendControlReason(base, extra string) string {
	base, extra = strings.TrimSpace(base), strings.TrimSpace(extra)
	if base == "" {
		return extra
	}
	if extra == "" {
		return base
	}
	return base + "; " + extra
}

func (s *Service) applyTopologyRunningPressure(ctx context.Context, task *domain.MigrationTask, chunk *domain.MigrationChunk, control domain.ChunkControl) domain.ChunkControl {
	if task == nil || chunk == nil || strings.TrimSpace(chunk.TopologyID) == "" {
		return control
	}
	table, err := s.repo.GetMigrationTable(ctx, chunk.TableID)
	if err != nil {
		return control
	}
	pct := 100
	pauseMS := 0
	reasons := []string{}
	health := repository.TopologyProfileHealth(table, chunk.TopologyID)
	if envDefaultOn("QMIGRATION_TOPOLOGY_DEGRADED_THROTTLE") && health == "DEGRADED" {
		degradedPct := clampInt(intEnv("QMIGRATION_TOPOLOGY_DEGRADED_BATCH_PCT", 50), 10, 100)
		baseCap := repository.TopologyDegradedMaxConcurrency()
		effectiveCap := repository.TopologyEffectiveConcurrencyCap(table, chunk.TopologyID)
		maxRecoveryCap := repository.TopologyRecoveryMaxConcurrency()
		if effectiveCap > baseCap && maxRecoveryCap > baseCap {
			degradedPct += (100 - degradedPct) * (effectiveCap - baseCap) / (maxRecoveryCap - baseCap)
			if degradedPct > 100 {
				degradedPct = 100
			}
		}
		if degradedPct < pct {
			pct = degradedPct
		}
		degradedPause := intEnv("QMIGRATION_TOPOLOGY_DEGRADED_PAUSE_MS", 250)
		if effectiveCap > baseCap && maxRecoveryCap > baseCap {
			degradedPause = degradedPause * (maxRecoveryCap - effectiveCap) / (maxRecoveryCap - baseCap)
		}
		if degradedPause > pauseMS {
			pauseMS = degradedPause
		}
		reasons = append(reasons, fmt.Sprintf("topology %s DEGRADED throttle batch=%d%% recovery_cap=%d", chunk.TopologyID, degradedPct, effectiveCap))
	}
	if repository.FaultDomainProtectionEnabled() {
		risk := repository.TopologyFaultDomainPeerRisk(table, chunk.TopologyID)
		if risk > 0 {
			fdPct := intEnv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_BATCH_PCT", 75)
			fdPause := intEnv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_PAUSE_MS", 100)
			if risk >= perfmodel.TopologyHealthRank("HALF_OPEN") {
				fdPct = intEnv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_BATCH_PCT", 50)
				fdPause = intEnv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_PAUSE_MS", 250)
			}
			fdPct = clampInt(fdPct, 10, 100)
			if fdPct < pct {
				pct = fdPct
			}
			if fdPause > pauseMS {
				pauseMS = fdPause
			}
			reasons = append(reasons, fmt.Sprintf("fault-domain peer risk=%d throttle batch=%d%%", risk, fdPct))
		}
	}
	if pct >= 100 && pauseMS <= 0 {
		return control
	}
	baseBatch := task.BatchRows
	if baseBatch <= 0 {
		baseBatch = 500
	}
	maxBatch := baseBatch * pct / 100
	if maxBatch < 50 {
		maxBatch = 50
	}
	if control.MaxBatchRows <= 0 || maxBatch < control.MaxBatchRows {
		control.MaxBatchRows = maxBatch
	}
	if control.TargetBatchRows <= 0 || control.TargetBatchRows > control.MaxBatchRows {
		control.TargetBatchRows = control.MaxBatchRows
	}
	if pauseMS > control.PauseMS {
		control.PauseMS = pauseMS
	}
	if control.Level == "" || control.Level == "NORMAL" {
		control.Level = "WARN"
	}
	for _, reason := range reasons {
		control.Reason = appendControlReason(control.Reason, reason)
	}
	scaleBudget := func(v int64) int64 {
		if v <= 0 {
			return 0
		}
		n := v * int64(pct) / 100
		if n < 1 {
			n = 1
		}
		return n
	}
	control.ReadBytesPerSec = scaleBudget(control.ReadBytesPerSec)
	control.WriteBytesPerSec = scaleBudget(control.WriteBytesPerSec)
	control.TargetBytesPerSec = scaleBudget(control.TargetBytesPerSec)
	return control
}

func (s *Service) topologyCircuitDrainReason(ctx context.Context, chunk *domain.MigrationChunk, progress domain.ChunkProgress) string {
	if chunk == nil || !envDefaultOn("QMIGRATION_TOPOLOGY_RUNNING_DRAIN") || strings.TrimSpace(chunk.TopologyID) == "" || !runningChunkYieldCursorSafe(chunk, progress) {
		return ""
	}
	table, err := s.repo.GetMigrationTable(ctx, chunk.TableID)
	if err != nil {
		return ""
	}
	health := repository.TopologyProfileHealth(table, chunk.TopologyID)
	if health != "CIRCUIT_OPEN" {
		return ""
	}
	return fmt.Sprintf("topology circuit drain topology=%s health=%s", chunk.TopologyID, health)
}

func (s *Service) shouldYieldRunningChunk(ctx context.Context, task *domain.MigrationTask, chunk *domain.MigrationChunk, worker *domain.Worker, progress domain.ChunkProgress) (bool, string) {
	if task == nil || chunk == nil || strings.TrimSpace(progress.CursorJSON) == "" {
		return false, ""
	}
	// RC36 drains already-running work after another completed sample opens the
	// chunk topology's circuit. Workers stop only at a committed-batch durable
	// cursor. The remainder retains its topology binding, so this sheds load
	// without pretending a shard can be reassigned to a different source DN.
	if reason := s.topologyCircuitDrainReason(ctx, chunk, progress); reason != "" {
		return true, reason
	}
	// RC37 also converges already-running DEGRADED work to the same cap used by
	// new-claim admission. A deterministic survivor set prevents simultaneous
	// renewals from draining the topology below its configured degraded cap.
	if reason := s.topologyDegradedShedReason(ctx, chunk, progress); reason != "" {
		return true, reason
	}
	// RC40 converges already-running work across a correlated rack/zone/region
	// to the same cap used by rc39 new-claim admission. The worker yields only at
	// a durable cursor and the remainder retains the same source topology/domain.
	if reason := s.faultDomainRunningShedReason(ctx, chunk, progress); reason != "" {
		return true, reason
	}
	if !envDefaultOn("QMIGRATION_RUNNING_CHUNK_REBALANCE") || !runningChunkYieldCursorSafe(chunk, progress) {
		return false, ""
	}
	if progress.BytesWritten < positiveInt64Env("QMIGRATION_RUNNING_CHUNK_REBALANCE_MIN_BYTES", 8<<20) {
		return false, ""
	}
	if !chunk.StartedAt.IsZero() && time.Since(chunk.StartedAt) < time.Duration(intEnv("QMIGRATION_RUNNING_CHUNK_REBALANCE_MIN_SECONDS", 30))*time.Second {
		return false, ""
	}
	counts, err := repository.CountTableRunnable(ctx, s.repo, task.ID, chunk.TableID)
	if err != nil {
		return false, ""
	}
	runnable, pending := counts.Runnable(), counts.Pending
	deficit := desiredTaskWorkers(task) - runnable
	better, loadReason := s.betterWorkerAvailable(ctx, worker)
	if deficit <= 0 && !(better && pending == 0) {
		return false, ""
	}
	if better && pending == 0 {
		return true, loadReason
	}
	return true, fmt.Sprintf("running hotspot runnable=%d desired=%d", runnable, desiredTaskWorkers(task))
}

func parseClockMinutes(v string) (int, bool) {
	parts := strings.Split(strings.TrimSpace(v), ":")
	if len(parts) != 2 {
		return 0, false
	}
	h, e1 := strconv.Atoi(parts[0])
	m, e2 := strconv.Atoi(parts[1])
	if e1 != nil || e2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}
func rateWindowActive(w domain.RateLimitWindow, now time.Time) bool {
	s, ok1 := parseClockMinutes(w.Start)
	e, ok2 := parseClockMinutes(w.End)
	if !ok1 || !ok2 {
		return false
	}
	cur := now.Hour()*60 + now.Minute()
	if s == e {
		return true
	}
	if s < e {
		return cur >= s && cur < e
	}
	return cur >= s || cur < e
}
func resolveTaskParallelismCap(task *domain.MigrationTask, at time.Time) int {
	if task == nil {
		return 1
	}
	cap := task.Parallelism
	if cap <= 0 {
		cap = 1
	}
	loc := time.Local
	if z := strings.TrimSpace(task.RateLimitTimezone); z != "" && !strings.EqualFold(z, "Local") {
		if l, e := time.LoadLocation(z); e == nil {
			loc = l
		}
	}
	now := at.In(loc)
	for _, w := range task.RateLimitWindows {
		if rateWindowActive(w, now) && w.Parallelism > 0 && w.Parallelism < cap {
			cap = w.Parallelism
			break
		}
	}
	return cap
}

func resolveTaskRateLimits(task *domain.MigrationTask, at time.Time) (readMB, writeMB, targetMB, rows int64, qps int) {
	if task == nil {
		return
	}
	readMB, writeMB, targetMB, rows, qps = task.ReadLimitMBps, task.WriteLimitMBps, task.TargetThroughputMBps, task.RowsLimitPerSec, task.QPSLimit
	loc := time.Local
	if z := strings.TrimSpace(task.RateLimitTimezone); z != "" && !strings.EqualFold(z, "Local") {
		if l, e := time.LoadLocation(z); e == nil {
			loc = l
		}
	}
	now := at.In(loc)
	for _, w := range task.RateLimitWindows {
		if !rateWindowActive(w, now) {
			continue
		}
		if w.ReadLimitMBps > 0 {
			readMB = w.ReadLimitMBps
		}
		if w.WriteLimitMBps > 0 {
			writeMB = w.WriteLimitMBps
		}
		if w.TargetThroughputMBps > 0 {
			targetMB = w.TargetThroughputMBps
		}
		if w.RowsPerSecond > 0 {
			rows = w.RowsPerSecond
		}
		if w.QPS > 0 {
			qps = w.QPS
		}
		break
	}
	return
}

func taskWorkerBudgets(task *domain.MigrationTask, at time.Time) (readBytesPerSec, writeBytesPerSec, targetBytesPerSec int64) {
	if task == nil {
		return 0, 0, 0
	}
	readMB, writeMB, targetMB, _, _ := resolveTaskRateLimits(task, at)
	workers := task.EffectiveParallelism
	if workers <= 0 {
		workers = task.Parallelism
	}
	if workers <= 0 {
		workers = 1
	}
	divide := func(total int64) int64 {
		if total <= 0 {
			return 0
		}
		n := total / int64(workers)
		if n <= 0 {
			n = 1
		}
		return n
	}
	readBytesPerSec = divide(readMB * (1 << 20))
	writeBytesPerSec = divide(writeMB * (1 << 20))
	globalTarget := int64(0)
	if targetMB > 0 {
		globalTarget = targetMB * (1 << 20)
	} else if task.ControllerTargetBytesSec > 0 {
		globalTarget = task.ControllerTargetBytesSec
	}
	targetBytesPerSec = divide(globalTarget)
	return
}

func taskTargetBytesPerWorker(task *domain.MigrationTask, at time.Time) int64 {
	_, _, target := taskWorkerBudgets(task, at)
	return target
}

func capControllerTarget(task *domain.MigrationTask, target int64, at time.Time) int64 {
	if target <= 0 || task == nil {
		return target
	}
	readMB, writeMB, _, _, _ := resolveTaskRateLimits(task, at)
	for _, mb := range []int64{readMB, writeMB} {
		if mb <= 0 {
			continue
		}
		capBytes := mb * (1 << 20)
		if target > capBytes {
			target = capBytes
		}
	}
	return target
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
func autoProbePct(task *domain.MigrationTask) int {
	if task.ControllerAutoProbePct <= 0 {
		return 125
	}
	return clampInt(task.ControllerAutoProbePct, 80, 160)
}
func slaHeadroomPct(task *domain.MigrationTask) int {
	if task.ControllerSLAHeadroomPct <= 0 {
		return 110
	}
	return clampInt(task.ControllerSLAHeadroomPct, 100, 150)
}
func estimatedRemainingBytes(task *domain.MigrationTask, tables []domain.MigrationTable) int64 {
	var total int64
	for _, t := range tables {
		if t.DataLength > 0 {
			total += t.DataLength
		}
	}
	left := total - task.BytesMigrated
	if left < 0 {
		left = 0
	}
	return left
}
func learnThroughputController(task *domain.MigrationTask, tables []domain.MigrationTable, now time.Time) {
	if task == nil || task.SpeedBytesSec <= 0 {
		return
	}
	level := strings.ToUpper(strings.TrimSpace(task.FlowControlLevel))
	changed := false
	if task.AutoThroughputEnabled && task.TargetThroughputMBps <= 0 {
		p := autoProbePct(task)
		util := int64(100)
		if task.ControllerTargetBytesSec > 0 {
			util = task.SpeedBytesSec * 100 / task.ControllerTargetBytesSec
		}
		switch level {
		case "CRITICAL":
			p -= 10
		case "WARN":
			p -= 5
		default:
			if util >= 95 {
				p += 2
			} else if util < 70 {
				p -= 3
			}
		}
		p = clampInt(p, 80, 160)
		if p != task.ControllerAutoProbePct {
			task.ControllerAutoProbePct = p
			changed = true
		}
	}
	if task.CompletionSLASeconds > 0 && !task.SLAStartedAt.IsZero() {
		left := task.CompletionSLASeconds - int64(now.Sub(task.SLAStartedAt).Seconds())
		if left < 1 {
			left = 1
		}
		remaining := estimatedRemainingBytes(task, tables)
		base := int64(0)
		if remaining > 0 {
			base = remaining / left
		}
		h := slaHeadroomPct(task)
		switch level {
		case "CRITICAL":
			h -= 5
		case "WARN":
			h -= 2
		default:
			if base > 0 && task.SpeedBytesSec < base {
				h += 3
			} else if base > 0 && task.SpeedBytesSec > base*125/100 {
				h--
			}
		}
		h = clampInt(h, 100, 150)
		if h != task.ControllerSLAHeadroomPct {
			task.ControllerSLAHeadroomPct = h
			changed = true
		}
	}
	if changed {
		task.ControllerLearningSamples++
	}
}

func reconcileThroughputController(task *domain.MigrationTask, tables []domain.MigrationTable, now time.Time) {
	if task == nil {
		return
	}
	if task.TargetThroughputMBps > 0 {
		task.ControllerTargetBytesSec = 0
		task.ThroughputControllerReason = fmt.Sprintf("manual target throughput %d MB/s", task.TargetThroughputMBps)
		return
	}
	var totalBytes int64
	for _, table := range tables {
		if table.DataLength > 0 {
			totalBytes += table.DataLength
		}
	}
	remaining := totalBytes - task.BytesMigrated
	if remaining < 0 {
		remaining = 0
	}
	if task.CompletionSLASeconds > 0 {
		if task.SLAStartedAt.IsZero() {
			task.SLAStartedAt = now
		}
		left := task.CompletionSLASeconds - int64(now.Sub(task.SLAStartedAt).Seconds())
		if left < 1 {
			left = 1
		}
		required := int64(0)
		if remaining > 0 {
			required = remaining / left
			// Keep a small scheduling headroom so a task does not continuously
			// oscillate exactly on the SLA boundary.
			required = required * int64(slaHeadroomPct(task)) / 100
		}
		required = capControllerTarget(task, required, now)
		task.ControllerTargetBytesSec = required
		task.ThroughputControllerReason = fmt.Sprintf("SLA remaining=%ds work=%dB headroom=%d%% required=%dB/s", left, remaining, slaHeadroomPct(task), required)
		return
	}
	if !task.AutoThroughputEnabled {
		task.ControllerTargetBytesSec = 0
		task.ThroughputControllerReason = ""
		return
	}
	if task.SpeedBytesSec <= 0 {
		task.ControllerTargetBytesSec = 0 // discovery phase: do not cap the first samples
		task.ThroughputControllerReason = "AUTO discovery: waiting for throughput samples"
		return
	}
	pct := int64(autoProbePct(task))
	switch strings.ToUpper(strings.TrimSpace(task.FlowControlLevel)) {
	case "WARN":
		if pct > 90 {
			pct = 90
		}
	case "CRITICAL":
		if pct > 70 {
			pct = 70
		}
	}
	target := task.SpeedBytesSec * pct / 100
	target = capControllerTarget(task, target, now)
	task.ControllerTargetBytesSec = target
	task.ThroughputControllerReason = fmt.Sprintf("AUTO speed=%dB/s probe=%d%% pressure=%s target=%dB/s", task.SpeedBytesSec, autoProbePct(task), task.FlowControlLevel, target)
}

func topologyHalfOpenCooldown() time.Duration {
	seconds := intEnv("QMIGRATION_TOPOLOGY_HALF_OPEN_AFTER_SECONDS", 300)
	return time.Duration(seconds) * time.Second
}

// promoteHalfOpenTopologies is only called after the normal scheduler cannot
// find any eligible work. It converts cooled-down open circuits into a single-
// concurrency HALF_OPEN probe state. Existing RC34 profiles without a state
// timestamp are timestamped first and must cool down before their first probe.
func (s *Service) promoteHalfOpenTopologies(ctx context.Context, now time.Time) (bool, error) {
	migrations, err := s.repo.ListMigrations(ctx)
	if err != nil {
		return false, err
	}
	cooldown := topologyHalfOpenCooldown()
	changed := false
	for _, task := range migrations {
		if task.Status != domain.StatusFullMigrating {
			continue
		}
		tables, err := s.repo.ListMigrationTables(ctx, task.ID)
		if err != nil {
			return changed, err
		}
		for i := range tables {
			tableChanged := false
			if tables[i].TopologyPerformance == nil {
				continue
			}
			for topologyID, profile := range tables[i].TopologyPerformance {
				if perfmodel.NormalizeTopologyHealth(profile.Health) != "CIRCUIT_OPEN" {
					continue
				}
				if profile.HealthChangedAt.IsZero() {
					profile.HealthChangedAt = now
					tables[i].TopologyPerformance[topologyID] = profile
					tableChanged = true
					continue
				}
				if now.Sub(profile.HealthChangedAt) < cooldown {
					continue
				}
				if !profile.LastProbeAt.IsZero() && now.Sub(profile.LastProbeAt) < cooldown {
					continue
				}
				profile.Health = "HALF_OPEN"
				profile.LastProbeAt = now
				profile.HealthChangedAt = now
				tables[i].TopologyPerformance[topologyID] = profile
				tableChanged = true
				changed = true
				s.logTask(ctx, task.ID, "", tables[i].ID, "", "INFO", fmt.Sprintf("topology %s entered HALF_OPEN probe after circuit cooldown", topologyID))
			}
			if tableChanged {
				if err := s.repo.UpdateMigrationTable(ctx, &tables[i]); err != nil {
					return changed, err
				}
			}
		}
	}
	return changed, nil
}

func (s *Service) ClaimChunk(ctx context.Context, workerID string) (*domain.ChunkJob, error) {
	worker, err := s.repo.GetWorker(ctx, workerID)
	if err != nil {
		return nil, fmt.Errorf("worker: %w", err)
	}
	if workerOverloaded(worker) {
		return nil, repository.ErrNoChunk
	}
	if better, _ := s.betterWorkerAvailable(ctx, worker); better {
		return nil, repository.ErrNoChunk
	}
	chunk, err := s.repo.ClaimChunk(ctx, workerID, chunkLease, worker.Capabilities)
	if errors.Is(err, repository.ErrNoChunk) {
		if changed, promoteErr := s.promoteHalfOpenTopologies(ctx, time.Now()); promoteErr != nil {
			return nil, promoteErr
		} else if changed {
			chunk, err = s.repo.ClaimChunk(ctx, workerID, chunkLease, worker.Capabilities)
		}
	}
	if err != nil {
		return nil, err
	}
	table, err := s.repo.GetMigrationTable(ctx, chunk.TableID)
	if err != nil {
		return nil, err
	}
	task, err := s.repo.GetMigration(ctx, chunk.TaskID)
	if err != nil {
		return nil, err
	}
	// Start the SLA clock on the first actual Full worker claim, not task
	// creation/precheck time. This keeps queueing and schema-planning time from
	// consuming the operator's data-copy SLA. AUTO/SLA targets are also seeded
	// before the first batch so pacing is effective immediately.
	if task.AutoThroughputEnabled || task.CompletionSLASeconds > 0 {
		if task.CompletionSLASeconds > 0 && task.SLAStartedAt.IsZero() {
			task.SLAStartedAt = time.Now()
		}
		if tables, e := s.repo.ListMigrationTables(ctx, task.ID); e == nil {
			reconcileThroughputController(task, tables, time.Now())
			task.UpdatedAt = time.Now()
			_ = s.repo.UpdateMigration(ctx, task)
		}
	}
	if cap := resolveTaskParallelismCap(task, time.Now()); cap > 0 && (task.EffectiveParallelism <= 0 || task.EffectiveParallelism > cap) {
		task.EffectiveParallelism = cap
		task.UpdatedAt = time.Now()
		_ = s.repo.UpdateMigration(ctx, task)
	}
	source, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, err
	}
	target, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, err
	}
	engineName := table.Engine
	if engineName == "" || engineName == "auto" {
		engineName = task.FullEngine
	}
	if engineName == "" || engineName == "auto" {
		engineName = unifiedEngineName
	}
	engineName = normalizeEngineName(engineName)
	now := time.Now()
	readMB, writeMB, _, rowsPS, qps := resolveTaskRateLimits(task, now)
	readBPS, writeBPS, targetBPS := taskWorkerBudgets(task, now)
	job := &domain.ChunkJob{Chunk: *chunk, Table: *table, Engine: engineName, Source: domain.Credential(*source), Target: domain.Credential(*target), BatchRows: task.BatchRows, ReadLimitMBps: readMB, WriteLimitMBps: writeMB, ReadBytesPerSec: readBPS, WriteBytesPerSec: writeBPS, TargetBytesPerSec: targetBPS, RowsPerSecond: rowsPS, QPS: qps, TransformRules: append([]domain.TransformRule(nil), task.TransformRules...)}
	if len(chunk.PlacementHint) > 0 && !labelsContain(worker.Labels, chunk.PlacementHint) {
		job.WorkSteal = true
		job.WorkStealReason = fmt.Sprintf("idle worker stole remote placement topology=%s hint=%v", chunk.TopologyID, chunk.PlacementHint)
		s.logTask(ctx, task.ID, workerID, table.ID, chunk.ID, "INFO", job.WorkStealReason)
	}
	return job, nil
}

// adaptiveBatchTarget converts the latest connector latency into a stable next
// batch target. Changes are deliberately bounded to +25% / -50% per feedback
// round so large 10-40TB migrations converge without the 2x oscillation of the
// original worker-local heuristic.
func adaptiveBatchTarget(progress domain.ChunkProgress) int {
	current := progress.LastBatchRows
	if current <= 0 {
		return 0
	}
	latency := progress.LastReadMS
	if progress.LastWriteMS > latency {
		latency = progress.LastWriteMS
	}
	if latency <= 0 {
		return 0
	}
	targetMS := int64(intEnv("QMIGRATION_ADAPTIVE_BATCH_TARGET_MS", 1200))
	desired := current
	switch {
	case latency > targetMS:
		desired = int(int64(current) * targetMS / latency)
	case latency < targetMS*3/4:
		step := current / 4
		if step < 1 {
			step = 1
		}
		desired = current + step
	}
	minChange := current / 2
	if minChange < 50 {
		minChange = 50
	}
	maxChange := current + current/4
	if maxChange < current+1 {
		maxChange = current + 1
	}
	if desired < minChange {
		desired = minChange
	}
	if desired > maxChange {
		desired = maxChange
	}
	minRows := intEnv("QMIGRATION_ADAPTIVE_BATCH_MIN_ROWS", 50)
	maxRows := intEnv("QMIGRATION_ADAPTIVE_BATCH_MAX_ROWS", 5000)
	if minRows > maxRows {
		minRows = maxRows
	}
	if desired < minRows {
		desired = minRows
	}
	if desired > maxRows {
		desired = maxRows
	}
	return desired
}

func ewmaRate(previous, instant int64) int64 {
	if instant < 0 {
		instant = 0
	}
	if previous <= 0 {
		return instant
	}
	alpha := int64(intEnv("QMIGRATION_SPEED_EWMA_ALPHA_PCT", 25))
	if alpha > 100 {
		alpha = 100
	}
	return (previous*(100-alpha) + instant*alpha) / 100
}

func backpressureControl(progress domain.ChunkProgress, worker *domain.Worker) domain.ChunkControl {
	warnMS := int64(intEnv("QMIGRATION_BACKPRESSURE_WARN_MS", 3000))
	criticalMS := int64(intEnv("QMIGRATION_BACKPRESSURE_CRITICAL_MS", 8000))
	latency := progress.LastReadMS
	reason := "source read latency"
	if progress.LastWriteMS > latency {
		latency = progress.LastWriteMS
		reason = "target write latency"
	}
	batch := progress.LastBatchRows
	if batch <= 0 {
		batch = 500
	}
	control := domain.ChunkControl{Level: "NORMAL", TargetBatchRows: adaptiveBatchTarget(progress)}
	if latency >= criticalMS {
		control.Level = "CRITICAL"
		control.PauseMS = intEnv("QMIGRATION_BACKPRESSURE_CRITICAL_PAUSE_MS", 1000)
		control.MaxBatchRows = batch / 2
		if control.MaxBatchRows < 50 {
			control.MaxBatchRows = 50
		}
		control.Reason = fmt.Sprintf("%s %dms >= %dms", reason, latency, criticalMS)
		if control.TargetBatchRows <= 0 || control.TargetBatchRows > control.MaxBatchRows {
			control.TargetBatchRows = control.MaxBatchRows
		}
		return control
	}
	if latency >= warnMS {
		control.Level = "WARN"
		control.PauseMS = intEnv("QMIGRATION_BACKPRESSURE_WARN_PAUSE_MS", 250)
		control.MaxBatchRows = batch * 3 / 4
		if control.MaxBatchRows < 100 {
			control.MaxBatchRows = 100
		}
		control.Reason = fmt.Sprintf("%s %dms >= %dms", reason, latency, warnMS)
	}
	if worker != nil {
		cpuWarn := floatEnv("QMIGRATION_BACKPRESSURE_WORKER_CPU_PCT", 90)
		memWarn := floatEnv("QMIGRATION_BACKPRESSURE_WORKER_MEMORY_PCT", 90)
		if (worker.CPUUsagePct > 0 && worker.CPUUsagePct >= cpuWarn) || (worker.MemoryUsagePct > 0 && worker.MemoryUsagePct >= memWarn) {
			if control.Level == "NORMAL" {
				control.Level = "WARN"
				control.PauseMS = intEnv("QMIGRATION_BACKPRESSURE_WARN_PAUSE_MS", 250)
				control.MaxBatchRows = batch * 3 / 4
				if control.MaxBatchRows < 100 {
					control.MaxBatchRows = 100
				}
			}
			control.Reason = fmt.Sprintf("worker pressure cpu=%.1f%% memory=%.1f%%", worker.CPUUsagePct, worker.MemoryUsagePct)
		}
	}
	if control.MaxBatchRows > 0 && (control.TargetBatchRows <= 0 || control.TargetBatchRows > control.MaxBatchRows) {
		control.TargetBatchRows = control.MaxBatchRows
	}
	return control
}

func (s *Service) RenewChunk(ctx context.Context, workerID, chunkID string, progress domain.ChunkProgress) (domain.ChunkControl, error) {
	c, err := s.repo.GetChunk(ctx, chunkID)
	if err != nil {
		return domain.ChunkControl{}, err
	}
	task, err := s.repo.GetMigration(ctx, c.TaskID)
	if err != nil {
		return domain.ChunkControl{}, err
	}
	if task.Status != domain.StatusFullMigrating {
		return domain.ChunkControl{}, fmt.Errorf("task is %s; chunk execution should stop", task.Status)
	}
	worker, _ := s.repo.GetWorker(ctx, workerID)
	control := backpressureControl(progress, worker)
	taskLevel, taskReason := s.reconcileTaskFlowControl(ctx, task, control.Level, control.Reason)
	control = mergeTaskPressure(control, taskLevel, taskReason, progress.LastBatchRows)
	control.ReadBytesPerSec, control.WriteBytesPerSec, control.TargetBytesPerSec = taskWorkerBudgets(task, time.Now())
	control = s.applyTopologyRunningPressure(ctx, task, c, control)
	progress.BackpressureLevel = control.Level
	if strings.TrimSpace(progress.CursorJSON) != "" || progress.RowsRead > 0 || progress.RowsWritten > 0 || progress.BytesRead > 0 || progress.BytesWritten > 0 || progress.LastReadMS > 0 || progress.LastWriteMS > 0 {
		if err := s.repo.UpdateChunkProgress(ctx, chunkID, workerID, progress); err != nil {
			return domain.ChunkControl{}, err
		}
	}
	if err := s.repo.RenewChunkLease(ctx, chunkID, workerID, chunkLease); err != nil {
		return domain.ChunkControl{}, err
	}
	if yield, reason := s.shouldYieldRunningChunk(ctx, task, c, worker, progress); yield {
		control.YieldAfterBatch = true
		control.YieldReason = reason
	}
	return control, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func remainderChunkBase(c *domain.MigrationChunk, no int) domain.MigrationChunk {
	return domain.MigrationChunk{
		ID: newID("chk"), TaskID: c.TaskID, TableID: c.TableID, ChunkNo: no,
		SplitType: c.SplitType, PrimaryKey: c.PrimaryKey, Status: domain.ChunkPending,
		PlacementHint: cloneStringMap(c.PlacementHint), TopologyID: c.TopologyID, TopologyKind: c.TopologyKind,
		PartitionName: c.PartitionName, HashBucket: c.HashBucket, HashBuckets: c.HashBuckets, CustomWhere: c.CustomWhere,
		FaultDomain: cloneStringMap(c.FaultDomain),
	}
}

func boundedPieceCount(task *domain.MigrationTask, remaining int64) int {
	pieces := desiredTaskWorkers(task)
	if pieces < 1 {
		pieces = 1
	}
	if hard := intEnv("QMIGRATION_RUNNING_CHUNK_REBALANCE_MAX_PIECES", 16); pieces > hard {
		pieces = hard
	}
	if remaining > 0 && int64(pieces) > remaining {
		pieces = int(remaining)
	}
	if pieces < 1 {
		pieces = 1
	}
	return pieces
}

func (s *Service) buildYieldRemainders(ctx context.Context, task *domain.MigrationTask, table *domain.MigrationTable, c *domain.MigrationChunk, cursorJSON string, maxChunkNo int) ([]domain.MigrationChunk, error) {
	nextNo := maxChunkNo + 1
	if numericRangeSplit(c.SplitType) {
		after, ok := numericResumeCursor(cursorJSON)
		if !ok || after < c.Start || after >= c.End {
			return nil, nil
		}
		oldEnd := c.End
		c.End = after
		c.CursorJSON = cursorJSON
		c.SplitType = "PK_RANGE_REBALANCED"
		remaining := oldEnd - after
		pieces := boundedPieceCount(task, remaining)
		base, extra := remaining/int64(pieces), remaining%int64(pieces)
		start := after + 1
		created := make([]domain.MigrationChunk, 0, pieces)
		for i := 0; i < pieces; i++ {
			sz := base
			if int64(i) < extra {
				sz++
			}
			end := start + sz - 1
			ch := remainderChunkBase(c, nextNo)
			nextNo++
			ch.SplitType = "PK_RANGE_REBALANCED"
			ch.Start = start
			ch.End = end
			created = append(created, ch)
			start = end + 1
		}
		if start != oldEnd+1 {
			return nil, fmt.Errorf("running numeric yield produced a gap for chunk %s", c.ID)
		}
		return created, nil
	}
	if !strings.Contains(strings.ToUpper(c.SplitType), "KEYSET") {
		return nil, nil
	}
	cursor, err := decodeKeysetBound(cursorJSON)
	if err != nil {
		return nil, err
	}
	if len(cursor) == 0 || strings.TrimSpace(cursorJSON) == strings.TrimSpace(c.EndCursorJSON) {
		return nil, nil
	}
	oldEnd := c.EndCursorJSON
	c.EndCursorJSON = cursorJSON
	c.CursorJSON = cursorJSON
	pieces := boundedPieceCount(task, 0)
	boundsJSON := []string{}
	if pieces > 1 && s.connectors != nil {
		if source, e := s.repo.GetDataSource(ctx, task.SourceID); e == nil {
			if src, e := s.connectors.New(*source); e == nil {
				defer src.Close()
				if splitter, ok := src.(connector.KeysetBoundaryConnector); ok {
					keys := append([]string(nil), table.PrimaryKeys...)
					if len(keys) == 0 && table.PrimaryKey != "" {
						keys = []string{table.PrimaryKey}
					}
					upper, _ := decodeKeysetBound(oldEnd)
					if len(keys) > 0 {
						if bounds, e := splitter.PlanKeysetBoundaries(ctx, connector.KeysetBoundaryRequest{Schema: table.SourceSchema, Table: table.SourceTable, Keys: keys, Columns: table.Columns, Partitions: pieces, LowerBound: cursor, UpperBound: upper}); e == nil {
							seen := map[string]bool{strings.TrimSpace(cursorJSON): true, strings.TrimSpace(oldEnd): true}
							for _, b := range bounds {
								raw, _ := json.Marshal(b)
								v := string(raw)
								if !seen[v] {
									seen[v] = true
									boundsJSON = append(boundsJSON, v)
									if len(boundsJSON) >= pieces-1 {
										break
									}
								}
							}
						}
					}
				}
			}
		}
	}
	points := append([]string{cursorJSON}, boundsJSON...)
	points = append(points, oldEnd)
	created := make([]domain.MigrationChunk, 0, len(points)-1)
	for i := 0; i < len(points)-1; i++ {
		if strings.TrimSpace(points[i]) == strings.TrimSpace(points[i+1]) {
			continue
		}
		ch := remainderChunkBase(c, nextNo)
		nextNo++
		ch.SplitType = "PRIMARY_KEY_KEYSET"
		ch.StartCursorJSON = points[i]
		ch.EndCursorJSON = points[i+1]
		if i == 0 {
			ch.CursorJSON = cursorJSON
		}
		created = append(created, ch)
	}
	return created, nil
}

func (s *Service) completeYieldedChunk(ctx context.Context, workerID string, c *domain.MigrationChunk, result domain.ChunkResult) error {
	task, err := s.repo.GetMigration(ctx, c.TaskID)
	if err != nil {
		return err
	}
	table, err := s.repo.GetMigrationTable(ctx, c.TableID)
	if err != nil {
		return err
	}
	maxChunkNo, err := repository.MaxTaskChunkNo(ctx, s.repo, c.TaskID)
	if err != nil {
		return err
	}
	cursor := strings.TrimSpace(result.CursorJSON)
	if cursor == "" {
		cursor = strings.TrimSpace(c.CursorJSON)
	}
	completed := *c
	completed.RowsRead = result.RowsRead
	completed.RowsWritten = result.RowsWritten
	completed.BytesRead = result.BytesRead
	completed.BytesWritten = result.BytesWritten
	completed.CursorJSON = cursor
	created, err := s.buildYieldRemainders(ctx, task, table, &completed, cursor, maxChunkNo)
	if err != nil {
		return err
	}
	if len(created) > 0 {
		created, err = s.relocateYieldRemainders(ctx, task, table, c, created)
		if err != nil {
			return err
		}
	}
	if len(created) == 0 {
		finishedAtCursor := false
		if numericRangeSplit(c.SplitType) {
			if after, ok := numericResumeCursor(cursor); ok && after >= c.End {
				finishedAtCursor = true
			}
		} else if strings.Contains(strings.ToUpper(c.SplitType), "KEYSET") {
			finishedAtCursor = strings.TrimSpace(cursor) != "" && strings.TrimSpace(cursor) == strings.TrimSpace(c.EndCursorJSON)
		}
		if !finishedAtCursor {
			return fmt.Errorf("refusing to complete yielded chunk %s without a provable remainder or end cursor", c.ID)
		}
		completed.Status = domain.ChunkSuccess
		completed.WorkerID = ""
		completed.LeaseUntil = time.Time{}
		completed.FinishedAt = time.Now()
		if err := s.repo.UpdateChunk(ctx, &completed); err != nil {
			return err
		}
		return s.refreshProgress(ctx, c.TaskID, result)
	}
	completed.Status = domain.ChunkSuccess
	completed.WorkerID = ""
	completed.LeaseUntil = time.Time{}
	completed.FinishedAt = time.Now()
	if err := s.repo.YieldChunk(ctx, workerID, &completed, created); err != nil {
		return err
	}
	task.AdaptiveRunningYields++
	task.AdaptiveHotspotSplits += int64(len(created))
	yieldReason := strings.ToLower(strings.TrimSpace(result.YieldReason))
	if strings.HasPrefix(yieldReason, "topology circuit drain") {
		task.AdaptiveTopologyDrains++
	}
	if strings.HasPrefix(yieldReason, "topology degraded shed") {
		task.AdaptiveTopologyDegradedYields++
	}
	if strings.HasPrefix(yieldReason, "fault-domain shed") {
		task.AdaptiveFaultDomainYields++
	}
	task.UpdatedAt = time.Now()
	_ = s.repo.UpdateMigration(ctx, task)
	reason := strings.TrimSpace(result.YieldReason)
	if reason == "" {
		reason = "adaptive rebalance"
	}
	s.logTask(ctx, c.TaskID, workerID, c.TableID, c.ID, "INFO", fmt.Sprintf("running chunk yielded cursor=%s remainders=%d reason=%s", cursor, len(created), reason))
	return s.refreshProgress(ctx, c.TaskID, result)
}

func (s *Service) CompleteChunk(ctx context.Context, workerID, chunkID string, result domain.ChunkResult) error {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	c, err := s.repo.GetChunk(ctx, chunkID)
	if err != nil {
		return err
	}
	if c.Status != domain.ChunkRunning || c.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	if err := s.recordTablePerformance(ctx, c, result); err != nil {
		return err
	}
	if result.Yielded {
		return s.completeYieldedChunk(ctx, workerID, c, result)
	}
	c.Status = domain.ChunkSuccess
	c.RowsRead = result.RowsRead
	c.RowsWritten = result.RowsWritten
	c.BytesRead = result.BytesRead
	c.BytesWritten = result.BytesWritten
	c.FinishedAt = time.Now()
	c.LeaseUntil = time.Time{}
	if err := s.repo.UpdateChunk(ctx, c); err != nil {
		return err
	}
	if err := s.adaptPendingChunks(ctx, c, result); err != nil {
		return err
	}
	s.logTask(ctx, c.TaskID, workerID, c.TableID, c.ID, "INFO", fmt.Sprintf("chunk #%d completed rows=%d bytes=%d duration_ms=%d", c.ChunkNo, result.RowsWritten, result.BytesWritten, result.DurationMS))
	return s.refreshProgress(ctx, c.TaskID, result)
}
func intEnv(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// adaptPendingChunks refines only unclaimed work after a completed chunk proves
// that the current granularity is too coarse. Integer ranges split by midpoint;
// bounded keyset chunks ask the source connector for a real median key inside
// the existing [lower, upper) boundary. Running/completed chunks are immutable,
// so durable checkpoints remain gap-free and resumable.
func decodeKeysetBound(raw string) ([]connector.Value, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var values []connector.Value
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, err
	}
	return values, nil
}

func profileTargetSeconds() int { return intEnv("QMIGRATION_ADAPTIVE_CHUNK_TARGET_SECONDS", 30) }

func topologyRecoveryMinDegradedDuration() time.Duration {
	seconds := intEnv("QMIGRATION_TOPOLOGY_RECOVERY_MIN_DEGRADED_SECONDS", 30)
	if seconds < 0 {
		seconds = 0
	}
	return time.Duration(seconds) * time.Second
}

func topologyRecoveryHealthyGoodSamples() int {
	n := intEnv("QMIGRATION_TOPOLOGY_RECOVERY_HEALTHY_GOOD_SAMPLES", 8)
	if n < 2 {
		n = 2
	}
	return n
}

func topologyRecoveryStepGoodSamples() int {
	n := intEnv("QMIGRATION_TOPOLOGY_RECOVERY_STEP_GOOD_SAMPLES", 2)
	if n < 1 {
		n = 1
	}
	return n
}

func topologyRecoveryConcurrencyCap(goodStreak int) int {
	base := repository.TopologyDegradedMaxConcurrency()
	maxCap := repository.TopologyRecoveryMaxConcurrency()
	if maxCap <= base || goodStreak <= 0 {
		return base
	}
	cap := base + goodStreak/topologyRecoveryStepGoodSamples()
	if cap > maxCap {
		cap = maxCap
	}
	return cap
}

func recommendedRowsForTopology(table *domain.MigrationTable, topologyID string) int64 {
	if table == nil {
		return 0
	}
	if topologyID != "" && table.TopologyPerformance != nil {
		if p, ok := table.TopologyPerformance[topologyID]; ok && p.RecommendedChunkRows > 0 {
			return p.RecommendedChunkRows
		}
	}
	return table.RecommendedChunkRows
}

func estimateNumericChunkRows(table *domain.MigrationTable, chunk *domain.MigrationChunk) int64 {
	if table == nil || chunk == nil || table.EstimatedRows <= 0 || table.MaxPK < table.MinPK || chunk.End < chunk.Start {
		return 0
	}
	totalSpan := table.MaxPK - table.MinPK + 1
	if totalSpan <= 0 {
		return 0
	}
	span := chunk.End - chunk.Start + 1
	return (table.EstimatedRows*span + totalSpan - 1) / totalSpan
}

func (s *Service) recordTablePerformance(ctx context.Context, chunk *domain.MigrationChunk, result domain.ChunkResult) error {
	if chunk == nil || result.DurationMS <= 0 || result.RowsWritten <= 0 {
		return nil
	}
	table, err := s.repo.GetMigrationTable(ctx, chunk.TableID)
	if err != nil {
		return err
	}
	bps := result.BytesWritten * 1000 / result.DurationMS
	rps := result.RowsWritten * 1000 / result.DurationMS
	alpha := intEnv("QMIGRATION_TABLE_PROFILE_EWMA_ALPHA_PCT", 25)
	table.ProfileBytesPerSec = perfmodel.EWMA(table.ProfileBytesPerSec, bps, alpha)
	table.ProfileRowsPerSec = perfmodel.EWMA(table.ProfileRowsPerSec, rps, alpha)
	minRows := positiveInt64Env("QMIGRATION_PREDICTED_CHUNK_MIN_ROWS", 1000)
	maxRows := positiveInt64Env("QMIGRATION_PREDICTED_CHUNK_MAX_ROWS", 10_000_000)
	table.RecommendedChunkRows = perfmodel.RecommendChunkRows(table.ProfileRowsPerSec, profileTargetSeconds(), minRows, maxRows)
	table.PerformanceSamples++
	if chunk.TopologyID != "" {
		if table.TopologyPerformance == nil {
			table.TopologyPerformance = map[string]domain.TableTopologyPerformance{}
		}
		p := table.TopologyPerformance[chunk.TopologyID]
		p.BytesPerSec = perfmodel.EWMA(p.BytesPerSec, bps, alpha)
		p.RowsPerSec = perfmodel.EWMA(p.RowsPerSec, rps, alpha)
		p.RecommendedChunkRows = perfmodel.RecommendChunkRows(p.RowsPerSec, profileTargetSeconds(), minRows, maxRows)
		p.Samples++
		p.DurationSamplesMS = perfmodel.AppendSample(p.DurationSamplesMS, result.DurationMS, intEnv("QMIGRATION_TOPOLOGY_LATENCY_SAMPLES", 64))
		p.P95DurationMS = perfmodel.Percentile(p.DurationSamplesMS, 95)
		p.P99DurationMS = perfmodel.Percentile(p.DurationSamplesMS, 99)
		slowP99 := int64(intEnv("QMIGRATION_TOPOLOGY_P99_SLOW_MS", 90000))
		profileBad := (table.ProfileBytesPerSec > 0 && p.BytesPerSec*100 < table.ProfileBytesPerSec*55) || p.P99DurationMS >= slowP99
		// Recovery hysteresis must use the current sample rather than the rolling
		// P99. Otherwise one historical outlier keeps HALF_OPEN/DEGRADED unhealthy
		// until it ages out of the latency window, defeating staged recovery.
		sampleBad := (table.ProfileBytesPerSec > 0 && bps*100 < table.ProfileBytesPerSec*55) || result.DurationMS >= slowP99
		previousHealth := perfmodel.NormalizeTopologyHealth(p.Health)
		now := time.Now()
		if sampleBad {
			p.SlowStreak++
			p.GoodStreak = 0
		} else {
			if p.SlowStreak > 0 {
				p.SlowStreak--
			}
			p.GoodStreak++
		}
		switch previousHealth {
		case "CIRCUIT_OPEN":
			// CIRCUIT_OPEN can recover only through the scheduler-created HALF_OPEN
			// probe. Samples racing with the open transition must not bypass cooldown.
			p.Health = "CIRCUIT_OPEN"
			p.GoodStreak = 0
			p.RecoveryConcurrencyCap = 0
		case "HALF_OPEN":
			if sampleBad {
				if p.SlowStreak < 5 {
					p.SlowStreak = 5
				}
				p.Health = "CIRCUIT_OPEN"
				p.GoodStreak = 0
				p.RecoveryConcurrencyCap = 0
			} else {
				// A successful probe enters a hysteresis-controlled DEGRADED recovery
				// phase. It starts at the conservative degraded cap and must earn more
				// concurrency with consecutive good samples.
				if p.SlowStreak > 1 {
					p.SlowStreak = 1
				}
				p.Health = "DEGRADED"
				p.GoodStreak = 1
				p.RecoveryConcurrencyCap = repository.TopologyDegradedMaxConcurrency()
			}
		case "DEGRADED":
			if p.HealthChangedAt.IsZero() {
				p.HealthChangedAt = now
			}
			if sampleBad {
				p.RecoveryConcurrencyCap = repository.TopologyDegradedMaxConcurrency()
				if p.SlowStreak >= 5 {
					p.Health = "CIRCUIT_OPEN"
					p.RecoveryConcurrencyCap = 0
				} else {
					p.Health = "DEGRADED"
				}
			} else {
				p.Health = "DEGRADED"
				if now.Sub(p.HealthChangedAt) >= topologyRecoveryMinDegradedDuration() {
					p.RecoveryConcurrencyCap = topologyRecoveryConcurrencyCap(p.GoodStreak)
					if p.GoodStreak >= topologyRecoveryHealthyGoodSamples() && p.RecoveryConcurrencyCap >= repository.TopologyRecoveryMaxConcurrency() {
						p.Health = "HEALTHY"
						p.SlowStreak = 0
						p.GoodStreak = 0
						p.RecoveryConcurrencyCap = 0
					}
				} else {
					p.RecoveryConcurrencyCap = repository.TopologyDegradedMaxConcurrency()
				}
			}
		default:
			p.RecoveryConcurrencyCap = 0
			if profileBad {
				p.GoodStreak = 0
				p.Health = perfmodel.TopologyHealthWithThreshold(p.BytesPerSec, table.ProfileBytesPerSec, p.P99DurationMS, p.SlowStreak, int(p.Samples), slowP99)
			} else {
				p.Health = "HEALTHY"
				p.GoodStreak = 0
			}
		}
		if perfmodel.NormalizeTopologyHealth(p.Health) != previousHealth {
			p.HealthChangedAt = now
		}
		table.TopologyPerformance[chunk.TopologyID] = p
	}
	return s.repo.UpdateMigrationTable(ctx, table)
}

func (s *Service) adaptPendingChunks(ctx context.Context, completed *domain.MigrationChunk, result domain.ChunkResult) error {
	if completed == nil || result.DurationMS <= 0 {
		return nil
	}
	targetSeconds := intEnv("QMIGRATION_ADAPTIVE_CHUNK_TARGET_SECONDS", 30)
	table, err := s.repo.GetMigrationTable(ctx, completed.TableID)
	if err != nil {
		return err
	}
	task, err := s.repo.GetMigration(ctx, completed.TaskID)
	if err != nil {
		return err
	}
	chunkBPS := int64(0)
	if result.DurationMS > 0 {
		chunkBPS = result.BytesWritten * 1000 / result.DurationMS
	}
	hotspotPct := int64(intEnv("QMIGRATION_ADAPTIVE_CHUNK_HOTSPOT_PCT", 50))
	slowByDuration := result.DurationMS >= int64(targetSeconds*2)*1000
	slowByThroughput := task.SpeedBytesSec > 0 && chunkBPS > 0 && chunkBPS*100 < task.SpeedBytesSec*hotspotPct
	if !slowByDuration && !slowByThroughput {
		return nil
	}
	engineName := table.Engine
	if engineName == "" {
		engineName = unifiedEngineName
	}
	if normalizeEngineName(engineName) != unifiedEngineName {
		return nil
	}
	chunks, err := repository.ListPendingTableChunks(ctx, s.repo, completed.TaskID, completed.TableID)
	if err != nil {
		return err
	}
	maxNo, err := repository.MaxTaskChunkNo(ctx, s.repo, completed.TaskID)
	if err != nil {
		return err
	}
	baseSplits := intEnv("QMIGRATION_ADAPTIVE_CHUNK_MAX_SPLITS", 8)
	if baseSplits < 1 {
		baseSplits = 1
	}
	maxSplits := 1 // a slow chunk still gets one refinement even without idle capacity
	desiredWorkers := task.EffectiveParallelism
	if desiredWorkers <= 0 {
		desiredWorkers = task.Parallelism
	}
	if desiredWorkers <= 0 {
		desiredWorkers = 1
	}
	counts, err := repository.CountTableRunnable(ctx, s.repo, completed.TaskID, completed.TableID)
	if err != nil {
		return err
	}
	runnable := counts.Runnable()
	if deficit := desiredWorkers - runnable; deficit > maxSplits {
		maxSplits = deficit
	}
	if maxSplits > baseSplits {
		maxSplits = baseSplits
	}
	hardMax := intEnv("QMIGRATION_ADAPTIVE_CHUNK_HARD_MAX_SPLITS", 16)
	if envDefaultOn("QMIGRATION_PREDICTIVE_CHUNK_SIZING") {
		for i := range chunks {
			ch := &chunks[i]
			if ch.TableID != completed.TableID || ch.Status != domain.ChunkPending || !numericRangeSplit(ch.SplitType) {
				continue
			}
			rec := recommendedRowsForTopology(table, ch.TopologyID)
			if rec <= 0 {
				continue
			}
			need := perfmodel.PiecesForRows(estimateNumericChunkRows(table, ch), rec, hardMax)
			if need-1 > maxSplits {
				maxSplits = need - 1
			}
		}
	}
	if maxSplits > hardMax {
		maxSplits = hardMax
	}
	if maxSplits > baseSplits && baseSplits > 0 {
		maxSplits = baseSplits
	}
	created := make([]domain.MigrationChunk, 0, maxSplits)
	splits := 0

	// Numeric ranges can be repartitioned into multiple gap-free pieces in one
	// control round. This is safer and faster than waiting for repeated completed
	// chunks when one large pending range is starving otherwise-idle workers.
	for i := range chunks {
		if splits >= maxSplits {
			break
		}
		ch := &chunks[i]
		if ch.TableID != completed.TableID || ch.Status != domain.ChunkPending || (ch.SplitType != "PRIMARY_KEY_RANGE" && ch.SplitType != "PK_RANGE" && ch.SplitType != "PK_RANGE_ADAPTIVE") || ch.End <= ch.Start {
			continue
		}
		remainingSplits := maxSplits - splits
		pieces := remainingSplits + 1
		if need := desiredWorkers - runnable + 1; need > 1 && need < pieces {
			pieces = need
		}
		if pieces < 2 {
			pieces = 2
		}
		span := ch.End - ch.Start + 1
		if int64(pieces) > span {
			pieces = int(span)
		}
		if pieces < 2 {
			continue
		}
		originalStart, originalEnd := ch.Start, ch.End
		base := span / int64(pieces)
		extra := span % int64(pieces)
		cursor := originalStart
		for piece := 0; piece < pieces; piece++ {
			size := base
			if int64(piece) < extra {
				size++
			}
			end := cursor + size - 1
			if piece == 0 {
				ch.Start, ch.End, ch.SplitType = cursor, end, "PK_RANGE_ADAPTIVE"
				if err := s.repo.UpdateChunk(ctx, ch); err != nil {
					return err
				}
			} else {
				maxNo++
				created = append(created, domain.MigrationChunk{ID: newID("chk"), TaskID: ch.TaskID, TableID: ch.TableID, ChunkNo: maxNo, SplitType: "PK_RANGE_ADAPTIVE", PrimaryKey: ch.PrimaryKey, Start: cursor, End: end, Status: domain.ChunkPending})
				splits++
			}
			cursor = end + 1
		}
		if cursor != originalEnd+1 {
			return fmt.Errorf("adaptive numeric split produced a gap for chunk %s", ch.ID)
		}
		runnable += pieces - 1
	}
	if len(created) > 0 {
		if err := s.repo.CreateChunks(ctx, created); err != nil {
			return err
		}
		task.AdaptiveHotspotSplits += int64(len(created))
		task.UpdatedAt = time.Now()
		_ = s.repo.UpdateMigration(ctx, task)
		s.logTask(ctx, task.ID, "", table.ID, completed.ID, "INFO", fmt.Sprintf("adaptive hotspot rebalance created=%d chunk_bps=%d task_bps=%d runnable=%d desired=%d", len(created), chunkBPS, task.SpeedBytesSec, runnable, desiredWorkers))
		return nil
	}

	// Generic stable keys need an actual source key as split point. Never refine
	// a previously-started pending chunk because its durable CursorJSON is an
	// exclusive resume position; leaving it intact avoids reinterpreting progress.
	if s.connectors == nil {
		return nil
	}
	source, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return err
	}
	src, err := s.connectors.New(*source)
	if err != nil {
		return nil // adaptive refinement is best-effort; migration remains valid
	}
	defer src.Close()
	splitter, ok := src.(connector.KeysetBoundaryConnector)
	if !ok {
		return nil
	}
	keys := append([]string(nil), table.PrimaryKeys...)
	if len(keys) == 0 && table.PrimaryKey != "" {
		keys = []string{table.PrimaryKey}
	}
	if len(keys) == 0 {
		return nil
	}
	created = created[:0]
	for i := range chunks {
		if splits >= maxSplits {
			break
		}
		ch := &chunks[i]
		if ch.TableID != completed.TableID || ch.Status != domain.ChunkPending || ch.SplitType != "PRIMARY_KEY_KEYSET" || strings.TrimSpace(ch.CursorJSON) != "" || ch.RowsRead > 0 || ch.RowsWritten > 0 {
			continue
		}
		remainingSplits := maxSplits - splits
		pieces := remainingSplits + 1
		if need := desiredWorkers - runnable + 1; need > 1 && need < pieces {
			pieces = need
		}
		if pieces < 2 {
			pieces = 2
		}
		lower, e := decodeKeysetBound(ch.StartCursorJSON)
		if e != nil {
			return fmt.Errorf("decode adaptive keyset lower bound: %w", e)
		}
		upper, e := decodeKeysetBound(ch.EndCursorJSON)
		if e != nil {
			return fmt.Errorf("decode adaptive keyset upper bound: %w", e)
		}
		bounds, e := splitter.PlanKeysetBoundaries(ctx, connector.KeysetBoundaryRequest{Schema: table.SourceSchema, Table: table.SourceTable, Keys: keys, Columns: table.Columns, Partitions: pieces, LowerBound: lower, UpperBound: upper})
		if e != nil || len(bounds) == 0 {
			continue
		}
		oldEnd := ch.EndCursorJSON
		boundaryJSON := make([]string, 0, len(bounds))
		seen := map[string]bool{strings.TrimSpace(ch.StartCursorJSON): true, strings.TrimSpace(ch.EndCursorJSON): true}
		for _, b := range bounds {
			raw, e := json.Marshal(b)
			if e != nil {
				return e
			}
			v := string(raw)
			if seen[v] {
				continue
			}
			seen[v] = true
			boundaryJSON = append(boundaryJSON, v)
			if len(boundaryJSON) >= remainingSplits {
				break
			}
		}
		if len(boundaryJSON) == 0 {
			continue
		}
		ch.EndCursorJSON = boundaryJSON[0]
		if err := s.repo.UpdateChunk(ctx, ch); err != nil {
			return err
		}
		points := append(boundaryJSON, oldEnd)
		for j := 0; j < len(points)-1; j++ {
			maxNo++
			base := remainderChunkBase(ch, maxNo)
			base.SplitType = "PRIMARY_KEY_KEYSET"
			base.StartCursorJSON = points[j]
			base.EndCursorJSON = points[j+1]
			created = append(created, base)
			splits++
			runnable++
		}
	}
	if len(created) > 0 {
		if err := s.repo.CreateChunks(ctx, created); err != nil {
			return err
		}
		task.AdaptiveHotspotSplits += int64(len(created))
		task.UpdatedAt = time.Now()
		_ = s.repo.UpdateMigration(ctx, task)
	}
	return nil
}

func (s *Service) FailChunk(ctx context.Context, workerID, chunkID string, result domain.ChunkResult) error {
	s.progressMu.Lock()
	defer s.progressMu.Unlock()
	c, err := s.repo.GetChunk(ctx, chunkID)
	if err != nil {
		return err
	}
	if c.Status != domain.ChunkRunning || c.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	task, err := s.repo.GetMigration(ctx, c.TaskID)
	if err != nil {
		return err
	}
	c.LastError = result.Error
	c.LeaseUntil = time.Time{}
	c.WorkerID = ""
	if task.Status == domain.StatusPaused || task.Status == domain.StatusCancelled {
		c.Status = domain.ChunkPending
		return s.repo.UpdateChunk(ctx, c)
	}
	c.RetryCount++
	if c.RetryCount <= task.MaxRetries {
		c.Status = domain.ChunkPending
	} else {
		c.Status = domain.ChunkFailed
		c.FinishedAt = time.Now()
		task.Status = domain.StatusFailed
		task.LastError = fmt.Sprintf("chunk %s failed after %d attempts: %s", c.ID, c.RetryCount, result.Error)
		task.UpdatedAt = time.Now()
		_ = s.repo.UpdateMigration(ctx, task)
	}
	if err := s.repo.UpdateChunk(ctx, c); err != nil {
		return err
	}
	level := "WARN"
	if c.Status == domain.ChunkFailed {
		level = "ERROR"
	}
	s.logTask(ctx, c.TaskID, workerID, c.TableID, c.ID, level, fmt.Sprintf("chunk #%d failed retry=%d/%d: %s", c.ChunkNo, c.RetryCount, task.MaxRetries, result.Error))
	return nil
}

func updateSLATailRisk(task *domain.MigrationTask, tables []domain.MigrationTable, now time.Time) {
	if task == nil {
		return
	}
	var maxP95, maxP99 int64
	var worstTopology string
	for _, table := range tables {
		for topologyID, profile := range table.TopologyPerformance {
			if profile.P95DurationMS > maxP95 {
				maxP95 = profile.P95DurationMS
			}
			if profile.P99DurationMS > maxP99 {
				maxP99 = profile.P99DurationMS
				worstTopology = topologyID
			}
		}
	}
	targetMS := int64(profileTargetSeconds()) * 1000
	task.SLAP95ETASeconds = perfmodel.TailRiskETA(task.ETASeconds, targetMS, maxP95, intEnv("QMIGRATION_SLA_P95_TAIL_WEIGHT_PCT", 25))
	task.SLAP99ETASeconds = perfmodel.TailRiskETA(task.ETASeconds, targetMS, maxP99, intEnv("QMIGRATION_SLA_P99_TAIL_WEIGHT_PCT", 50))
	if task.CompletionSLASeconds <= 0 || task.SLAStartedAt.IsZero() {
		task.SLARiskLevel = "NONE"
		if maxP99 > targetMS && task.ETASeconds > 0 {
			task.SLARiskReason = fmt.Sprintf("tail-only estimate topology=%s base=%ds p95=%ds p99=%ds", worstTopology, task.ETASeconds, task.SLAP95ETASeconds, task.SLAP99ETASeconds)
		} else {
			task.SLARiskReason = ""
		}
		return
	}
	remaining := task.CompletionSLASeconds - int64(now.Sub(task.SLAStartedAt).Seconds())
	if remaining < 0 {
		remaining = 0
	}
	switch {
	case task.SLAP99ETASeconds > remaining:
		task.SLARiskLevel = "CRITICAL"
	case task.SLAP95ETASeconds > remaining:
		task.SLARiskLevel = "WARN"
	default:
		task.SLARiskLevel = "NORMAL"
	}
	task.SLARiskReason = fmt.Sprintf("sla_remaining=%ds base_eta=%ds p95_eta=%ds p99_eta=%ds worst_topology=%s", remaining, task.ETASeconds, task.SLAP95ETASeconds, task.SLAP99ETASeconds, worstTopology)
}

func (s *Service) refreshProgress(ctx context.Context, taskID string, last domain.ChunkResult) error {
	summary, err := repository.SummarizeChunks(ctx, s.repo, taskID)
	if err != nil {
		return err
	}
	task, err := s.repo.GetMigration(ctx, taskID)
	if err != nil {
		return err
	}
	tables, err := s.repo.ListMigrationTables(ctx, taskID)
	if err != nil {
		return err
	}
	for i := range tables {
		ts := summary.Tables[tables[i].ID]
		tables[i].TotalChunks = ts.Total
		tables[i].FinishedChunks = ts.Success
		tables[i].RowsMigrated = ts.RowsWritten
		tables[i].BytesMigrated = ts.BytesWritten
		if tables[i].TotalChunks > 0 && ts.Success == tables[i].TotalChunks {
			tables[i].Status = "FINISHED"
		} else if ts.Success > 0 {
			tables[i].Status = "RUNNING"
		}
		_ = s.repo.UpdateMigrationTable(ctx, &tables[i])
	}
	task.TotalChunks = summary.Total
	task.FinishedChunks = summary.Success
	task.RowsMigrated = summary.RowsWritten
	task.BytesMigrated = summary.BytesWritten
	if summary.Total > 0 {
		task.Progress = float64(summary.Success) * 100 / float64(summary.Total)
	}
	if last.DurationMS > 0 {
		instantBytes := last.BytesWritten * 1000 / last.DurationMS
		instantRows := last.RowsWritten * 1000 / last.DurationMS
		task.SpeedBytesSec = ewmaRate(task.SpeedBytesSec, instantBytes)
		task.SpeedRowsSec = ewmaRate(task.SpeedRowsSec, instantRows)
	}
	controllerNow := time.Now()
	learnThroughputController(task, tables, controllerNow)
	reconcileThroughputController(task, tables, controllerNow)
	var estimatedTotalBytes, estimatedTotalRows int64
	for _, table := range tables {
		estimatedTotalBytes += table.DataLength
		estimatedTotalRows += table.EstimatedRows
	}
	switch {
	case task.SpeedBytesSec > 0 && estimatedTotalBytes > task.BytesMigrated:
		task.ETASeconds = (estimatedTotalBytes - task.BytesMigrated) / task.SpeedBytesSec
	case task.SpeedRowsSec > 0 && estimatedTotalRows > task.RowsMigrated:
		task.ETASeconds = (estimatedTotalRows - task.RowsMigrated) / task.SpeedRowsSec
	default:
		task.ETASeconds = 0
	}
	updateSLATailRisk(task, tables, controllerNow)
	task.UpdatedAt = time.Now()
	startValidation := false
	if summary.Total > 0 && summary.Success == summary.Total && task.Status == domain.StatusFullMigrating {
		if err := Transition(task, domain.StatusFullFinished); err == nil {
			if err := s.syncGeneratedValueState(ctx, task, tables); err != nil {
				task.Status = domain.StatusFailed
				task.LastError = "generated-value state synchronization failed: " + err.Error()
				_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Generated-value state sync failed", Message: task.LastError, TaskID: task.ID, CreatedAt: time.Now()})
			} else if err := s.applyPostLoadIndexes(ctx, task, tables); err != nil {
				task.Status = domain.StatusFailed
				task.LastError = "post-load index creation failed: " + err.Error()
				_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Post-load DDL failed", Message: task.LastError, TaskID: task.ID, CreatedAt: time.Now()})
			} else if task.Mode == domain.ModeFullAndIncremental {
				// Native CDC has already been capturing into the durable spool while
				// the snapshot was running. Always catch up that backlog before
				// validation; validating a target that is knowingly behind the spool
				// produces avoidable false mismatches on active databases.
				_ = Transition(task, domain.StatusCDCCatchingUp)
			} else if task.ValidationEnabled {
				_ = Transition(task, domain.StatusValidating)
				startValidation = true
			} else {
				if err := s.applyPostLoadForeignKeys(ctx, task, tables); err != nil {
					task.Status = domain.StatusFailed
					task.LastError = "post-load foreign key creation failed: " + err.Error()
					_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Post-load DDL failed", Message: task.LastError, TaskID: task.ID, CreatedAt: time.Now()})
				} else if task.Mode == domain.ModeFull {
					if err := s.finalizeGeneratedValueModes(ctx, task, tables); err != nil {
						task.Status = domain.StatusFailed
						task.LastError = "generated-value mode finalization failed: " + err.Error()
						_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Generated-value mode finalization failed", Message: task.LastError, TaskID: task.ID, CreatedAt: time.Now()})
					} else {
						_ = Transition(task, domain.StatusFinished)
					}
				} else {
					_ = Transition(task, domain.StatusCDCCatchingUp)
				}
			}
		}
	}
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	if task.Status == domain.StatusCDCCatchingUp {
		s.scheduleCDCSpoolDrain(task.ID, "forward")
	}
	if startValidation {
		go s.validateTask(task.ID)
	}
	return nil
}

func postLoadMode(mode string) string {
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" {
		return "NONE"
	}
	return mode
}

func hasSecondaryIndexes(tables []domain.MigrationTable) bool {
	for _, table := range tables {
		for _, idx := range table.Indexes {
			if !idx.Primary && len(idx.Columns) > 0 {
				return true
			}
		}
	}
	return false
}

func hasForeignKeys(tables []domain.MigrationTable) bool {
	for _, table := range tables {
		if len(table.ForeignKeys) > 0 {
			return true
		}
	}
	return false
}

func indexCompatible(index domain.IndexInfo, existing []domain.IndexInfo) bool {
	for _, candidate := range existing {
		if candidate.Primary == index.Primary && candidate.Unique == index.Unique && sameColumnsFold(candidate.Columns, index.Columns) {
			return true
		}
	}
	return false
}

func foreignKeyCompatible(fk domain.ForeignKeyInfo, existing []domain.ForeignKeyInfo) bool {
	for _, candidate := range existing {
		if sameColumnsFold(candidate.Columns, fk.Columns) && sameColumnsFold(candidate.ReferencedColumns, fk.ReferencedColumns) && strings.EqualFold(candidate.ReferencedSchema, fk.ReferencedSchema) && strings.EqualFold(candidate.ReferencedTable, fk.ReferencedTable) {
			return true
		}
	}
	return false
}

func (s *Service) postLoadConnector(ctx context.Context, task *domain.MigrationTask, tables []domain.MigrationTable, required bool) (connector.PostLoadSchemaConnector, func(), error) {
	if !required {
		return nil, func() {}, nil
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, func() {}, err
	}
	if err := s.connectors.Require(targetDS.Type, connector.CapabilityPostLoadSchema); err != nil {
		_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "warning", Title: "Post-load DDL deferred", Message: "Target QMigration connector does not yet advertise post-load-schema capability; the unified engine will not delegate this DDL to another runtime", TaskID: task.ID, CreatedAt: time.Now()})
		return nil, func() {}, nil
	}
	raw, err := s.connectors.New(*targetDS)
	if err != nil {
		return nil, func() {}, err
	}
	ddl, ok := raw.(connector.PostLoadSchemaConnector)
	if !ok {
		raw.Close()
		return nil, func() {}, fmt.Errorf("target connector %s does not support post-load schema DDL", targetDS.Type)
	}
	return ddl, func() { _ = raw.Close() }, nil
}

func (s *Service) syncGeneratedValueState(ctx context.Context, task *domain.MigrationTask, tables []domain.MigrationTable) error {
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return err
	}
	raw, err := s.connectors.New(*targetDS)
	if err != nil {
		return err
	}
	defer raw.Close()
	syncer, ok := raw.(connector.GeneratedValueStateConnector)
	if !ok {
		return nil
	}
	for _, table := range tables {
		cols := table.TargetColumns
		if len(cols) == 0 {
			cols = table.Columns
		}
		if err := syncer.SyncGeneratedValueState(ctx, table.TargetSchema, table.TargetTable, cols); err != nil {
			return fmt.Errorf("%s.%s generated-value state: %w", table.TargetSchema, table.TargetTable, err)
		}
	}
	return nil
}

func (s *Service) finalizeGeneratedValueModes(ctx context.Context, task *domain.MigrationTask, tables []domain.MigrationTable) error {
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return err
	}
	raw, err := s.connectors.New(*targetDS)
	if err != nil {
		return err
	}
	defer raw.Close()
	finalizer, ok := raw.(connector.CutoverGeneratedValueConnector)
	if !ok {
		return nil
	}
	for _, table := range tables {
		cols := table.TargetColumns
		if len(cols) == 0 {
			cols = table.Columns
		}
		if err := finalizer.FinalizeGeneratedValueModes(ctx, table.TargetSchema, table.TargetTable, cols); err != nil {
			return fmt.Errorf("%s.%s generated-value mode finalization: %w", table.TargetSchema, table.TargetTable, err)
		}
	}
	return nil
}

func (s *Service) applyPostLoadIndexes(ctx context.Context, task *domain.MigrationTask, tables []domain.MigrationTable) error {
	mode := postLoadMode(task.PostLoadDDLMode)
	if mode != "INDEXES" && mode != "INDEXES_AND_FOREIGN_KEYS" {
		return nil
	}
	ddl, closeFn, err := s.postLoadConnector(ctx, task, tables, hasSecondaryIndexes(tables))
	if err != nil || ddl == nil {
		return err
	}
	defer closeFn()
	for _, table := range tables {
		meta, err := ddl.GetTableMetadata(ctx, table.TargetSchema, table.TargetTable)
		if err != nil {
			return err
		}
		existing := meta.Indexes
		for _, idx := range table.Indexes {
			if idx.Primary || len(idx.Columns) == 0 || indexCompatible(idx, existing) {
				continue
			}
			if err := ddl.CreateIndex(ctx, table.TargetSchema, table.TargetTable, idx); err != nil {
				return fmt.Errorf("%s.%s index %s: %w", table.TargetSchema, table.TargetTable, idx.Name, err)
			}
			existing = append(existing, idx)
		}
	}
	return nil
}

func (s *Service) applyPostLoadForeignKeys(ctx context.Context, task *domain.MigrationTask, tables []domain.MigrationTable) error {
	if postLoadMode(task.PostLoadDDLMode) != "INDEXES_AND_FOREIGN_KEYS" || !hasForeignKeys(tables) {
		return nil
	}
	ddl, closeFn, err := s.postLoadConnector(ctx, task, tables, true)
	if err != nil || ddl == nil {
		return err
	}
	defer closeFn()
	for _, table := range tables {
		meta, err := ddl.GetTableMetadata(ctx, table.TargetSchema, table.TargetTable)
		if err != nil {
			return err
		}
		existing := meta.ForeignKeys
		for _, fk := range table.ForeignKeys {
			if len(fk.Columns) == 0 || foreignKeyCompatible(fk, existing) {
				continue
			}
			if err := ddl.CreateForeignKey(ctx, table.TargetSchema, table.TargetTable, fk); err != nil {
				return fmt.Errorf("%s.%s foreign key %s: %w", table.TargetSchema, table.TargetTable, fk.Name, err)
			}
			existing = append(existing, fk)
		}
	}
	return nil
}

func (s *Service) Precheck(ctx context.Context, id string) ([]domain.PrecheckItem, error) {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	srcDS, err := s.repo.GetDataSource(ctx, m.SourceID)
	if err != nil {
		return nil, err
	}
	dstDS, err := s.repo.GetDataSource(ctx, m.TargetID)
	if err != nil {
		return nil, err
	}
	items := []domain.PrecheckItem{}
	check := func(name string, ds *domain.DataSource, needCDC bool) {
		c, e := s.connectors.New(*ds)
		if e != nil {
			items = append(items, domain.PrecheckItem{Name: name, Level: domain.PrecheckFailed, Message: e.Error()})
			return
		}
		defer c.Close()
		if e = c.TestConnection(ctx); e != nil {
			items = append(items, domain.PrecheckItem{Name: name, Level: domain.PrecheckFailed, Message: e.Error()})
			return
		}
		v, e := c.GetVersion(ctx)
		if e != nil {
			v = "unknown"
		}
		items = append(items, domain.PrecheckItem{Name: name, Level: domain.PrecheckPass, Message: "connected, version=" + v})
		if inspector, ok := c.(connector.MigrationPrecheckConnector); ok {
			items = append(items, inspector.MigrationPrechecks(ctx, needCDC)...)
		}
	}
	check("source_connection", srcDS, m.Mode != domain.ModeFull)
	check("target_connection", dstDS, false)
	if m.ChunkRows < 1000 {
		items = append(items, domain.PrecheckItem{Name: "chunk_rows", Level: domain.PrecheckWarning, Message: "chunk_rows is very small and may increase scheduling overhead"})
	} else {
		items = append(items, domain.PrecheckItem{Name: "chunk_rows", Level: domain.PrecheckPass, Message: fmt.Sprintf("%d rows", m.ChunkRows)})
	}
	if m.Mode != domain.ModeFull {
		msg := "QMigration Unified CDC uses a built-in native log reader and apply-before-checkpoint semantics"
		if srcDS.Type.IsMySQLFamily() {
			msg = "QMigration Unified CDC uses the MySQL binlog protocol directly; ROW/FULL prechecks and transaction checkpoints are enforced"
		} else if srcDS.Type.IsPostgreSQLFamily() {
			msg = "QMigration Unified CDC uses PostgreSQL pgoutput directly and acknowledges LSN only after target apply"
		}
		items = append(items, domain.PrecheckItem{Name: "cdc_engine", Level: domain.PrecheckPass, Message: msg})
	}

	if m.Mode != domain.ModeIncremental {
		if e := s.connectors.Require(srcDS.Type, connector.CapabilityFullRead); e != nil {
			items = append(items, domain.PrecheckItem{Name: "source_connector", Level: domain.PrecheckFailed, Message: e.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "source_connector", Level: domain.PrecheckPass, Message: "QMigration native full-read capability available"})
		}
		if e := s.connectors.Require(dstDS.Type, connector.CapabilityFullWrite); e != nil {
			items = append(items, domain.PrecheckItem{Name: "target_connector", Level: domain.PrecheckFailed, Message: e.Error()})
		} else {
			items = append(items, domain.PrecheckItem{Name: "target_connector", Level: domain.PrecheckPass, Message: "QMigration native full-write capability available"})
		}
	}
	if m.Mode != domain.ModeFull {
		if e := s.connectors.Require(srcDS.Type, connector.CapabilityCDCRead); e != nil {
			items = append(items, domain.PrecheckItem{Name: "source_cdc_connector", Level: domain.PrecheckFailed, Message: e.Error()})
		}
		if e := s.connectors.Require(dstDS.Type, connector.CapabilityCDCApply); e != nil {
			items = append(items, domain.PrecheckItem{Name: "target_cdc_connector", Level: domain.PrecheckFailed, Message: e.Error()})
		} else if !s.connectors.Supports(dstDS.Type, connector.CapabilityCDCTransactional) {
			items = append(items, domain.PrecheckItem{Name: "target_cdc_atomicity", Level: domain.PrecheckWarning, Message: "target CDC apply is retry-idempotent but does not advertise source-transaction atomicity; intermediate states may be visible until a replayed transaction converges"})
		}
	}

	return items, nil
}

// AssessCompatibility performs a read-only migration assessment before execution.
// It reports connector gaps explicitly. QMigration never hides an unsupported native connector behind a third-party execution engine.
func (s *Service) AssessCompatibility(ctx context.Context, id string) (*domain.CompatibilityAssessment, error) {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	sourceDS, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, err
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, err
	}
	out := &domain.CompatibilityAssessment{TaskID: id, CanStart: true, AssessedAt: time.Now()}
	add := func(level domain.CompatibilityLevel, objectType, sourceObject, targetObject, code, message string) {
		out.Findings = append(out.Findings, domain.CompatibilityFinding{ObjectType: objectType, SourceObject: sourceObject, TargetObject: targetObject, Level: level, Code: code, Message: message})
		switch level {
		case domain.CompatibilityCompatible:
			out.Compatible++
		case domain.CompatibilityWarning:
			out.Warnings++
		case domain.CompatibilityUnsupported:
			out.Unsupported++
			out.CanStart = false
		}
	}
	fullSupported := true
	if task.Mode != domain.ModeIncremental {
		if e := s.requireFullPair(*sourceDS, *targetDS); e != nil {
			fullSupported = false
			add(domain.CompatibilityUnsupported, "ENGINE", string(sourceDS.Type), string(targetDS.Type), "UNIFIED_CONNECTOR_MISSING", e.Error())
		}
	}
	if fullSupported {
		add(domain.CompatibilityCompatible, "ENGINE", unifiedEngineName, unifiedEngineName, "UNIFIED_ENGINE", "Data movement is executed by the single QMigration Unified Engine through native Connector SPI capabilities")
	}
	if task.Mode != domain.ModeFull {
		if e := validateCDCEngineSource(task.CDCEngine, *sourceDS, "forward"); e != nil {
			add(domain.CompatibilityUnsupported, "CDC", task.CDCEngine, string(sourceDS.Type), "CDC_ENGINE_SOURCE_MISMATCH", e.Error())
		} else {
			add(domain.CompatibilityCompatible, "CDC", task.CDCEngine, string(sourceDS.Type), "CDC_ENGINE_SOURCE_COMPATIBLE", "Forward CDC engine matches the source database family")
		}
		rollbackEngine := s.chooseRollbackCDCEngine(task, *targetDS)
		if rollbackEngine == "" {
			add(domain.CompatibilityWarning, "ROLLBACK_CDC", string(targetDS.Type), "", "ROLLBACK_CDC_UNCONFIGURED", "QMigration does not yet have a native reverse CDC reader for the post-cutover source database")
		} else if e := validateCDCEngineSource(rollbackEngine, *targetDS, "rollback"); e != nil {
			add(domain.CompatibilityUnsupported, "ROLLBACK_CDC", rollbackEngine, string(targetDS.Type), "ROLLBACK_CDC_ENGINE_SOURCE_MISMATCH", e.Error())
		} else {
			add(domain.CompatibilityCompatible, "ROLLBACK_CDC", rollbackEngine, string(targetDS.Type), "ROLLBACK_CDC_ENGINE_SELECTION", "Rollback CDC engine is compatible with the post-cutover source database")
		}
		if sourceDS.Type.IsMySQLFamily() {
			if strings.EqualFold(task.CDCDDLMode, "SAME_FAMILY") {
				sameFamily := sourceDS.Type.IsMySQLFamily() && targetDS.Type.IsMySQLFamily()
				if sameFamily {
					add(domain.CompatibilityWarning, "CDC_DDL", string(sourceDS.Type), string(targetDS.Type), "NATIVE_MYSQL_DDL_SAME_FAMILY", "Native MySQL CDC can replay DDL only when every migrated schema/table/column mapping is identity; runtime revalidates this before executing each DDL")
				} else {
					add(domain.CompatibilityUnsupported, "CDC_DDL", string(sourceDS.Type), string(targetDS.Type), "NATIVE_MYSQL_DDL_CROSS_FAMILY", "cdc_ddl_mode=SAME_FAMILY cannot translate MySQL DDL to a different database family; use coordinated schema change or wait for a QMigration heterogeneous DDL translator")
				}
			} else {
				add(domain.CompatibilityWarning, "CDC_DDL", string(sourceDS.Type), string(targetDS.Type), "NATIVE_MYSQL_DDL_FAIL_SAFE", "Native MySQL CDC rejects DDL by default and does not advance the checkpoint; enable SAME_FAMILY only for identity mappings")
			}
		}
		if sourceDS.Type.IsPostgreSQLFamily() {
			add(domain.CompatibilityWarning, "CDC_DDL", string(sourceDS.Type), string(targetDS.Type), "NATIVE_POSTGRES_DDL_LIMIT", "PostgreSQL pgoutput does not carry general DDL; schema changes must be coordinated separately during CDC")
		}
	}
	if task.Mode != domain.ModeIncremental {
		srcCapErr := s.connectors.Require(sourceDS.Type, connector.CapabilityFullRead)
		dstCapErr := s.connectors.Require(targetDS.Type, connector.CapabilityFullWrite)
		if srcCapErr != nil || dstCapErr != nil {
			add(domain.CompatibilityUnsupported, "CONNECTOR", string(sourceDS.Type), string(targetDS.Type), "NATIVE_FULL_CAPABILITY_REQUIRED", "QMigration unified full load requires source full-read and target full-write capabilities; protocol probes alone are not migration support")
			return out, nil
		}
	}

	src, err := s.connectors.New(*sourceDS)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	mappings, err := resolveMappings(ctx, src, *sourceDS, *targetDS, task.Tables)
	if err != nil {
		add(domain.CompatibilityUnsupported, "SCHEMA", defaultSchema(*sourceDS), defaultSchema(*targetDS), "DISCOVERY_FAILED", err.Error())
		return out, nil
	}
	dst, err := s.connectors.New(*targetDS)
	if err != nil {
		return nil, err
	}
	defer dst.Close()
	for _, m := range mappings {
		srcName := objectName(m.SourceSchema, m.SourceTable)
		dstName := objectName(m.TargetSchema, m.TargetTable)
		meta, e := src.GetTableMetadata(ctx, m.SourceSchema, m.SourceTable)
		if e != nil {
			add(domain.CompatibilityUnsupported, "TABLE", srcName, dstName, "SOURCE_METADATA", e.Error())
			continue
		}
		if sourceDS.Type.IsMySQLFamily() {
			for _, col := range meta.Columns {
				if strings.EqualFold(col.DataType, "json") {
					add(domain.CompatibilityWarning, "CDC_COLUMN", srcName+"."+col.Name, dstName, "NATIVE_MYSQL_BINARY_JSON", "Native MySQL CDC decodes standard MySQL binary JSON plus known OPAQUE DECIMAL/DATE/TIME/DATETIME/TIMESTAMP scalars; unknown MySQL-specific OPAQUE subtypes remain fail-safe and stop before checkpoint advancement")
				}
			}
		}
		if normalizeEngineName(task.FullEngine) == unifiedEngineName {
			keys, uniqueKey := stableMigrationKey(meta)
			if len(keys) == 0 {
				add(domain.CompatibilityUnsupported, "MIGRATION_KEY", srcName, dstName, "STABLE_KEY_REQUIRED", "Native resumable migration requires a primary key or a UNIQUE NOT NULL index")
			} else if uniqueKey != nil {
				add(domain.CompatibilityCompatible, "MIGRATION_KEY", srcName, dstName, "UNIQUE_NOT_NULL_KEYSET", fmt.Sprintf("No primary key; UNIQUE NOT NULL index %s on %v can be used as the durable Native keyset key", uniqueKey.Name, keys))
			} else if len(keys) == 1 && meta.PrimaryKeyNumeric {
				add(domain.CompatibilityCompatible, "PRIMARY_KEY", srcName+"."+meta.PrimaryKey, dstName, "INTEGER_PK_RANGE", "Signed integer primary key will use parallel range chunks")
			} else {
				add(domain.CompatibilityCompatible, "PRIMARY_KEY", srcName, dstName, "GENERIC_KEYSET_PK", fmt.Sprintf("Primary key %v will use resumable lexicographic keyset pagination with a durable JSON cursor", keys))
			}

		}
		cols := filterGenerated(meta.Columns)
		targetCols, _, mapErr := mapTargetColumns(cols, meta.PrimaryKey, m.Columns)
		if mapErr != nil {
			add(domain.CompatibilityUnsupported, "COLUMN_MAPPING", srcName, dstName, "COLUMN_MAPPING_INVALID", mapErr.Error())
			continue
		}
		if task.Mode != domain.ModeFull && strings.EqualFold(task.CDCConflictMode, "LAST_WRITE_WINS") {
			sourceConflictCol, targetConflictCol, conflictErr := conflictColumns(task, "forward", cols, targetCols)
			if conflictErr != nil {
				add(domain.CompatibilityUnsupported, "CDC_CONFLICT", srcName, dstName, "CONFLICT_COLUMN_INVALID", conflictErr.Error())
			} else {
				add(domain.CompatibilityCompatible, "CDC_CONFLICT", srcName+"."+sourceConflictCol.Name, dstName+"."+targetConflictCol.Name, "LAST_WRITE_WINS_COLUMN", "LAST_WRITE_WINS will lock the target row by mapped primary key and compare this version column before INSERT/UPDATE apply")
				add(domain.CompatibilityWarning, "CDC_CONFLICT", srcName, dstName, "LAST_WRITE_WINS_DELETE_SOURCE_WINS", "DELETE events remain idempotent source-wins because a deleted row has no durable version/tombstone to compare")
			}
		}
		dstMeta, e := dst.GetTableMetadata(ctx, m.TargetSchema, m.TargetTable)
		if e != nil || dstMeta == nil || len(dstMeta.Columns) == 0 {
			if task.AutoCreateTable {
				if _, ok := dst.(connector.SchemaConnector); ok {
					add(domain.CompatibilityCompatible, "TABLE", srcName, dstName, "AUTO_CREATE", "Target table is absent and can be created automatically")
					assessDeferredIndexesAndForeignKeys(add, meta, m, srcName, dstName)
				} else {
					add(domain.CompatibilityUnsupported, "TABLE", srcName, dstName, "AUTO_CREATE_UNSUPPORTED", "Target connector cannot auto-create this table")
				}
			} else {
				add(domain.CompatibilityUnsupported, "TABLE", srcName, dstName, "TARGET_TABLE_MISSING", "Target table is missing and automatic table creation is disabled")
			}
			continue
		}
		if e := validateTargetColumns(targetCols, dstMeta); e != nil {
			add(domain.CompatibilityUnsupported, "COLUMN", srcName, dstName, "TARGET_COLUMNS", e.Error())
		} else {
			add(domain.CompatibilityCompatible, "TABLE", srcName, dstName, "TABLE_COMPATIBLE", fmt.Sprintf("%d source columns can be mapped to the target table", len(targetCols)))
		}
		assessExistingIndexesAndForeignKeys(add, meta, dstMeta, m, srcName, dstName)
	}

	// Non-table objects are assessed independently from row migration. They are
	// warnings by default because data migration can still proceed safely while
	// routines/triggers/views are converted in a separate schema phase.
	if objectSource, ok := src.(connector.SchemaObjectConnector); ok {
		seenSchemas := map[string]bool{}
		for _, m := range mappings {
			if seenSchemas[m.SourceSchema] {
				continue
			}
			seenSchemas[m.SourceSchema] = true
			objects, e := objectSource.ListSchemaObjects(ctx, m.SourceSchema)
			if e != nil {
				add(domain.CompatibilityWarning, "SCHEMA_OBJECT", m.SourceSchema, "", "SCHEMA_OBJECT_DISCOVERY_FAILED", e.Error())
				continue
			}
			for _, obj := range objects {
				sourceObject := objectName(obj.Schema, obj.Name)
				switch obj.Type {
				case domain.SchemaObjectSequence:
					if !obj.BindingKnown {
						add(domain.CompatibilityWarning, "SEQUENCE", sourceObject, "", "SEQUENCE_BINDING_UNKNOWN", "PostgreSQL sequence ownership/identity metadata could not be determined; automatic object apply will remain manual")
					} else if targetDS.Type.IsPostgreSQLFamily() && strings.EqualFold(obj.Definition, "OWNED") {
						targetSchema, ok := schemaObjectTargetSchema(obj, mappings, *sourceDS, *targetDS)
						targetRelated, relatedOK := schemaObjectTargetRelatedTo(obj, mappings, targetSchema)
						if ok && relatedOK && targetRelated != "" {
							add(domain.CompatibilityCompatible, "SEQUENCE", sourceObject, objectName(targetSchema, targetRelated), "SEQUENCE_SERIAL_BINDING", "SERIAL backing sequence can restore OWNED BY/default binding and synchronize last_value/is_called")
						} else {
							add(domain.CompatibilityWarning, "SEQUENCE", sourceObject, "", "SEQUENCE_BINDING_MAPPING_REQUIRED", "SERIAL sequence ownership cannot be mapped unambiguously to the target table/column")
						}
					} else if targetDS.Type.IsPostgreSQLFamily() && strings.EqualFold(obj.Definition, "IDENTITY") {
						add(domain.CompatibilityWarning, "SEQUENCE", sourceObject, obj.RelatedTo, "SEQUENCE_IDENTITY_PRESERVE", "IDENTITY semantics must already exist on the target; QMigration only synchronizes the backing sequence state and will not convert IDENTITY into a plain sequence")
					} else if targetDS.Type.IsPostgreSQLFamily() {
						add(domain.CompatibilityCompatible, "SEQUENCE", sourceObject, "", "SEQUENCE_NATIVE", "Standalone sequence can be created natively and its last_value/is_called synchronized after full load")
					} else {
						add(domain.CompatibilityWarning, "SEQUENCE", sourceObject, "", "SEQUENCE_CONVERSION_REQUIRED", "Target does not expose PostgreSQL sequence semantics directly; convert to AUTO_INCREMENT/identity or application-managed numbering")
					}
				case domain.SchemaObjectView:
					add(domain.CompatibilityWarning, "VIEW", sourceObject, "", "VIEW_REVIEW_REQUIRED", "View definition was discovered and should be translated/validated after base tables are created")
				case domain.SchemaObjectTrigger:
					add(domain.CompatibilityWarning, "TRIGGER", sourceObject, obj.RelatedTo, "TRIGGER_REVIEW_REQUIRED", "Trigger should normally remain disabled during full load and be recreated only after semantic review")
				case domain.SchemaObjectFunction, domain.SchemaObjectProcedure:
					add(domain.CompatibilityWarning, string(obj.Type), sourceObject, "", "ROUTINE_MANUAL_CONVERSION", "Stored routine DDL was discovered but cross-database procedural SQL conversion requires semantic review")
				}
			}
		}
	}
	return out, nil
}

func sameNativeDatabaseFamily(a, b domain.DataSourceType) bool {
	return (a.IsMySQLFamily() && b.IsMySQLFamily()) || (a.IsPostgreSQLFamily() && b.IsPostgreSQLFamily()) || (a == domain.DataSourceOracle && b == domain.DataSourceOracle) || (a == domain.DataSourceSQLServer && b == domain.DataSourceSQLServer)
}

func schemaObjectIdentityMappings(mappings []domain.TableMapping) bool {
	for _, m := range mappings {
		if !strings.EqualFold(m.SourceSchema, m.TargetSchema) || !strings.EqualFold(m.SourceTable, m.TargetTable) {
			return false
		}
		for _, c := range m.Columns {
			target := strings.TrimSpace(c.TargetColumn)
			if target == "" {
				target = c.SourceColumn
			}
			if !strings.EqualFold(c.SourceColumn, target) {
				return false
			}
		}
	}
	return true
}

func schemaObjectTargetSchema(obj domain.SchemaObject, mappings []domain.TableMapping, sourceDS, targetDS domain.DataSource) (string, bool) {
	// Triggers belong to a concrete table, so use that table's mapping first.
	if obj.RelatedTo != "" {
		for _, m := range mappings {
			if strings.EqualFold(m.SourceSchema, obj.Schema) && strings.EqualFold(m.SourceTable, obj.RelatedTo) {
				return m.TargetSchema, true
			}
		}
	}
	candidates := map[string]string{}
	for _, m := range mappings {
		if strings.EqualFold(m.SourceSchema, obj.Schema) && m.TargetSchema != "" {
			candidates[strings.ToLower(m.TargetSchema)] = m.TargetSchema
		}
	}
	if len(candidates) == 1 {
		for _, v := range candidates {
			return v, true
		}
	}
	if len(candidates) > 1 {
		return "", false
	}
	if strings.EqualFold(obj.Schema, defaultSchema(sourceDS)) {
		target := defaultSchema(targetDS)
		if target != "" {
			return target, true
		}
	}
	return "", false
}

func schemaObjectTargetRelatedTo(obj domain.SchemaObject, mappings []domain.TableMapping, targetSchema string) (string, bool) {
	if strings.TrimSpace(obj.RelatedTo) == "" {
		return "", true
	}
	parts := strings.SplitN(strings.TrimSpace(obj.RelatedTo), ".", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	sourceTable, sourceColumn := parts[0], parts[1]
	for _, m := range mappings {
		if !strings.EqualFold(m.SourceSchema, obj.Schema) || !strings.EqualFold(m.SourceTable, sourceTable) {
			continue
		}
		mappedSchema := strings.TrimSpace(m.TargetSchema)
		if mappedSchema == "" {
			mappedSchema = targetSchema
		}
		if !strings.EqualFold(mappedSchema, targetSchema) {
			return "", false
		}
		targetTable := strings.TrimSpace(m.TargetTable)
		if targetTable == "" {
			targetTable = sourceTable
		}
		targetColumn := sourceColumn
		for _, c := range m.Columns {
			if strings.EqualFold(c.SourceColumn, sourceColumn) {
				if strings.TrimSpace(c.TargetColumn) != "" {
					targetColumn = c.TargetColumn
				}
				break
			}
		}
		return targetTable + "." + targetColumn, true
	}
	return "", false
}

func splitSchemaObjectRelatedTo(value string) (table, column string, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(value), ".", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func schemaObjectKey(typ domain.SchemaObjectType, name string) string {
	return strings.ToUpper(string(typ)) + "\x00" + strings.ToLower(name)
}

func normalizedSchemaObjectDefinition(value string) string {
	value = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), ";"))
	return strings.Join(strings.Fields(value), " ")
}

func equivalentViewDefinition(source, target domain.SchemaObject) bool {
	a := normalizedSchemaObjectDefinition(source.Definition)
	b := normalizedSchemaObjectDefinition(target.Definition)
	return a != "" && b != "" && a == b
}

func containsSchemaObjectType(types map[domain.SchemaObjectType]bool, typ domain.SchemaObjectType) bool {
	return len(types) == 0 || types[typ]
}

func finalizeSchemaObjectViewDependencies(plan *domain.SchemaObjectPlan) {
	if plan == nil || len(plan.Items) == 0 {
		return
	}
	viewIndex := map[string]int{}
	for i, item := range plan.Items {
		if item.Source.Type == domain.SchemaObjectView && item.Action == domain.SchemaObjectApplySafe {
			viewIndex[strings.ToLower(objectName(item.Source.Schema, item.Source.Name))] = i
		}
	}
	if len(viewIndex) == 0 {
		return
	}
	indegree := map[int]int{}
	edges := map[int][]int{}
	for _, idx := range viewIndex {
		indegree[idx] = 0
	}
	for _, idx := range viewIndex {
		for _, dep := range plan.Items[idx].Source.Dependencies {
			depIdx, ok := viewIndex[strings.ToLower(strings.TrimSpace(dep))]
			if !ok || depIdx == idx {
				if ok && depIdx == idx {
					indegree[idx]++
				}
				continue
			}
			edges[depIdx] = append(edges[depIdx], idx)
			indegree[idx]++
		}
	}
	queue := []int{}
	for idx, degree := range indegree {
		if degree == 0 {
			queue = append(queue, idx)
		}
	}
	sort.Slice(queue, func(i, j int) bool {
		a, b := plan.Items[queue[i]].Source, plan.Items[queue[j]].Source
		return strings.ToLower(objectName(a.Schema, a.Name)) < strings.ToLower(objectName(b.Schema, b.Name))
	})
	orderedViews := []int{}
	for len(queue) > 0 {
		idx := queue[0]
		queue = queue[1:]
		orderedViews = append(orderedViews, idx)
		for _, next := range edges[idx] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
				sort.Slice(queue, func(i, j int) bool {
					a, b := plan.Items[queue[i]].Source, plan.Items[queue[j]].Source
					return strings.ToLower(objectName(a.Schema, a.Name)) < strings.ToLower(objectName(b.Schema, b.Name))
				})
			}
		}
	}
	processed := map[int]bool{}
	for _, idx := range orderedViews {
		processed[idx] = true
	}
	for idx := range indegree {
		if processed[idx] {
			continue
		}
		plan.Items[idx].Action = domain.SchemaObjectManual
		plan.Items[idx].Reason = "view dependency cycle detected; create the involved views manually in a coordinated order"
		plan.SafeActions--
		plan.Manual++
	}

	// Apply sequences before views, then views in dependency order. Keep all
	// skipped/manual items afterwards so the console remains deterministic.
	reordered := make([]domain.SchemaObjectPlanItem, 0, len(plan.Items))
	used := map[int]bool{}
	for i, item := range plan.Items {
		if item.Action == domain.SchemaObjectSyncSequence {
			reordered = append(reordered, item)
			used[i] = true
		}
	}
	for _, idx := range orderedViews {
		if !processed[idx] || plan.Items[idx].Action != domain.SchemaObjectApplySafe {
			continue
		}
		reordered = append(reordered, plan.Items[idx])
		used[idx] = true
	}
	for i, item := range plan.Items {
		if !used[i] {
			reordered = append(reordered, item)
		}
	}
	plan.Items = reordered
}

func dstDDLAvailable(c connector.Connector) bool {
	if c == nil {
		return false
	}
	_, ok := c.(connector.DDLApplyConnector)
	return ok
}

// PlanSchemaObjects classifies non-table schema objects into actions that are
// safe to execute automatically versus objects that need semantic review.
// It is deliberately conservative: no heterogeneous procedural SQL rewriting
// and no automatic schema rename inside object definitions.
func (s *Service) PlanSchemaObjects(ctx context.Context, id string) (*domain.SchemaObjectPlan, error) {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	sourceDS, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, err
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, err
	}
	src, err := s.connectors.New(*sourceDS)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	sourceObjects, ok := src.(connector.SchemaObjectConnector)
	if !ok {
		return nil, errors.New("source connector does not support schema object discovery")
	}
	mappings, err := resolveMappings(ctx, src, *sourceDS, *targetDS, task.Tables)
	if err != nil {
		return nil, err
	}

	var dst connector.Connector
	var targetObjects connector.SchemaObjectConnector
	if s.connectors.Require(targetDS.Type, connector.CapabilitySchemaObjects) == nil {
		dst, err = s.connectors.New(*targetDS)
		if err != nil {
			return nil, err
		}
		defer dst.Close()
		targetObjects, _ = dst.(connector.SchemaObjectConnector)
	}

	plan := &domain.SchemaObjectPlan{TaskID: id, PlannedAt: time.Now()}
	translator, translatorErr := schematranslate.FromEnv()
	if translatorErr != nil {
		return nil, translatorErr
	}
	identity := sameNativeDatabaseFamily(sourceDS.Type, targetDS.Type) && schemaObjectIdentityMappings(mappings)
	seenSchemas := map[string]bool{}
	sourceSchemas := []string{}
	base := defaultSchema(*sourceDS)
	if base != "" {
		seenSchemas[strings.ToLower(base)] = true
		sourceSchemas = append(sourceSchemas, base)
	}
	for _, m := range mappings {
		key := strings.ToLower(m.SourceSchema)
		if m.SourceSchema != "" && !seenSchemas[key] {
			seenSchemas[key] = true
			sourceSchemas = append(sourceSchemas, m.SourceSchema)
		}
	}

	targetCatalog := map[string]map[string]domain.SchemaObject{}
	loadTargetCatalog := func(schema string) map[string]domain.SchemaObject {
		key := strings.ToLower(schema)
		if existing, ok := targetCatalog[key]; ok {
			return existing
		}
		catalog := map[string]domain.SchemaObject{}
		targetCatalog[key] = catalog
		if dst == nil || targetObjects == nil || schema == "" {
			return catalog
		}
		schemas, e := dst.ListSchemas(ctx)
		if e != nil {
			return catalog
		}
		exists := false
		for _, item := range schemas {
			if strings.EqualFold(item.Name, schema) {
				exists = true
				break
			}
		}
		if !exists {
			return catalog
		}
		items, e := targetObjects.ListSchemaObjects(ctx, schema)
		if e != nil {
			return catalog
		}
		for _, item := range items {
			catalog[schemaObjectKey(item.Type, item.Name)] = item
		}
		return catalog
	}

	for _, schema := range sourceSchemas {
		objects, e := sourceObjects.ListSchemaObjects(ctx, schema)
		if e != nil {
			return nil, fmt.Errorf("discover schema objects in %s: %w", schema, e)
		}
		for _, obj := range objects {
			item := domain.SchemaObjectPlanItem{Source: obj, TargetName: obj.Name, Action: domain.SchemaObjectManual}
			targetSchema, unambiguous := schemaObjectTargetSchema(obj, mappings, *sourceDS, *targetDS)
			item.TargetSchema = targetSchema
			if !unambiguous || targetSchema == "" {
				item.Reason = "source schema maps to multiple target schemas; select/convert this object manually"
				plan.Manual++
				plan.Items = append(plan.Items, item)
				continue
			}
			targetObject, targetExists := loadTargetCatalog(targetSchema)[schemaObjectKey(obj.Type, obj.Name)]
			item.TargetExists = targetExists
			switch obj.Type {
			case domain.SchemaObjectSequence:
				_, sourceSeq := src.(connector.SequenceStateConnector)
				_, targetSeq := dst.(connector.SequenceStateConnector)
				_, targetDDL := dst.(connector.DDLApplyConnector)
				_, targetBinding := dst.(connector.SequenceBindingConnector)
				baseSafe := sourceDS.Type.IsPostgreSQLFamily() && targetDS.Type.IsPostgreSQLFamily() && sourceSeq && targetSeq && targetDDL && obj.Schema == targetSchema && strings.TrimSpace(obj.DDL) != ""
				if !baseSafe {
					item.Reason = "automatic sequence migration is limited to PostgreSQL-family targets with identical schema names"
					plan.Manual++
					break
				}
				if !obj.BindingKnown {
					item.Reason = "sequence ownership/identity metadata is unavailable; automatic migration is disabled to avoid changing SERIAL/IDENTITY semantics"
					plan.Manual++
					break
				}
				if strings.EqualFold(obj.Definition, "IDENTITY") && !item.TargetExists {
					item.Reason = "identity-backed sequence has no matching target object; preserve PostgreSQL IDENTITY semantics manually instead of converting it to a plain sequence"
					plan.Manual++
					break
				}
				if strings.EqualFold(obj.Definition, "IDENTITY") && !strings.EqualFold(targetObject.Definition, "IDENTITY") {
					item.Reason = "source sequence backs an IDENTITY column but the same-name target sequence is not identity-backed; automatic synchronization would change semantics"
					plan.Manual++
					break
				}
				if strings.EqualFold(obj.Definition, "OWNED") {
					item.TargetRelatedTo, unambiguous = schemaObjectTargetRelatedTo(obj, mappings, targetSchema)
					if !unambiguous || item.TargetRelatedTo == "" {
						item.Reason = "SERIAL sequence ownership cannot be mapped unambiguously to a target table/column"
						plan.Manual++
						break
					}
					if !targetBinding {
						item.Reason = "target connector cannot restore PostgreSQL sequence OWNED BY/default binding"
						plan.Manual++
						break
					}
				}
				item.Action = domain.SchemaObjectSyncSequence
				switch {
				case strings.EqualFold(obj.Definition, "OWNED") && item.TargetExists:
					item.Reason = "target SERIAL sequence exists; restore OWNED BY/default binding and synchronize last_value/is_called"
				case strings.EqualFold(obj.Definition, "OWNED"):
					item.Reason = "create SERIAL backing sequence, restore OWNED BY/default binding, and synchronize last_value/is_called"
				case strings.EqualFold(obj.Definition, "IDENTITY"):
					item.Reason = "matching target IDENTITY sequence exists; synchronize last_value/is_called without changing identity semantics"
				case item.TargetExists:
					item.Reason = "target standalone sequence exists; synchronize last_value/is_called after full load"
				default:
					item.Reason = "create the standalone PostgreSQL sequence and synchronize last_value/is_called"
				}
				plan.SafeActions++
			case domain.SchemaObjectView:
				if item.TargetExists {
					if identity && equivalentViewDefinition(obj, targetObject) {
						item.Action = domain.SchemaObjectSkipExisting
						item.Reason = "target view already exists with an equivalent discovered definition"
						plan.Skipped++
					} else {
						item.Reason = "target view already exists but its definition is different or cannot be proven equivalent; QMigration will not overwrite it automatically"
						plan.Manual++
					}
				} else if !obj.DependenciesKnown {
					item.Reason = "view dependency metadata is unavailable; automatic creation is disabled to avoid applying dependent views out of order"
					plan.Manual++
				} else if identity && obj.Schema == targetSchema && strings.TrimSpace(obj.DDL) != "" {
					if _, ok := dst.(connector.DDLApplyConnector); ok {
						item.Action = domain.SchemaObjectApplySafe
						item.Reason = "same-family identity mapping; discovered view DDL can be applied safely after base tables exist"
						plan.SafeActions++
					} else {
						item.Reason = "target connector cannot execute DDL"
						plan.Manual++
					}
				} else {
					item.Reason = "view requires manual conversion because database family/schema/table/column mappings are not identity-safe"
					plan.Manual++
				}
			case domain.SchemaObjectTrigger, domain.SchemaObjectFunction, domain.SchemaObjectProcedure:
				manualReason := "stored procedural object requires semantic and side-effect review"
				if translator != nil && dstDDLAvailable(dst) && strings.TrimSpace(obj.DDL) != "" {
					tr, e := translator.Translate(ctx, schematranslate.Request{SourceFamily: string(sourceDS.Type), TargetFamily: string(targetDS.Type), ObjectType: string(obj.Type), SourceSchema: obj.Schema, TargetSchema: targetSchema, Name: obj.Name, SourceDDL: obj.DDL})
					if e == nil {
						item.Action = domain.SchemaObjectApplySafe
						item.TargetDDL = tr.TargetDDL
						item.TranslationEvidenceSHA256 = tr.TargetDDLSHA256
						item.Reason = "external translation provider attested semantic/side-effect review and deterministic target DDL"
						plan.SafeActions++
					} else {
						item.Reason = manualReason + ": " + e.Error()
						plan.Manual++
					}
				} else {
					item.Reason = manualReason
					plan.Manual++
				}
			default:
				item.Reason = "schema object type is not eligible for automatic execution"
				plan.Manual++
			}
			plan.Items = append(plan.Items, item)
		}
	}
	finalizeSchemaObjectViewDependencies(plan)
	return plan, nil
}

// ApplySchemaObjects executes only actions classified safe by PlanSchemaObjects.
// Manual objects are never executed by this endpoint. The operation is explicit
// and confirm-gated because DDL changes target schema state outside row migration.
func (s *Service) ApplySchemaObjects(ctx context.Context, id string, req domain.SchemaObjectApplyRequest) (*domain.SchemaObjectApplyResult, error) {
	if !req.Confirm {
		return nil, errors.New("confirm=true is required to apply schema objects")
	}
	plan, err := s.PlanSchemaObjects(ctx, id)
	if err != nil {
		return nil, err
	}
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	if task.Mode == domain.ModeIncremental {
		return nil, errors.New("schema object apply is not available for incremental-only tasks")
	}
	switch task.Status {
	case domain.StatusFullFinished, domain.StatusValidating, domain.StatusCDCCatchingUp, domain.StatusReadyCutover, domain.StatusCutoverRunning, domain.StatusFinished:
	default:
		return nil, fmt.Errorf("schema objects can only be applied after base-table full load completes; current status=%s", task.Status)
	}
	sourceDS, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, err
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, err
	}
	src, err := s.connectors.New(*sourceDS)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	dst, err := s.connectors.New(*targetDS)
	if err != nil {
		return nil, err
	}
	defer dst.Close()
	dstDDL, ddlOK := dst.(connector.DDLApplyConnector)
	srcSeq, srcSeqOK := src.(connector.SequenceStateConnector)
	dstSeq, dstSeqOK := dst.(connector.SequenceStateConnector)
	dstBinding, dstBindingOK := dst.(connector.SequenceBindingConnector)
	dstSchema, _ := dst.(connector.SchemaConnector)

	selected := map[domain.SchemaObjectType]bool{}
	for _, typ := range req.Types {
		selected[typ] = true
	}
	result := &domain.SchemaObjectApplyResult{TaskID: id}
	sequenceApplied := false
	sequenceFailed := false
	for _, item := range plan.Items {
		if !containsSchemaObjectType(selected, item.Source.Type) {
			continue
		}
		entry := domain.SchemaObjectApplyItemResult{Type: item.Source.Type, SourceSchema: item.Source.Schema, Name: item.Source.Name, TargetSchema: item.TargetSchema}
		switch item.Action {
		case domain.SchemaObjectSkipExisting, domain.SchemaObjectManual:
			entry.Status = "SKIPPED"
			entry.Message = item.Reason
			result.Skipped++
		case domain.SchemaObjectApplySafe:
			if !ddlOK {
				entry.Status = "FAILED"
				entry.Message = "target connector cannot execute DDL"
				result.Failed++
				break
			}
			if dstSchema != nil {
				if e := dstSchema.EnsureSchema(ctx, item.TargetSchema); e != nil {
					entry.Status = "FAILED"
					entry.Message = e.Error()
					result.Failed++
					break
				}
			}
			ddl := item.TargetDDL
			if strings.TrimSpace(ddl) == "" {
				ddl = item.Source.DDL
			}
			if e := dstDDL.ExecDDL(ctx, item.TargetSchema, ddl); e != nil {
				entry.Status = "FAILED"
				entry.Message = e.Error()
				result.Failed++
			} else {
				entry.Status = "APPLIED"
				result.Applied++
			}
		case domain.SchemaObjectSyncSequence:
			if !ddlOK || !srcSeqOK || !dstSeqOK {
				entry.Status = "FAILED"
				entry.Message = "sequence synchronization connectors are unavailable"
				result.Failed++
				sequenceFailed = true
				break
			}
			if dstSchema != nil {
				if e := dstSchema.EnsureSchema(ctx, item.TargetSchema); e != nil {
					entry.Status = "FAILED"
					entry.Message = e.Error()
					result.Failed++
					sequenceFailed = true
					break
				}
			}
			if !item.TargetExists {
				if e := dstDDL.ExecDDL(ctx, item.TargetSchema, item.Source.DDL); e != nil {
					entry.Status = "FAILED"
					entry.Message = "create sequence: " + e.Error()
					result.Failed++
					sequenceFailed = true
					break
				}
			}
			if strings.EqualFold(item.Source.Definition, "OWNED") {
				if !dstBindingOK {
					entry.Status = "FAILED"
					entry.Message = "target connector cannot restore sequence binding"
					result.Failed++
					sequenceFailed = true
					break
				}
				table, column, ok := splitSchemaObjectRelatedTo(item.TargetRelatedTo)
				if !ok {
					entry.Status = "FAILED"
					entry.Message = "invalid target sequence binding " + item.TargetRelatedTo
					result.Failed++
					sequenceFailed = true
					break
				}
				if e := dstBinding.BindSequence(ctx, item.TargetSchema, item.TargetName, table, column); e != nil {
					entry.Status = "FAILED"
					entry.Message = "restore sequence binding: " + e.Error()
					result.Failed++
					sequenceFailed = true
					break
				}
			}
			value, called, e := srcSeq.GetSequenceState(ctx, item.Source.Schema, item.Source.Name)
			if e != nil {
				entry.Status = "FAILED"
				entry.Message = "read source sequence state: " + e.Error()
				result.Failed++
				sequenceFailed = true
				break
			}
			if e = dstSeq.SetSequenceState(ctx, item.TargetSchema, item.TargetName, value, called); e != nil {
				entry.Status = "FAILED"
				entry.Message = "set target sequence state: " + e.Error()
				result.Failed++
				sequenceFailed = true
				break
			}
			entry.Status = "APPLIED"
			entry.Message = "sequence state synchronized to last_value=" + value
			result.Applied++
			sequenceApplied = true
		default:
			entry.Status = "SKIPPED"
			entry.Message = "unsupported plan action"
			result.Skipped++
		}
		result.Items = append(result.Items, entry)
	}
	if sequenceApplied && !sequenceFailed {
		task.SequenceSyncedAt = time.Now()
		task.UpdatedAt = time.Now()
		if err := s.repo.UpdateMigration(ctx, task); err != nil {
			return result, err
		}
	}
	return result, nil
}

func mappedColumnsForAssessment(columns []string, mappings []domain.ColumnMapping) []string {
	renames := make(map[string]string, len(mappings))
	for _, mapping := range mappings {
		target := strings.TrimSpace(mapping.TargetColumn)
		if target == "" {
			target = mapping.SourceColumn
		}
		renames[strings.ToLower(mapping.SourceColumn)] = target
	}
	out := make([]string, 0, len(columns))
	for _, col := range columns {
		if target, ok := renames[strings.ToLower(col)]; ok {
			out = append(out, target)
		} else {
			out = append(out, col)
		}
	}
	return out
}

func sameColumnsFold(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func assessDeferredIndexesAndForeignKeys(add func(domain.CompatibilityLevel, string, string, string, string, string), meta *domain.TableMetadata, mapping domain.TableMapping, srcName, dstName string) {
	for _, idx := range meta.Indexes {
		if idx.Primary {
			continue
		}
		targetCols := mappedColumnsForAssessment(idx.Columns, mapping.Columns)
		kind := "INDEX"
		if idx.Unique {
			kind = "UNIQUE_INDEX"
		}
		add(domain.CompatibilityWarning, kind, srcName+"."+idx.Name, dstName, "INDEX_DEFERRED", fmt.Sprintf("Secondary index on columns %v is not created by the current base-table auto-create path; create it after full load to reduce write amplification", targetCols))
	}
	for _, fk := range meta.ForeignKeys {
		cols := mappedColumnsForAssessment(fk.Columns, mapping.Columns)
		add(domain.CompatibilityWarning, "FOREIGN_KEY", srcName+"."+fk.Name, dstName, "FOREIGN_KEY_DEFERRED", fmt.Sprintf("Foreign key on columns %v referencing %s.%s should be recreated after full load and validation", cols, fk.ReferencedSchema, fk.ReferencedTable))
	}
}

func assessExistingIndexesAndForeignKeys(add func(domain.CompatibilityLevel, string, string, string, string, string), source, target *domain.TableMetadata, mapping domain.TableMapping, srcName, dstName string) {
	for _, idx := range source.Indexes {
		if idx.Primary {
			continue
		}
		mapped := mappedColumnsForAssessment(idx.Columns, mapping.Columns)
		found := false
		for _, targetIdx := range target.Indexes {
			if targetIdx.Primary {
				continue
			}
			if idx.Unique == targetIdx.Unique && sameColumnsFold(mapped, targetIdx.Columns) {
				found = true
				break
			}
		}
		kind := "INDEX"
		if idx.Unique {
			kind = "UNIQUE_INDEX"
		}
		if found {
			add(domain.CompatibilityCompatible, kind, srcName+"."+idx.Name, dstName, "INDEX_PRESENT", fmt.Sprintf("Target has a compatible index for mapped columns %v", mapped))
		} else {
			add(domain.CompatibilityWarning, kind, srcName+"."+idx.Name, dstName, "INDEX_MISSING", fmt.Sprintf("Target is missing a compatible index for mapped columns %v; create it after full load", mapped))
		}
	}
	for _, fk := range source.ForeignKeys {
		mapped := mappedColumnsForAssessment(fk.Columns, mapping.Columns)
		found := false
		for _, targetFK := range target.ForeignKeys {
			if sameColumnsFold(mapped, targetFK.Columns) && strings.EqualFold(fk.ReferencedTable, targetFK.ReferencedTable) {
				found = true
				break
			}
		}
		if found {
			add(domain.CompatibilityCompatible, "FOREIGN_KEY", srcName+"."+fk.Name, dstName, "FOREIGN_KEY_PRESENT", fmt.Sprintf("Target has a foreign key on mapped columns %v", mapped))
		} else {
			add(domain.CompatibilityWarning, "FOREIGN_KEY", srcName+"."+fk.Name, dstName, "FOREIGN_KEY_MISSING", fmt.Sprintf("Target is missing the source foreign key on mapped columns %v; recreate it after full load and validation", mapped))
		}
	}
}

func (s *Service) ValidationResults(ctx context.Context, id string) ([]domain.ValidationResult, error) {
	return s.repo.ListValidationResults(ctx, id)
}

func (s *Service) ValidationArchive(ctx context.Context, id string) (*domain.ValidationArchive, error) {
	p, ok := s.repo.(repository.ValidationArchiveProvider)
	if !ok {
		return nil, errors.New("validation archive is not supported by this repository")
	}
	return p.GetValidationArchive(ctx, id)
}

func (s *Service) EnsureValidationArchive(ctx context.Context, id string) (*domain.ValidationArchive, bool, error) {
	return repository.EnsureValidationArchive(ctx, s.repo, id, int(envInt64("QMIGRATION_VALIDATION_ARCHIVE_PAGE_SIZE", 512)))
}

func (s *Service) ValidationReportArchive(ctx context.Context, taskID, evidenceDigest string) (*domain.ValidationReportArchiveRecord, error) {
	p, ok := s.repo.(repository.ValidationReportArchiveProvider)
	if !ok {
		return nil, errors.New("validation report archive registry is not supported by this repository")
	}
	return p.GetValidationReportArchive(ctx, taskID, evidenceDigest)
}

func (s *Service) RecordValidationReportArchive(ctx context.Context, record *domain.ValidationReportArchiveRecord) (bool, error) {
	p, ok := s.repo.(repository.ValidationReportArchiveProvider)
	if !ok {
		return false, errors.New("validation report archive registry is not supported by this repository")
	}
	return p.CreateValidationReportArchive(ctx, record)
}

func (s *Service) archiveValidationBestEffort(ctx context.Context, taskID string) {
	if taskID == "" {
		return
	}
	if _, _, err := repository.EnsureValidationArchive(ctx, s.repo, taskID, int(envInt64("QMIGRATION_VALIDATION_ARCHIVE_PAGE_SIZE", 512))); err != nil && !errors.Is(err, repository.ErrNoValidationEvidence) && !errors.Is(err, repository.ErrValidationArchiveNotTerminal) {
		s.logTask(ctx, taskID, "", "", "", "WARN", "validation archive creation failed: "+err.Error())
	}
}
func (s *Service) ValidateNow(ctx context.Context, id string) error {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if m.Status != domain.StatusFullFinished && m.Status != domain.StatusFinished && m.Status != domain.StatusValidating && m.Status != domain.StatusCDCCatchingUp {
		return fmt.Errorf("task cannot validate from %s", m.Status)
	}

	// Full+CDC validation must freeze target apply atomically with barrier capture.
	// ApplyEngineJobCDCEvents uses the same lock, so no target transaction can
	// advance between the durable checkpoint we capture and the VALIDATING state
	// that redirects new source transactions into the durable spool.
	var unlock func()
	if m.Mode != domain.ModeFull && m.Status != domain.StatusValidating {
		lock := s.cdcSpoolLock(id, "forward")
		lock.Lock()
		unlock = lock.Unlock
		defer unlock()
		m, err = s.repo.GetMigration(ctx, id)
		if err != nil {
			return err
		}
	}
	if m.Status == domain.StatusCDCCatchingUp {
		stats, statErr := s.repo.CDCSpoolStats(ctx, id, "forward")
		if statErr != nil {
			return statErr
		}
		if stats.PendingTransactions > 0 {
			return fmt.Errorf("task cannot validate while CDC spool has %d pending transactions", stats.PendingTransactions)
		}
	}
	if m.Status != domain.StatusValidating {
		if err := s.captureValidationBarrier(ctx, m); err != nil {
			return err
		}
		if err := Transition(m, domain.StatusValidating); err != nil {
			return err
		}
		m.UpdatedAt = time.Now()
		if err := s.repo.UpdateMigration(ctx, m); err != nil {
			return err
		}
	}
	go s.validateTask(id)
	return nil
}

func validationPageSize() int {
	v := int(envInt64("QMIGRATION_VALIDATION_CHUNK_PAGE_SIZE", 512))
	if v < 16 {
		v = 16
	}
	if v > 5000 {
		v = 5000
	}
	return v
}

func complexValidationReadRequest(tbl domain.MigrationTable, piece domain.MigrationChunk, primaryKeys []string) connector.ReadBatchRequest {
	req := connector.ReadBatchRequest{Schema: tbl.SourceSchema, Table: tbl.SourceTable, PrimaryKey: primaryKeys[0], PrimaryKeys: primaryKeys, StartPK: piece.Start, EndPK: piece.End, Partition: piece.PartitionName, HashBucket: piece.HashBucket, HashBuckets: piece.HashBuckets, CustomWhere: piece.CustomWhere}
	if strings.Contains(piece.SplitType, "KEYSET") {
		req.UseKeyset = true
		req.LowerBound, _ = decodeKeysetBound(piece.StartCursorJSON)
		req.UpperBound, _ = decodeKeysetBound(piece.EndCursorJSON)
	}
	return req
}

func streamComplexValidationSourceChecksum(ctx context.Context, repo repository.Repository, taskID string, src connector.DataConnector, tbl domain.MigrationTable, primaryKeys []string, batchRows, pageSize int) (validationpkg.Checksum, error) {
	acc := validationpkg.NewUnorderedAccumulator()
	afterNo, afterID := -1, ""
	for {
		page, err := repository.ListTableChunksPage(ctx, repo, taskID, tbl.ID, afterNo, afterID, pageSize)
		if err != nil {
			return validationpkg.Checksum{}, err
		}
		if len(page) == 0 {
			break
		}
		for _, piece := range page {
			if err := acc.AddRequest(ctx, src, complexValidationReadRequest(tbl, piece, primaryKeys), tbl.Columns, tbl.Columns, batchRows); err != nil {
				return validationpkg.Checksum{}, err
			}
		}
		last := page[len(page)-1]
		afterNo, afterID = last.ChunkNo, last.ID
		if len(page) < pageSize {
			break
		}
	}
	return acc.Checksum(), nil
}

func (s *Service) validateTask(id string) {
	ctx := context.Background()
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return
	}
	srcDS, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		s.failTask(id, err)
		return
	}
	dstDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		s.failTask(id, err)
		return
	}
	sr, err := s.connectors.New(*srcDS)
	if err != nil {
		s.failTask(id, err)
		return
	}
	defer sr.Close()
	dr, err := s.connectors.New(*dstDS)
	if err != nil {
		s.failTask(id, err)
		return
	}
	defer dr.Close()
	src, ok := sr.(connector.DataConnector)
	if !ok {
		s.failTask(id, errors.New("source cannot validate data"))
		return
	}
	dst, ok := dr.(connector.DataConnector)
	if !ok {
		s.failTask(id, errors.New("target cannot validate data"))
		return
	}
	if task.Mode != domain.ModeFull && strings.TrimSpace(task.ValidationBarrierPositionValue) != "" {
		barrier := domain.CDCPosition{TaskID: id, Direction: "forward", DatabaseType: string(srcDS.Type), PositionType: task.ValidationBarrierPositionType, PositionValue: task.ValidationBarrierPositionValue, Resource: task.ValidationBarrierResource}
		if snapper, supported := sr.(connector.ValidationSnapshotConnector); supported {
			snapshot, snapErr := snapper.OpenValidationSnapshot(ctx, barrier)
			if snapErr != nil {
				s.failTask(id, fmt.Errorf("open exact validation snapshot at %s=%s: %w", barrier.PositionType, barrier.PositionValue, snapErr))
				return
			}
			defer snapshot.Close()
			src = snapshot
			s.logTask(ctx, id, "", "", "", "INFO", fmt.Sprintf("validation source pinned to exact %s=%s snapshot", barrier.PositionType, barrier.PositionValue))
		} else if validationRequireExactWatermark() {
			s.failTask(id, fmt.Errorf("source %s does not implement exact validation snapshots required by QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK", srcDS.Type))
			return
		}
	}

	tables, err := s.repo.ListMigrationTables(ctx, id)
	if err != nil {
		s.failTask(id, err)
		return
	}
	mismatch := 0
	pageSize := validationPageSize()
	for _, tbl := range tables {
		first, err := repository.ListTableChunksPage(ctx, s.repo, id, tbl.ID, -1, "", 1)
		if err != nil {
			s.failTask(id, err)
			return
		}
		if len(first) == 0 {
			continue
		}
		complexSplit := first[0].SplitType != "PRIMARY_KEY_RANGE" && first[0].SplitType != "PK_RANGE" && first[0].SplitType != "PK_RANGE_ADAPTIVE"
		if complexSplit {
			current, e := s.repo.GetMigration(ctx, id)
			if e != nil || current.Status == domain.StatusPaused || current.Status == domain.StatusCancelled {
				return
			}
			targetCols := tbl.TargetColumns
			if len(targetCols) == 0 {
				targetCols = tbl.Columns
			}
			sourcePKs := append([]string(nil), tbl.PrimaryKeys...)
			if len(sourcePKs) == 0 && tbl.PrimaryKey != "" {
				sourcePKs = []string{tbl.PrimaryKey}
			}
			targetPKs := append([]string(nil), tbl.TargetPrimaryKeys...)
			if len(targetPKs) == 0 && tbl.TargetPrimaryKey != "" {
				targetPKs = []string{tbl.TargetPrimaryKey}
			}
			started := time.Now()
			var a, b validationpkg.Checksum
			var e1, e2 error
			if len(sourcePKs) == 0 || len(targetPKs) == 0 {
				e1 = errors.New("complex validation requires stable source and target migration keys")
			} else {
				// Stream source descriptors page-by-page. Large HASH/PARTITION tables can
				// contain hundreds of thousands of physical chunks; retaining every
				// ReadBatchRequest here would make validation heap scale with table size.
				a, e1 = streamComplexValidationSourceChecksum(ctx, s.repo, id, src, tbl, sourcePKs, task.BatchRows, pageSize)
				targetRequests := []connector.ReadBatchRequest{{Schema: tbl.TargetSchema, Table: tbl.TargetTable, PrimaryKey: targetPKs[0], PrimaryKeys: targetPKs, UseKeyset: true}}
				b, e2 = validationpkg.UnorderedCanonicalRequests(ctx, dst, targetRequests, targetCols, tbl.Columns, task.BatchRows)
			}
			finished := time.Now()
			status := domain.ValidationSuccess
			lastErr := ""
			if e1 != nil || e2 != nil {
				status = domain.ValidationError
				if e1 != nil {
					lastErr = e1.Error()
				} else {
					lastErr = e2.Error()
				}
				mismatch++
			} else if a != b {
				status = domain.ValidationMismatch
				mismatch++
			}
			// The complex table is scanned once, then coverage is persisted in a
			// second bounded pass. This keeps both request descriptors and coverage
			// writes O(page size) in the control-plane heap.
			afterNo, afterID := -1, ""
			for {
				page, pageErr := repository.ListTableChunksPage(ctx, s.repo, id, tbl.ID, afterNo, afterID, pageSize)
				if pageErr != nil {
					s.failTask(id, pageErr)
					return
				}
				if len(page) == 0 {
					break
				}
				for _, piece := range page {
					if piece.Status != domain.ChunkSuccess {
						continue
					}
					vr := domain.ValidationResult{ID: newID("val"), TaskID: id, TableID: tbl.ID, ChunkID: piece.ID, Status: status, SourceRows: a.Rows, TargetRows: b.Rows, SourceChecksum: a.Hash, TargetChecksum: b.Hash, LastError: lastErr, StartedAt: started, FinishedAt: finished}
					_ = s.repo.CreateValidationResult(ctx, &vr)
				}
				last := page[len(page)-1]
				afterNo, afterID = last.ChunkNo, last.ID
				if len(page) < pageSize {
					break
				}
			}
			continue
		}

		afterNo, afterID := -1, ""
		for {
			page, err := repository.ListTableChunksPage(ctx, s.repo, id, tbl.ID, afterNo, afterID, pageSize)
			if err != nil {
				s.failTask(id, err)
				return
			}
			if len(page) == 0 {
				break
			}
			for _, ch := range page {
				if ch.Status != domain.ChunkSuccess {
					continue
				}
				current, e := s.repo.GetMigration(ctx, id)
				if e != nil || current.Status == domain.StatusPaused || current.Status == domain.StatusCancelled {
					return
				}
				vr := domain.ValidationResult{ID: newID("val"), TaskID: id, TableID: tbl.ID, ChunkID: ch.ID, Status: domain.ValidationRunning, StartedAt: time.Now()}
				targetCols := tbl.TargetColumns
				if len(targetCols) == 0 {
					targetCols = tbl.Columns
				}
				a, e1 := validationpkg.Range(ctx, src, tbl.SourceSchema, tbl.SourceTable, tbl.PrimaryKey, tbl.Columns, ch.Start, ch.End, task.BatchRows)
				targetPK := tbl.TargetPrimaryKey
				if targetPK == "" {
					targetPK = tbl.PrimaryKey
				}
				b, e2 := validationpkg.RangeCanonical(ctx, dst, tbl.TargetSchema, tbl.TargetTable, targetPK, targetCols, tbl.Columns, ch.Start, ch.End, task.BatchRows)
				vr.FinishedAt = time.Now()
				if e1 != nil || e2 != nil {
					vr.Status = domain.ValidationError
					if e1 != nil {
						vr.LastError = e1.Error()
					} else {
						vr.LastError = e2.Error()
					}
					mismatch++
				} else {
					vr.SourceRows = a.Rows
					vr.TargetRows = b.Rows
					vr.SourceChecksum = a.Hash
					vr.TargetChecksum = b.Hash
					if a == b {
						vr.Status = domain.ValidationSuccess
					} else {
						vr.Status = domain.ValidationMismatch
						mismatch++
					}
				}
				_ = s.repo.CreateValidationResult(ctx, &vr)
			}
			last := page[len(page)-1]
			afterNo, afterID = last.ChunkNo, last.ID
			if len(page) < pageSize {
				break
			}
		}
	}

	task, _ = s.repo.GetMigration(ctx, id)
	if task != nil && task.Mode != domain.ModeFull {
		latest, latestErr := s.latestCDC(ctx, id, "forward")
		if latestErr != nil {
			s.failTask(id, fmt.Errorf("validation barrier verification: %w", latestErr))
			return
		}
		if !validationBarrierMatches(task, latest) {
			_ = s.repo.DeleteValidationResults(ctx, id)
			old := task.ValidationBarrierPositionValue
			current := ""
			if latest != nil {
				current = latest.PositionValue
			}
			task.Status = domain.StatusCDCCatchingUp
			task.LastError = ""
			task.ValidationBarrierPositionType = ""
			task.ValidationBarrierPositionValue = ""
			task.ValidationBarrierResource = ""
			task.ValidationBarrierCapturedAt = time.Time{}
			task.UpdatedAt = time.Now()
			_ = s.repo.UpdateMigration(ctx, task)
			s.logTask(ctx, id, "", "", "", "WARN", fmt.Sprintf("validation barrier drifted from %s to %s; discarded validation results and returned to CDC catch-up", old, current))
			s.scheduleCDCSpoolDrain(id, "forward")
			return
		}
	}
	if mismatch > 0 {
		task.Status = domain.StatusFailed
		task.LastError = fmt.Sprintf("validation found %d mismatched/error groups", mismatch)
		_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Data validation failed", Message: task.LastError, TaskID: id, CreatedAt: time.Now()})
	} else {
		if err := s.applyPostLoadForeignKeys(ctx, task, tables); err != nil {
			task.Status = domain.StatusFailed
			task.LastError = "post-load foreign key creation failed: " + err.Error()
			_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "critical", Title: "Post-load DDL failed", Message: task.LastError, TaskID: id, CreatedAt: time.Now()})
		} else if task.Mode == domain.ModeFull {
			task.Status = domain.StatusFinished
			task.Progress = 100
		} else {
			task.Status = domain.StatusCDCCatchingUp
		}
	}
	task.UpdatedAt = time.Now()
	_ = s.repo.UpdateMigration(ctx, task)
	if task.Status == domain.StatusFinished || task.Status == domain.StatusRolledBack {
		s.archiveValidationBestEffort(ctx, task.ID)
	}
	if task.Status == domain.StatusCDCCatchingUp {
		s.scheduleCDCSpoolDrain(task.ID, "forward")
	}
}

func (s *Service) MarkCDCStarted(ctx context.Context, id string, p *domain.CDCPosition) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusCDCInitializing {
		return fmt.Errorf("task cannot start CDC from %s", task.Status)
	}
	if p != nil && (p.PositionValue != "" || p.LagMS != 0) {
		p.ID = newID("cdc")
		p.TaskID = id
		p.Direction = "forward"
		p.RecordedAt = time.Now()
		_ = s.repo.CreateCDCPosition(ctx, p)
	}
	if task.Mode == domain.ModeIncremental {
		if err := Transition(task, domain.StatusCDCCatchingUp); err != nil {
			return err
		}
	} else {
		if err := Transition(task, domain.StatusFullMigrating); err != nil {
			return err
		}
		if task.TotalChunks == 0 {
			task.Progress = 100
			_ = Transition(task, domain.StatusFullFinished)
			_ = Transition(task, domain.StatusCDCCatchingUp)
		}
	}
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	if task.Status == domain.StatusCDCCatchingUp {
		s.scheduleCDCSpoolDrain(task.ID, "forward")
	}
	return nil
}

func decodeCDCValue(field domain.CDCField) (connector.Value, error) {
	if field.Null {
		return connector.Value{Null: true}, nil
	}
	switch strings.ToLower(strings.TrimSpace(field.Encoding)) {
	case "", "text", "utf8", "json":
		return connector.Value{Raw: []byte(field.Value)}, nil
	case "base64":
		v, err := base64.StdEncoding.DecodeString(field.Value)
		if err != nil {
			return connector.Value{}, fmt.Errorf("decode base64 field %s: %w", field.Column, err)
		}
		return connector.Value{Raw: v}, nil
	default:
		return connector.Value{}, fmt.Errorf("unsupported CDC field encoding %q for %s", field.Encoding, field.Column)
	}
}

func cdcFieldMap(fields []domain.CDCField) map[string]domain.CDCField {
	out := make(map[string]domain.CDCField, len(fields))
	for _, f := range fields {
		out[f.Column] = f
	}
	return out
}

func reverseMigrationTable(t domain.MigrationTable) domain.MigrationTable {
	r := t
	r.SourceSchema, r.TargetSchema = t.TargetSchema, t.SourceSchema
	r.SourceTable, r.TargetTable = t.TargetTable, t.SourceTable
	r.PrimaryKey, r.TargetPrimaryKey = t.TargetPrimaryKey, t.PrimaryKey
	r.PrimaryKeys, r.TargetPrimaryKeys = append([]string(nil), t.TargetPrimaryKeys...), append([]string(nil), t.PrimaryKeys...)
	r.Columns, r.TargetColumns = t.TargetColumns, t.Columns
	if r.PrimaryKey == "" {
		r.PrimaryKey = t.PrimaryKey
	}
	if r.TargetPrimaryKey == "" {
		r.TargetPrimaryKey = t.TargetPrimaryKey
	}
	return r
}

type cdcSpoolDrainLeaseRepository interface {
	AcquireCDCSpoolDrainLease(context.Context, string, string, string, time.Duration) (bool, error)
	ReleaseCDCSpoolDrainLease(context.Context, string, string, string) error
}

func (s *Service) acquireCDCSpoolDrainLease(ctx context.Context, taskID, direction string) (bool, error) {
	p, ok := s.repo.(cdcSpoolDrainLeaseRepository)
	if !ok {
		return true, nil
	}
	ttl := time.Duration(envInt64("QMIGRATION_CDC_SPOOL_DRAIN_LEASE_SECONDS", 300)) * time.Second
	if ttl < 30*time.Second {
		ttl = 30 * time.Second
	}
	return p.AcquireCDCSpoolDrainLease(ctx, taskID, direction, s.instanceID, ttl)
}

func (s *Service) releaseCDCSpoolDrainLease(taskID, direction string) {
	p, ok := s.repo.(cdcSpoolDrainLeaseRepository)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.ReleaseCDCSpoolDrainLease(ctx, taskID, direction, s.instanceID)
}

func (s *Service) cdcSpoolLock(taskID, direction string) *sync.Mutex {
	key := taskID + "\x00" + direction
	s.cdcSpoolMu.Lock()
	defer s.cdcSpoolMu.Unlock()
	if lock := s.cdcSpoolLocks[key]; lock != nil {
		return lock
	}
	lock := &sync.Mutex{}
	s.cdcSpoolLocks[key] = lock
	return lock
}

func cdcApplyReady(task *domain.MigrationTask, direction string) bool {
	if task == nil {
		return false
	}
	if direction == "reverse" {
		return task.Status == domain.StatusRollbackSyncing || task.Status == domain.StatusRollbackReady
	}
	// VALIDATING deliberately freezes target apply. Source CDC capture remains
	// active and ApplyEngineJobCDCEvents durably stages incoming transactions so
	// validation observes a stable target at ValidationBarrierPositionValue.
	return task.Status == domain.StatusCDCCatchingUp || task.Status == domain.StatusReadyCutover
}

func cdcCaptureReady(task *domain.MigrationTask, direction string) bool {
	if task == nil {
		return false
	}
	if direction == "reverse" {
		return task.Status == domain.StatusRollbackSyncing || task.Status == domain.StatusRollbackReady || task.Status == domain.StatusRollbackPreparing
	}
	switch task.Status {
	case domain.StatusFullMigrating, domain.StatusFullFinished, domain.StatusValidating, domain.StatusCDCCatchingUp, domain.StatusReadyCutover:
		return true
	default:
		return false
	}
}

func cdcSpoolID(taskID, direction string, last domain.CDCEvent) string {
	raw := taskID + "\x00" + direction + "\x00" + last.PositionType + "\x00" + last.PositionValue + "\x00" + last.Resource
	sum := sha256.Sum256([]byte(raw))
	return "spool_" + hex.EncodeToString(sum[:12])
}

func envInt64(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}

func (s *Service) stageCDCEvents(ctx context.Context, taskID, direction string, events []domain.CDCEvent) (*domain.CDCApplyResult, error) {
	if len(events) == 0 {
		return nil, errors.New("CDC spool transaction is empty")
	}
	last := events[len(events)-1]
	if strings.TrimSpace(last.PositionValue) == "" {
		return nil, errors.New("CDC spool requires a durable source position")
	}
	raw, err := json.Marshal(events)
	if err != nil {
		return nil, fmt.Errorf("marshal CDC spool transaction: %w", err)
	}
	maxTx := envInt64("QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES", 16<<20)
	if maxTx > 0 && int64(len(raw)) > maxTx {
		return nil, fmt.Errorf("CDC transaction is %d bytes and exceeds QMIGRATION_CDC_SPOOL_MAX_TRANSACTION_BYTES=%d; source position is not acknowledged", len(raw), maxTx)
	}
	stats, err := s.repo.CDCSpoolStats(ctx, taskID, direction)
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(strings.TrimSpace(stats.StorageLevel)) {
	case "CRITICAL":
		return nil, fmt.Errorf("CDC spool storage is CRITICAL at %.1f%% used; source position is not acknowledged", stats.StorageUsedPct)
	case "WARN":
		delay := time.Duration(envInt64("QMIGRATION_CDC_SPOOL_WARN_BACKPRESSURE_MS", 250)) * time.Millisecond
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	maxPending := envInt64("QMIGRATION_CDC_SPOOL_MAX_PENDING_BYTES", 64<<30)
	if maxPending > 0 && stats.PendingBytes+int64(len(raw)) > maxPending {
		return nil, fmt.Errorf("CDC spool capacity would exceed %d bytes (pending=%d incoming=%d); source position is not acknowledged", maxPending, stats.PendingBytes, len(raw))
	}
	now := time.Now()
	rec := &domain.CDCSpoolRecord{
		ID: cdcSpoolID(taskID, direction, last), TaskID: taskID, Direction: direction,
		PositionType: last.PositionType, PositionValue: last.PositionValue, Resource: last.Resource,
		SourceTimestampMS: last.SourceTimestampMS, Events: append([]domain.CDCEvent(nil), events...),
		EventCount: len(events), PayloadBytes: int64(len(raw)), Status: domain.CDCSpoolPending, CreatedAt: now,
	}
	if err := faultinject.Check("cdc.spool.before_persist"); err != nil {
		return nil, err
	}
	if err := s.repo.CreateCDCSpool(ctx, rec); err != nil {
		return nil, fmt.Errorf("persist CDC spool: %w", err)
	}
	if err := faultinject.Check("cdc.spool.after_persist_before_ack"); err != nil {
		return nil, err
	}
	return &domain.CDCApplyResult{Applied: 0, LastEventID: last.ID, PositionType: last.PositionType, PositionValue: last.PositionValue, Staged: true, SpoolSequence: rec.Sequence}, nil
}

func (s *Service) drainCDCSpoolLocked(ctx context.Context, taskID, direction string, maxTransactions int) (domain.CDCSpoolStats, error) {
	if maxTransactions <= 0 {
		maxTransactions = 1000
	}
	acquired, err := s.acquireCDCSpoolDrainLease(ctx, taskID, direction)
	if err != nil {
		return domain.CDCSpoolStats{}, err
	}
	if !acquired {
		return s.repo.CDCSpoolStats(ctx, taskID, direction)
	}
	defer s.releaseCDCSpoolDrainLease(taskID, direction)
	task, err := s.repo.GetMigration(ctx, taskID)
	if err != nil {
		return domain.CDCSpoolStats{}, err
	}
	if !cdcApplyReady(task, direction) {
		return s.repo.CDCSpoolStats(ctx, taskID, direction)
	}
	remaining := maxTransactions
	for remaining > 0 {
		batch := remaining
		if batch > 100 {
			batch = 100
		}
		if renewed, renewErr := s.acquireCDCSpoolDrainLease(ctx, taskID, direction); renewErr != nil {
			return domain.CDCSpoolStats{}, renewErr
		} else if !renewed {
			return s.repo.CDCSpoolStats(ctx, taskID, direction)
		}
		items, e := s.repo.ListCDCSpool(ctx, taskID, direction, batch)
		if e != nil {
			return domain.CDCSpoolStats{}, e
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			if e := faultinject.Check("cdc.spool.before_target_apply"); e != nil {
				return domain.CDCSpoolStats{}, e
			}
			applyReq := domain.CDCApplyRequest{Direction: direction, Events: item.Events}
			if _, e := s.ApplyCDCEvents(ctx, taskID, applyReq); e != nil {
				// The durable spool item being drained is the ordering gap. Record that
				// exact retained transaction, not a newer request that happened to
				// trigger the drain. This is also the only path used by background
				// scheduled drains, so COMMIT_UNCERTAIN must be persisted here.
				s.recordCDCDeadLetter(ctx, taskID, direction, applyReq, e)
				return domain.CDCSpoolStats{}, fmt.Errorf("drain CDC spool sequence %d position %s: %w", item.Sequence, item.PositionValue, e)
			}
			if e := faultinject.Check("cdc.spool.after_target_apply_before_mark"); e != nil {
				return domain.CDCSpoolStats{}, e
			}
			if e := s.repo.MarkCDCSpoolApplied(ctx, item.ID, time.Now()); e != nil {
				return domain.CDCSpoolStats{}, e
			}
			if e := faultinject.Check("cdc.spool.after_mark_applied"); e != nil {
				return domain.CDCSpoolStats{}, e
			}
			remaining--
			if remaining <= 0 {
				break
			}
		}
	}
	keep := 1000
	if raw := strings.TrimSpace(os.Getenv("QMIGRATION_CDC_SPOOL_KEEP_APPLIED")); raw != "" {
		if n, e := strconv.Atoi(raw); e == nil && n >= 0 {
			keep = n
		}
	}
	_ = s.repo.DeleteAppliedCDCSpool(ctx, taskID, direction, keep)
	return s.repo.CDCSpoolStats(ctx, taskID, direction)
}

func (s *Service) DrainCDCSpool(ctx context.Context, taskID, direction string, maxTransactions int) (domain.CDCSpoolStats, error) {
	if direction == "" {
		direction = "forward"
	}
	lock := s.cdcSpoolLock(taskID, direction)
	lock.Lock()
	stats, err := s.drainCDCSpoolLocked(ctx, taskID, direction, maxTransactions)
	lock.Unlock()
	if err == nil && direction == "forward" {
		s.maybeStartValidationAfterCatchup(ctx, taskID)
	}
	return stats, err
}

func (s *Service) CDCSpoolStats(ctx context.Context, taskID, direction string) (domain.CDCSpoolStats, error) {
	if direction == "" {
		direction = "forward"
	}
	return s.repo.CDCSpoolStats(ctx, taskID, direction)
}

func validationCatchupMaxLagMS() int64 {
	return envInt64("QMIGRATION_VALIDATION_MAX_CDC_LAG_MS", 5000)
}

func validationStableWindow() time.Duration {
	return time.Duration(envInt64("QMIGRATION_VALIDATION_STABLE_WINDOW_SECONDS", 2)) * time.Second
}

func validationRequireExactWatermark() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REQUIRE_EXACT_WATERMARK"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (s *Service) captureValidationBarrier(ctx context.Context, task *domain.MigrationTask) error {
	if task == nil || task.Mode == domain.ModeFull {
		return nil
	}
	latest, err := s.latestCDC(ctx, task.ID, "forward")
	if err != nil {
		return err
	}
	if latest != nil {
		window := validationStableWindow()
		if window > 0 && !latest.RecordedAt.IsZero() && time.Since(latest.RecordedAt) < window {
			return fmt.Errorf("CDC validation barrier is not stable yet; latest checkpoint is only %s old (required quiet window %s)", time.Since(latest.RecordedAt).Round(time.Millisecond), window)
		}
		task.ValidationBarrierPositionType = latest.PositionType
		task.ValidationBarrierPositionValue = latest.PositionValue
		task.ValidationBarrierResource = latest.Resource
	} else if strings.TrimSpace(task.CDCStartPositionValue) != "" {
		task.ValidationBarrierPositionType = task.CDCStartPositionType
		task.ValidationBarrierPositionValue = task.CDCStartPositionValue
		task.ValidationBarrierResource = task.CDCStartResource
	} else {
		return errors.New("cannot capture validation barrier before a durable CDC position exists")
	}
	task.ValidationBarrierCapturedAt = time.Now()
	return nil
}

func validationBarrierMatches(task *domain.MigrationTask, latest *domain.CDCPosition) bool {
	if task == nil || task.Mode == domain.ModeFull {
		return true
	}
	if latest == nil {
		return strings.TrimSpace(task.ValidationBarrierPositionValue) == strings.TrimSpace(task.CDCStartPositionValue)
	}
	return strings.EqualFold(strings.TrimSpace(task.ValidationBarrierPositionType), strings.TrimSpace(latest.PositionType)) &&
		strings.TrimSpace(task.ValidationBarrierPositionValue) == strings.TrimSpace(latest.PositionValue) &&
		strings.TrimSpace(task.ValidationBarrierResource) == strings.TrimSpace(latest.Resource)
}

// maybeStartValidationAfterCatchup advances a full+incremental task into its
// validation phase only after the durable CDC spool is empty and the live CDC
// lag is within the configured validation gate. Validation results are durable,
// so a successful/attempted validation is never started a second time merely
// because the task returns to CDC_CATCHING_UP afterwards.
func (s *Service) maybeStartValidationAfterCatchup(ctx context.Context, taskID string) bool {
	s.validationMu.Lock()
	defer s.validationMu.Unlock()

	// Serialize the empty-spool check, barrier capture and VALIDATING transition
	// with CDC apply. This closes the old race where a target transaction could
	// commit after barrier capture but before the task became VALIDATING.
	lock := s.cdcSpoolLock(taskID, "forward")
	lock.Lock()
	defer lock.Unlock()

	task, err := s.repo.GetMigration(ctx, taskID)
	if err != nil || task.Status != domain.StatusCDCCatchingUp || task.Mode != domain.ModeFullAndIncremental || !task.ValidationEnabled {
		return false
	}
	hasResults, err := repository.HasValidationResults(ctx, s.repo, taskID)
	if err != nil || hasResults {
		return false
	}
	stats, err := s.repo.CDCSpoolStats(ctx, taskID, "forward")
	if err != nil || stats.PendingTransactions > 0 {
		return false
	}
	maxLag := validationCatchupMaxLagMS()
	if maxLag > 0 && task.CDCLagMS > maxLag {
		return false
	}
	if err := s.captureValidationBarrier(ctx, task); err != nil {
		return false
	}
	if err := Transition(task, domain.StatusValidating); err != nil {
		return false
	}
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return false
	}
	s.logTask(ctx, taskID, "", "", "", "INFO", fmt.Sprintf("CDC spool caught up; target apply frozen at validation barrier %s=%s; new source transactions will spool; lag=%dms", task.ValidationBarrierPositionType, task.ValidationBarrierPositionValue, task.CDCLagMS))
	go s.validateTask(taskID)
	return true
}

func (s *Service) scheduleCDCSpoolDrain(taskID, direction string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		for {
			stats, err := s.DrainCDCSpool(ctx, taskID, direction, 10000)
			if err != nil {
				s.logTask(context.Background(), taskID, "", "", "", "ERROR", "CDC spool drain failed: "+err.Error())
				return
			}
			if stats.PendingTransactions == 0 {
				s.maybeStartValidationAfterCatchup(ctx, taskID)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()
}

// ApplyCDCEvents is a native target-side event bridge. It deliberately accepts
// already-decoded row events rather than parsing Binlog/WAL here: parsers,
// Debezium bridges and future native readers can all reuse the same idempotent
// table mapping, delete handling and durable CDC checkpoint path.
// INSERT/UPDATE events must contain a full after-image for all migrated columns.
func (s *Service) ApplyEngineJobCDCEvents(ctx context.Context, workerID, jobID string, req domain.CDCApplyRequest) (result *domain.CDCApplyResult, retErr error) {
	job, err := s.repo.GetEngineJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.WorkerID != workerID {
		return nil, repository.ErrLeaseOwner
	}
	if job.Status != domain.EngineJobRunning && job.Status != domain.EngineJobStopRequested {
		return nil, fmt.Errorf("engine job %s cannot apply CDC from %s", jobID, job.Status)
	}
	req.Direction = job.Direction
	lock := s.cdcSpoolLock(job.TaskID, job.Direction)
	lock.Lock()
	defer func() {
		lock.Unlock()
		if retErr == nil && job.Direction == "forward" {
			s.maybeStartValidationAfterCatchup(ctx, job.TaskID)
		}
	}()
	task, err := s.repo.GetMigration(ctx, job.TaskID)
	if err != nil {
		return nil, err
	}
	if !cdcApplyReady(task, job.Direction) {
		return s.stageCDCEvents(ctx, job.TaskID, job.Direction, req.Events)
	}
	stats, statErr := s.repo.CDCSpoolStats(ctx, job.TaskID, job.Direction)
	if statErr != nil {
		return nil, statErr
	}
	if stats.PendingTransactions > 0 {
		staged, stageErr := s.stageCDCEvents(ctx, job.TaskID, job.Direction, req.Events)
		if stageErr != nil {
			return nil, stageErr
		}
		maxDrain := 1000
		if raw := strings.TrimSpace(os.Getenv("QMIGRATION_CDC_SPOOL_DRAIN_PER_REQUEST")); raw != "" {
			if n, e := strconv.Atoi(raw); e == nil && n > 0 {
				maxDrain = n
			}
		}
		if _, drainErr := s.drainCDCSpoolLocked(ctx, job.TaskID, job.Direction, maxDrain); drainErr != nil {
			// drainCDCSpoolLocked records the exact historical spool transaction
			// that failed. Do not dead-letter this newer request by mistake.
			return nil, drainErr
		}
		return staged, nil
	}
	result, applyErr := s.ApplyCDCEvents(ctx, job.TaskID, req)
	if applyErr != nil {
		s.recordCDCDeadLetter(ctx, job.TaskID, job.Direction, req, applyErr)
	}
	return result, applyErr
}

func cdcDeadLetterID(taskID, direction string, event domain.CDCEvent) string {
	raw := taskID + "\x00" + direction + "\x00" + event.PositionType + "\x00" + event.PositionValue + "\x00" + event.Resource
	sum := sha256.Sum256([]byte(raw))
	return "dlq_" + hex.EncodeToString(sum[:8])
}

const cdcPreCommitFenceMessage = "pre-COMMIT durable ambiguity fence; target outcome must never be inferred without checkpoint"

type cdcCommitFence struct {
	ID      string
	Existed bool
}

// beginCDCCommitFence durably retains the exact source transaction before a
// transactional target COMMIT is sent. If the process dies any time from this
// point through durable checkpoint persistence, restart sees COMMIT_UNCERTAIN
// and will not blindly reapply target DML. A pre-existing DLQ (for example a
// REPLAY_REQUIRED item) is reused so its operator recovery remains ordered.
func (s *Service) beginCDCCommitFence(ctx context.Context, taskID, direction string, req domain.CDCApplyRequest) (*cdcCommitFence, error) {
	if len(req.Events) == 0 {
		return nil, errors.New("cannot persist CDC commit fence without events")
	}
	last := req.Events[len(req.Events)-1]
	if strings.TrimSpace(last.PositionValue) == "" {
		return nil, errors.New("cannot persist CDC commit fence without durable source position")
	}
	id := cdcDeadLetterID(taskID, direction, last)
	now := time.Now()
	items, err := s.repo.ListCDCDeadLetters(ctx, taskID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].ID != id {
			continue
		}
		v := items[i]
		v.Direction = direction
		v.PositionType, v.PositionValue, v.Resource = last.PositionType, last.PositionValue, last.Resource
		v.Events = append([]domain.CDCEvent(nil), req.Events...)
		v.Status = domain.CDCDeadLetterCommitUncertain
		v.LastError = cdcPreCommitFenceMessage
		v.UpdatedAt = now
		v.ResolvedAt = time.Time{}
		if err := s.repo.UpdateCDCDeadLetter(ctx, &v); err != nil {
			return nil, err
		}
		return &cdcCommitFence{ID: id, Existed: true}, nil
	}
	v := &domain.CDCDeadLetter{
		ID: id, TaskID: taskID, Direction: direction,
		PositionType: last.PositionType, PositionValue: last.PositionValue, Resource: last.Resource,
		Events: append([]domain.CDCEvent(nil), req.Events...), LastError: cdcPreCommitFenceMessage,
		RetryCount: 0, Status: domain.CDCDeadLetterCommitUncertain, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.repo.CreateCDCDeadLetter(ctx, v); err != nil {
		return nil, err
	}
	return &cdcCommitFence{ID: id}, nil
}

// completeCDCCommitFence is called only after the target commit AND the durable
// QMigration source checkpoint have both succeeded. New transient fences are
// deleted to avoid one DLQ row per successful transaction; a fence that reused
// an operator-visible prior DLQ is retained as RESOLVED audit history.
func (s *Service) completeCDCCommitFence(ctx context.Context, fence *cdcCommitFence) error {
	if fence == nil || fence.ID == "" {
		return nil
	}
	if !fence.Existed {
		return s.repo.DeleteCDCDeadLetter(ctx, fence.ID)
	}
	v, err := s.repo.GetCDCDeadLetter(ctx, fence.ID)
	if err != nil {
		return err
	}
	now := time.Now()
	v.Status = domain.CDCDeadLetterResolved
	v.LastError = ""
	v.ResolvedAt, v.UpdatedAt = now, now
	return s.repo.UpdateCDCDeadLetter(ctx, v)
}

// cleanupTransientCDCCommitFence handles the narrow metadata failure window in
// which the durable checkpoint was written but deletion of a newly-created
// pre-COMMIT fence failed. The matching source position proves target apply was
// already checkpointed, so only the zero-retry transient fence is removable.
func (s *Service) cleanupTransientCDCCommitFence(ctx context.Context, taskID, direction string, last domain.CDCEvent) error {
	id := cdcDeadLetterID(taskID, direction, last)
	v, err := s.repo.GetCDCDeadLetter(ctx, id)
	if err != nil {
		return nil
	}
	if v.Status == domain.CDCDeadLetterCommitUncertain && v.RetryCount == 0 && v.LastError == cdcPreCommitFenceMessage {
		return s.repo.DeleteCDCDeadLetter(ctx, id)
	}
	return nil
}

type cdcCommitOutcomeUnknownError struct{ cause error }

type cdcBlockedByCommitUncertainError struct {
	dlqID, positionType, positionValue string
	status                             domain.CDCDeadLetterStatus
}

type cdcReplayDLQContextKey struct{}

func (e *cdcBlockedByCommitUncertainError) Error() string {
	if e.status == domain.CDCDeadLetterReplayRequired {
		return fmt.Sprintf("CDC is blocked by retained target COMMIT recovery at %s (%s); explicit replay of DLQ %s is required before later source events", e.positionValue, e.positionType, e.dlqID)
	}
	return fmt.Sprintf("CDC is blocked by unresolved target COMMIT outcome at %s (%s); resolve DLQ %s as COMMITTED or NOT_COMMITTED before applying more source events", e.positionValue, e.positionType, e.dlqID)
}

func (e *cdcCommitOutcomeUnknownError) Error() string {
	return "target CDC COMMIT outcome is unknown; automatic replay is blocked: " + e.cause.Error()
}
func (e *cdcCommitOutcomeUnknownError) Unwrap() error { return e.cause }

func (s *Service) unresolvedCommitRecovery(ctx context.Context, taskID, direction string) (*domain.CDCDeadLetter, error) {
	items, err := s.repo.ListCDCDeadLetters(ctx, taskID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if !strings.EqualFold(items[i].Direction, direction) {
			continue
		}
		if items[i].Status == domain.CDCDeadLetterCommitUncertain || items[i].Status == domain.CDCDeadLetterReplayRequired {
			v := items[i]
			return &v, nil
		}
	}
	return nil, nil
}

func (s *Service) recordCDCDeadLetter(ctx context.Context, taskID, direction string, req domain.CDCApplyRequest, cause error) {
	var blocked *cdcBlockedByCommitUncertainError
	if errors.As(cause, &blocked) {
		// The original COMMIT_UNCERTAIN DLQ already owns this ordering gap. Do
		// not create one new DLQ per source retry while that gap is unresolved.
		return
	}
	if len(req.Events) == 0 || req.Events[len(req.Events)-1].PositionValue == "" {
		return
	}
	maxBytes := 4 << 20
	if raw := os.Getenv("QMIGRATION_CDC_DLQ_MAX_BYTES"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			maxBytes = n
		}
	}
	payload, _ := json.Marshal(req.Events)
	if maxBytes == 0 || len(payload) > maxBytes {
		_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "error", Title: "CDC DLQ payload not persisted", Message: fmt.Sprintf("failed CDC transaction at %s is %d bytes (limit %d): %v", req.Events[len(req.Events)-1].PositionValue, len(payload), maxBytes, cause), TaskID: taskID, CreatedAt: time.Now()})
		return
	}
	last := req.Events[len(req.Events)-1]
	id := cdcDeadLetterID(taskID, direction, last)
	now := time.Now()
	status := domain.CDCDeadLetterOpen
	title := "CDC transaction dead-lettered"
	var uncertain *cdcCommitOutcomeUnknownError
	if errors.As(cause, &uncertain) {
		status = domain.CDCDeadLetterCommitUncertain
		title = "CDC target commit outcome unknown"
	}
	if existing, err := s.repo.GetCDCDeadLetter(ctx, id); err == nil {
		existing.Events = append([]domain.CDCEvent(nil), req.Events...)
		existing.LastError = cause.Error()
		existing.RetryCount++
		// Never downgrade an unresolved ambiguous COMMIT to a normal OPEN DLQ
		// just because a later automatic retry was correctly blocked. Only the
		// explicit resolution API may leave COMMIT_UNCERTAIN.
		if existing.Status == domain.CDCDeadLetterCommitUncertain && status != domain.CDCDeadLetterCommitUncertain {
			status = domain.CDCDeadLetterCommitUncertain
		}
		existing.Status = status
		existing.UpdatedAt = now
		existing.ResolvedAt = time.Time{}
		_ = s.repo.UpdateCDCDeadLetter(ctx, existing)
	} else {
		_ = s.repo.CreateCDCDeadLetter(ctx, &domain.CDCDeadLetter{ID: id, TaskID: taskID, Direction: direction, PositionType: last.PositionType, PositionValue: last.PositionValue, Resource: last.Resource, Events: append([]domain.CDCEvent(nil), req.Events...), LastError: cause.Error(), RetryCount: 1, Status: status, CreatedAt: now, UpdatedAt: now})
	}
	_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "error", Title: title, Message: fmt.Sprintf("%s CDC transaction at %s failed: %v", direction, last.PositionValue, cause), TaskID: taskID, CreatedAt: now})
}

func (s *Service) CDCDeadLetters(ctx context.Context, taskID string) ([]domain.CDCDeadLetter, error) {
	return s.repo.ListCDCDeadLetters(ctx, taskID)
}

func (s *Service) ReplayCDCDeadLetter(ctx context.Context, taskID, id string) (*domain.CDCApplyResult, error) {
	dlq, err := s.repo.GetCDCDeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}
	if dlq.TaskID != taskID {
		return nil, errors.New("CDC dead letter does not belong to migration")
	}
	if dlq.Status == domain.CDCDeadLetterCommitUncertain {
		return nil, errors.New("CDC dead letter has COMMIT_UNCERTAIN status; automatic replay is forbidden until the target commit outcome is resolved")
	}
	replayCtx := context.WithValue(ctx, cdcReplayDLQContextKey{}, id)
	result, applyErr := s.ApplyCDCEvents(replayCtx, taskID, domain.CDCApplyRequest{Direction: dlq.Direction, Events: append([]domain.CDCEvent(nil), dlq.Events...)})
	now := time.Now()
	dlq.UpdatedAt = now
	if applyErr != nil {
		dlq.RetryCount++
		dlq.LastError = applyErr.Error()
		var uncertain *cdcCommitOutcomeUnknownError
		if errors.As(applyErr, &uncertain) {
			// An explicit NOT_COMMITTED replay can itself lose the new COMMIT
			// response. Return to the strict uncertain state and require another
			// operator decision; never assume the second attempt failed.
			dlq.Status = domain.CDCDeadLetterCommitUncertain
		}
		_ = s.repo.UpdateCDCDeadLetter(ctx, dlq)
		return result, applyErr
	}
	dlq.Status = domain.CDCDeadLetterResolved
	dlq.LastError = ""
	dlq.ResolvedAt = now
	if err := s.repo.UpdateCDCDeadLetter(ctx, dlq); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Service) ResolveCDCCommitUncertain(ctx context.Context, taskID, id, decision string) (*domain.CDCApplyResult, error) {
	dlq, err := s.repo.GetCDCDeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}
	if dlq.TaskID != taskID {
		return nil, errors.New("CDC dead letter does not belong to migration")
	}
	if dlq.Status != domain.CDCDeadLetterCommitUncertain {
		return nil, fmt.Errorf("CDC dead letter %s is %s, not COMMIT_UNCERTAIN", id, dlq.Status)
	}
	decision = strings.ToUpper(strings.TrimSpace(decision))
	now := time.Now()
	switch decision {
	case "COMMITTED":
		if len(dlq.Events) == 0 {
			return nil, errors.New("COMMIT_UNCERTAIN dead letter has no retained events")
		}
		task, err := s.repo.GetMigration(ctx, taskID)
		if err != nil {
			return nil, err
		}
		sourceDS, err := s.repo.GetDataSource(ctx, task.SourceID)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(dlq.Direction, "reverse") {
			sourceDS, err = s.repo.GetDataSource(ctx, task.TargetID)
			if err != nil {
				return nil, err
			}
		}
		last := dlq.Events[len(dlq.Events)-1]
		res := &domain.CDCApplyResult{Applied: len(dlq.Events), LastEventID: last.ID}
		res, err = s.persistCDCApplyResult(ctx, taskID, dlq.Direction, sourceDS, res, last)
		if err != nil {
			return res, fmt.Errorf("persist operator-confirmed committed CDC checkpoint: %w", err)
		}
		dlq.Status = domain.CDCDeadLetterResolved
		dlq.LastError = "operator confirmed target transaction COMMITTED; durable source checkpoint advanced without replay"
		dlq.ResolvedAt, dlq.UpdatedAt = now, now
		if err := s.repo.UpdateCDCDeadLetter(ctx, dlq); err != nil {
			return res, err
		}
		return res, nil
	case "NOT_COMMITTED":
		// The operator has established that the target transaction did not commit.
		// Re-open the retained item and immediately execute the explicit replay in
		// this same operator action. This preserves source ordering and guarantees
		// that a successful retry also closes the DLQ instead of leaving a stale
		// OPEN item behind while the source reader races to redeliver it.
		dlq.Status = domain.CDCDeadLetterReplayRequired
		dlq.LastError = "operator confirmed target transaction NOT_COMMITTED; explicit retained-event replay required"
		dlq.UpdatedAt = now
		if err := s.repo.UpdateCDCDeadLetter(ctx, dlq); err != nil {
			return nil, err
		}
		return s.ReplayCDCDeadLetter(ctx, taskID, id)
	default:
		return nil, errors.New("decision must be COMMITTED or NOT_COMMITTED")
	}
}

func cdcDDLIdentitySafe(tables []domain.MigrationTable, direction string) error {
	if len(tables) == 0 {
		return errors.New("CDC DDL replay requires planned migration tables")
	}
	for _, table := range tables {
		if direction == "reverse" {
			table = reverseMigrationTable(table)
		}
		if !strings.EqualFold(table.SourceSchema, table.TargetSchema) || !strings.EqualFold(table.SourceTable, table.TargetTable) {
			return fmt.Errorf("CDC DDL same-family replay requires identity schema/table mapping; %s maps to %s", objectName(table.SourceSchema, table.SourceTable), objectName(table.TargetSchema, table.TargetTable))
		}
		targetCols := table.TargetColumns
		if len(targetCols) == 0 {
			targetCols = table.Columns
		}
		if len(table.Columns) != len(targetCols) {
			return fmt.Errorf("CDC DDL replay requires identity columns for %s", objectName(table.SourceSchema, table.SourceTable))
		}
		for i := range table.Columns {
			if !strings.EqualFold(table.Columns[i].Name, targetCols[i].Name) {
				return fmt.Errorf("CDC DDL replay does not support column rename %s.%s -> %s", objectName(table.SourceSchema, table.SourceTable), table.Columns[i].Name, targetCols[i].Name)
			}
		}
	}
	return nil
}

func (s *Service) persistCDCApplyResult(ctx context.Context, id, direction string, sourceDS *domain.DataSource, result *domain.CDCApplyResult, last domain.CDCEvent) (*domain.CDCApplyResult, error) {
	positionType := last.PositionType
	if positionType == "" {
		positionType = "EVENT_BRIDGE"
	}
	nowMS := time.Now().UnixMilli()
	lag := int64(0)
	if last.SourceTimestampMS > 0 && nowMS > last.SourceTimestampMS {
		lag = nowMS - last.SourceTimestampMS
	}
	position := &domain.CDCPosition{DatabaseType: string(sourceDS.Type), PositionType: positionType, PositionValue: last.PositionValue, Resource: last.Resource, SourceTimestampMS: last.SourceTimestampMS, ApplyTimestampMS: nowMS, LagMS: lag, EventsTotal: int64(result.Applied + result.SkippedConflicts), EventsPending: 0}
	if err := faultinject.Check("cdc.apply.after_target_before_checkpoint"); err != nil {
		return result, err
	}
	var err error
	if direction == "reverse" {
		err = s.RecordRollbackCDCProgress(ctx, id, position)
	} else {
		err = s.recordCDCProgress(ctx, id, position, false)
	}
	if err != nil {
		return result, fmt.Errorf("persist CDC checkpoint after apply: %w", err)
	}
	result.PositionType = positionType
	result.PositionValue = last.PositionValue
	if err := faultinject.Check("cdc.apply.after_checkpoint_before_source_ack"); err != nil {
		return result, err
	}
	return result, nil
}

func compareConflictVersions(source, target string) int {
	source, target = strings.TrimSpace(source), strings.TrimSpace(target)
	layouts := []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		sv, se := time.Parse(layout, source)
		tv, te := time.Parse(layout, target)
		if se == nil && te == nil {
			if sv.Before(tv) {
				return -1
			}
			if sv.After(tv) {
				return 1
			}
			return 0
		}
	}
	if sr, ok := new(big.Rat).SetString(source); ok {
		if tr, ok := new(big.Rat).SetString(target); ok {
			return sr.Cmp(tr)
		}
	}
	return strings.Compare(source, target)
}

func cdcKeyValues(fields map[string]domain.CDCField, sourcePKs, targetPKs []string, targetCols []domain.ColumnInfo) ([]connector.Value, []domain.ColumnInfo, error) {
	values := make([]connector.Value, len(sourcePKs))
	cols := make([]domain.ColumnInfo, len(targetPKs))
	for i := range sourcePKs {
		field, ok := fields[sourcePKs[i]]
		if !ok {
			return nil, nil, fmt.Errorf("missing primary key field %s", sourcePKs[i])
		}
		v, err := decodeCDCValue(field)
		if err != nil {
			return nil, nil, err
		}
		if v.Null {
			return nil, nil, fmt.Errorf("primary key %s cannot be null", sourcePKs[i])
		}
		values[i] = v
		found := false
		for _, col := range targetCols {
			if strings.EqualFold(col.Name, targetPKs[i]) {
				cols[i] = col
				found = true
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("target primary key %s is not present in mapped columns", targetPKs[i])
		}
	}
	return values, cols, nil
}

func keyFingerprint(values []connector.Value) string {
	h := sha256.New()
	for _, v := range values {
		if v.Null {
			h.Write([]byte{0})
		} else {
			h.Write([]byte{1})
			h.Write(v.Raw)
		}
		h.Write([]byte{0xff})
	}
	return hex.EncodeToString(h.Sum(nil)[:12])
}

func conflictColumns(task *domain.MigrationTask, direction string, sourceCols, targetCols []domain.ColumnInfo) (domain.ColumnInfo, domain.ColumnInfo, error) {
	name := strings.TrimSpace(task.CDCConflictColumn)
	if name == "" {
		return domain.ColumnInfo{}, domain.ColumnInfo{}, errors.New("LAST_WRITE_WINS requires cdc_conflict_column")
	}
	if len(sourceCols) != len(targetCols) {
		return domain.ColumnInfo{}, domain.ColumnInfo{}, errors.New("source/target column mapping length mismatch")
	}
	if direction == "forward" {
		for i, col := range sourceCols {
			if strings.EqualFold(col.Name, name) {
				return col, targetCols[i], nil
			}
		}
	} else {
		// The task field is expressed in the forward source namespace. After
		// reverseMigrationTable, targetCols are the original source columns.
		for i, col := range targetCols {
			if strings.EqualFold(col.Name, name) {
				return sourceCols[i], col, nil
			}
		}
	}
	return domain.ColumnInfo{}, domain.ColumnInfo{}, fmt.Errorf("conflict column %s is not present in table mapping", name)
}

func makeConflictRecord(taskID, direction string, event domain.CDCEvent, keyValues []connector.Value, policy string, decision domain.CDCConflictDecision, sourceVersion, targetVersion string) domain.CDCConflictRecord {
	fingerprint := keyFingerprint(keyValues)
	h := sha256.Sum256([]byte(strings.Join([]string{taskID, direction, event.PositionType, event.PositionValue, event.ID, fingerprint, string(decision)}, "\x00")))
	return domain.CDCConflictRecord{
		ID:             "cfl_" + hex.EncodeToString(h[:12]),
		TaskID:         taskID,
		Direction:      direction,
		SourceSchema:   event.SourceSchema,
		SourceTable:    event.SourceTable,
		KeyFingerprint: fingerprint,
		Policy:         policy,
		Decision:       decision,
		SourceVersion:  sourceVersion,
		TargetVersion:  targetVersion,
		PositionType:   event.PositionType,
		PositionValue:  event.PositionValue,
		CreatedAt:      time.Now(),
	}
}

func (s *Service) CDCConflicts(ctx context.Context, taskID string) ([]domain.CDCConflictRecord, error) {
	return s.repo.ListCDCConflicts(ctx, taskID, 1000)
}

func valuesEqual(a, b []connector.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Null != b[i].Null || !bytes.Equal(a[i].Raw, b[i].Raw) {
			return false
		}
	}
	return true
}

func (s *Service) ApplyCDCEvents(ctx context.Context, id string, req domain.CDCApplyRequest) (*domain.CDCApplyResult, error) {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	direction := strings.ToLower(strings.TrimSpace(req.Direction))
	if direction == "" {
		direction = "forward"
	}
	if direction != "forward" && direction != "reverse" {
		return nil, fmt.Errorf("invalid CDC direction %q", direction)
	}
	if len(req.Events) == 0 {
		return nil, errors.New("CDC event batch is empty")
	}
	if req.Events[len(req.Events)-1].PositionValue == "" {
		return nil, errors.New("final CDC event must include a durable source position before any rows can be applied")
	}
	if direction == "forward" {
		if task.Status != domain.StatusCDCCatchingUp && task.Status != domain.StatusReadyCutover {
			return nil, fmt.Errorf("forward CDC apply is unsafe from %s; events must be buffered until full load is complete", task.Status)
		}
	} else if task.Status != domain.StatusRollbackSyncing && task.Status != domain.StatusRollbackReady {
		return nil, fmt.Errorf("reverse CDC apply is unsafe from %s", task.Status)
	}

	lastIncoming := req.Events[len(req.Events)-1]
	if positions, listErr := s.repo.ListCDCPositions(ctx, id, 50); listErr == nil {
		for _, p := range positions {
			if p.Direction != direction {
				continue
			}
			if strings.EqualFold(p.PositionType, lastIncoming.PositionType) && p.PositionValue == lastIncoming.PositionValue {
				if err := s.cleanupTransientCDCCommitFence(ctx, id, direction, lastIncoming); err != nil {
					return nil, fmt.Errorf("cleanup checkpointed CDC commit fence: %w", err)
				}
				return &domain.CDCApplyResult{Applied: 0, LastEventID: lastIncoming.ID, PositionType: p.PositionType, PositionValue: p.PositionValue, Duplicate: true}, nil
			}
			break
		}
	}
	if recovery, e := s.unresolvedCommitRecovery(ctx, id, direction); e != nil {
		return nil, fmt.Errorf("check unresolved CDC commit outcome: %w", e)
	} else if recovery != nil {
		bypassID, _ := ctx.Value(cdcReplayDLQContextKey{}).(string)
		if !(recovery.Status == domain.CDCDeadLetterReplayRequired && bypassID == recovery.ID) {
			return nil, &cdcBlockedByCommitUncertainError{dlqID: recovery.ID, positionType: recovery.PositionType, positionValue: recovery.PositionValue, status: recovery.Status}
		}
	}

	sourceDS, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, err
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, err
	}
	if direction == "reverse" {
		sourceDS, targetDS = targetDS, sourceDS
	}
	checkpointOnly := true
	for _, event := range req.Events {
		if event.Operation != domain.CDCCheckpoint {
			checkpointOnly = false
			break
		}
	}
	if checkpointOnly {
		result := &domain.CDCApplyResult{}
		last := req.Events[len(req.Events)-1]
		result.LastEventID = last.ID
		return s.persistCDCApplyResult(ctx, id, direction, sourceDS, result, last)
	}
	// Re-check the target capability at apply time, not only during planning.
	// Experimental connector gates are process-local and may change across a
	// Worker/Server restart; continuing to apply after a gate was removed would
	// bypass the operator's explicit qualification boundary. Checkpoint-only
	// windows deliberately do not need a target connector.
	if err := s.connectors.Require(targetDS.Type, connector.CapabilityCDCApply); err != nil {
		return nil, fmt.Errorf("CDC target %s: %w", targetDS.Type, err)
	}
	targetRaw, err := s.connectors.New(*targetDS)
	if err != nil {
		return nil, err
	}
	defer targetRaw.Close()

	ddlCount := 0
	for _, event := range req.Events {
		if event.Operation == domain.CDCDDL {
			ddlCount++
		}
	}
	if ddlCount > 0 {
		if ddlCount != len(req.Events) {
			return nil, errors.New("CDC DDL and row events cannot be applied in the same target transaction")
		}
		if strings.ToUpper(strings.TrimSpace(task.CDCDDLMode)) != "SAME_FAMILY" {
			return nil, errors.New("CDC DDL replay is disabled; set cdc_ddl_mode=SAME_FAMILY only after validating same-family identity mappings")
		}
		sameFamily := (sourceDS.Type.IsMySQLFamily() && targetDS.Type.IsMySQLFamily()) || (sourceDS.Type.IsPostgreSQLFamily() && targetDS.Type.IsPostgreSQLFamily()) || (sourceDS.Type == domain.DataSourceOracle && targetDS.Type == domain.DataSourceOracle) || (sourceDS.Type == domain.DataSourceGaussDB && targetDS.Type == domain.DataSourceGaussDB)
		if !sameFamily {
			return nil, fmt.Errorf("CDC DDL SAME_FAMILY cannot replay %s DDL on %s", sourceDS.Type, targetDS.Type)
		}
		tables, err := s.repo.ListMigrationTables(ctx, id)
		if err != nil {
			return nil, err
		}
		if err := cdcDDLIdentitySafe(tables, direction); err != nil {
			return nil, err
		}
		ddlTarget, ok := targetRaw.(connector.DDLApplyConnector)
		if !ok {
			return nil, fmt.Errorf("target connector %s does not support CDC DDL apply", targetDS.Type)
		}
		result := &domain.CDCApplyResult{}
		defaultSourceSchema := defaultSchema(*sourceDS)
		var last domain.CDCEvent
		for i, event := range req.Events {
			if strings.TrimSpace(event.SQL) == "" {
				return result, fmt.Errorf("CDC DDL event %d has empty SQL", i)
			}
			schema := event.SourceSchema
			if schema == "" {
				schema = defaultSourceSchema
			}
			if err := ddlTarget.ExecDDL(ctx, schema, event.SQL); err != nil {
				return result, fmt.Errorf("apply CDC DDL in schema %s: %w", schema, err)
			}
			result.Applied++
			result.LastEventID = event.ID
			last = event
		}
		return s.persistCDCApplyResult(ctx, id, direction, sourceDS, result, last)
	}
	target, ok := targetRaw.(connector.CDCApplyConnector)
	if !ok {
		return nil, fmt.Errorf("target connector %s does not support native CDC apply", targetDS.Type)
	}
	conflictMode := strings.ToUpper(strings.TrimSpace(task.CDCConflictMode))
	if conflictMode == "" {
		conflictMode = "SOURCE_WINS"
	}
	var pointLookup connector.PointLookupConnector
	if conflictMode == "LAST_WRITE_WINS" {
		lookup, ok := targetRaw.(connector.PointLookupConnector)
		if !ok {
			return nil, fmt.Errorf("target connector %s does not support LAST_WRITE_WINS point lookup", targetDS.Type)
		}
		pointLookup = lookup
	}
	var tx connector.TransactionalCDCApplyConnector
	txOpen := false
	if transactional, ok := targetRaw.(connector.TransactionalCDCApplyConnector); ok {
		tx = transactional
		if err := tx.BeginCDCTransaction(ctx); err != nil {
			return nil, fmt.Errorf("begin target CDC transaction: %w", err)
		}
		txOpen = true
		defer func() {
			if txOpen {
				_ = tx.RollbackCDCTransaction(context.Background())
			}
		}()
	}
	tables, err := s.repo.ListMigrationTables(ctx, id)
	if err != nil {
		return nil, err
	}
	byName := map[string]domain.MigrationTable{}
	for _, table := range tables {
		if direction == "reverse" {
			table = reverseMigrationTable(table)
		}
		byName[objectName(table.SourceSchema, table.SourceTable)] = table
	}
	defaultSourceSchema := defaultSchema(*sourceDS)
	result := &domain.CDCApplyResult{}
	pendingConflicts := make([]domain.CDCConflictRecord, 0)
	var last domain.CDCEvent
	for index, event := range req.Events {
		schema := event.SourceSchema
		if schema == "" {
			schema = defaultSourceSchema
		}
		table, ok := byName[objectName(schema, event.SourceTable)]
		if !ok {
			return result, fmt.Errorf("CDC event %d references unmapped table %s", index, objectName(schema, event.SourceTable))
		}
		sourceCols := table.Columns
		targetCols := table.TargetColumns
		if len(targetCols) == 0 {
			targetCols = sourceCols
		}
		if len(sourceCols) != len(targetCols) {
			return result, fmt.Errorf("CDC table mapping %s has %d source columns but %d target columns", objectName(schema, event.SourceTable), len(sourceCols), len(targetCols))
		}
		sourcePKs := append([]string(nil), table.PrimaryKeys...)
		if len(sourcePKs) == 0 && table.PrimaryKey != "" {
			sourcePKs = []string{table.PrimaryKey}
		}
		targetPKs := append([]string(nil), table.TargetPrimaryKeys...)
		if len(targetPKs) == 0 && table.TargetPrimaryKey != "" {
			targetPKs = []string{table.TargetPrimaryKey}
		}
		if len(targetPKs) == 0 {
			targetPKs = append([]string(nil), sourcePKs...)
		}
		if len(sourcePKs) != len(targetPKs) {
			return result, fmt.Errorf("CDC primary-key mapping %s has %d source keys but %d target keys", objectName(schema, event.SourceTable), len(sourcePKs), len(targetPKs))
		}
		targetPK := ""
		if len(targetPKs) > 0 {
			targetPK = targetPKs[0]
		}
		switch event.Operation {
		case domain.CDCInsert, domain.CDCUpdate:
			after := cdcFieldMap(event.After)
			row := make([]connector.Value, len(sourceCols))
			for i, col := range sourceCols {
				field, exists := after[col.Name]
				if !exists {
					return result, fmt.Errorf("CDC %s event for %s is missing after-image column %s; QMigration native apply requires full row images", event.Operation, objectName(schema, event.SourceTable), col.Name)
				}
				v, e := decodeCDCValue(field)
				if e != nil {
					return result, e
				}
				row[i] = v
			}
			var newKeyValues []connector.Value
			var keyCols []domain.ColumnInfo
			if len(sourcePKs) > 0 {
				newKeyValues, keyCols, err = cdcKeyValues(after, sourcePKs, targetPKs, targetCols)
				if err != nil {
					return result, fmt.Errorf("CDC %s primary key for %s: %w", event.Operation, objectName(schema, event.SourceTable), err)
				}
			}

			applySource := true
			if conflictMode == "LAST_WRITE_WINS" {
				if len(sourcePKs) == 0 {
					return result, fmt.Errorf("LAST_WRITE_WINS for %s requires a primary key", objectName(schema, event.SourceTable))
				}
				sourceVersionCol, targetVersionCol, e := conflictColumns(task, direction, sourceCols, targetCols)
				if e != nil {
					return result, fmt.Errorf("LAST_WRITE_WINS for %s: %w", objectName(schema, event.SourceTable), e)
				}
				versionField, exists := after[sourceVersionCol.Name]
				if !exists {
					return result, fmt.Errorf("LAST_WRITE_WINS event for %s is missing conflict column %s in after image", objectName(schema, event.SourceTable), sourceVersionCol.Name)
				}
				sourceVersionValue, e := decodeCDCValue(versionField)
				if e != nil {
					return result, e
				}
				if sourceVersionValue.Null {
					return result, fmt.Errorf("LAST_WRITE_WINS conflict column %s cannot be NULL", sourceVersionCol.Name)
				}
				sourceVersion := string(sourceVersionValue.Raw)
				current, exists, e := pointLookup.ReadByKey(ctx, connector.ReadByKeyRequest{
					Schema:      table.TargetSchema,
					Table:       table.TargetTable,
					PrimaryKeys: targetPKs,
					KeyColumns:  keyCols,
					KeyValues:   newKeyValues,
					Columns:     []domain.ColumnInfo{targetVersionCol},
				})
				if e != nil {
					return result, fmt.Errorf("LAST_WRITE_WINS lookup %s: %w", objectName(table.TargetSchema, table.TargetTable), e)
				}
				if exists && len(current) == 1 && !current[0].Null {
					targetVersion := string(current[0].Raw)
					if compareConflictVersions(sourceVersion, targetVersion) <= 0 {
						pendingConflicts = append(pendingConflicts, makeConflictRecord(id, direction, event, newKeyValues, conflictMode, domain.CDCConflictTargetKept, sourceVersion, targetVersion))
						result.SkippedConflicts++
						applySource = false
					} else {
						pendingConflicts = append(pendingConflicts, makeConflictRecord(id, direction, event, newKeyValues, conflictMode, domain.CDCConflictSourceApplied, sourceVersion, targetVersion))
					}
				}
			}
			if !applySource {
				result.LastEventID = event.ID
				last = event
				continue
			}

			// UPDATE can move a row to a different primary key. Upserting the
			// after image alone would leave the old target row behind, so delete
			// the mapped before-key inside the same target transaction first.
			if event.Operation == domain.CDCUpdate && len(sourcePKs) > 0 {
				before := cdcFieldMap(event.Before)
				hasAllOldKeys := true
				for _, key := range sourcePKs {
					if _, exists := before[key]; !exists {
						hasAllOldKeys = false
						break
					}
				}
				if hasAllOldKeys {
					oldKeyValues, oldKeyCols, e := cdcKeyValues(before, sourcePKs, targetPKs, targetCols)
					if e != nil {
						return result, fmt.Errorf("CDC UPDATE old primary key for %s: %w", objectName(schema, event.SourceTable), e)
					}
					if !valuesEqual(oldKeyValues, newKeyValues) {
						del := connector.DeleteByKeyRequest{Schema: table.TargetSchema, Table: table.TargetTable, PrimaryKeys: targetPKs, Columns: oldKeyCols, Values: oldKeyValues}
						if len(targetPKs) == 1 {
							del.PrimaryKey, del.Column, del.Value = targetPKs[0], oldKeyCols[0], oldKeyValues[0]
						}
						if e := target.DeleteByKey(ctx, del); e != nil {
							return result, fmt.Errorf("delete old primary key before CDC UPDATE on %s: %w", objectName(table.TargetSchema, table.TargetTable), e)
						}
					}
				}
			}
			if _, e := target.WriteBatch(ctx, connector.WriteBatchRequest{Schema: table.TargetSchema, Table: table.TargetTable, Columns: targetCols, Rows: [][]connector.Value{row}, PrimaryKey: targetPK, PrimaryKeys: targetPKs}); e != nil {
				return result, fmt.Errorf("apply CDC %s to %s: %w", event.Operation, objectName(table.TargetSchema, table.TargetTable), e)
			}
		case domain.CDCDelete:
			if len(sourcePKs) == 0 {
				return result, fmt.Errorf("CDC DELETE for %s requires a mapped primary key", objectName(schema, event.SourceTable))
			}
			before := cdcFieldMap(event.Before)
			after := cdcFieldMap(event.After)
			values := make([]connector.Value, len(sourcePKs))
			pkCols := make([]domain.ColumnInfo, len(targetPKs))
			for pi := range sourcePKs {
				field, exists := before[sourcePKs[pi]]
				if !exists {
					field, exists = after[sourcePKs[pi]]
				}
				if !exists {
					return result, fmt.Errorf("CDC DELETE for %s is missing primary key field %s", objectName(schema, event.SourceTable), sourcePKs[pi])
				}
				value, e := decodeCDCValue(field)
				if e != nil {
					return result, e
				}
				values[pi] = value
				found := false
				for ci := range targetCols {
					if strings.EqualFold(targetCols[ci].Name, targetPKs[pi]) {
						pkCols[pi] = targetCols[ci]
						found = true
						break
					}
				}
				if !found {
					return result, fmt.Errorf("target primary key %s is not present in mapped columns", targetPKs[pi])
				}
			}
			del := connector.DeleteByKeyRequest{Schema: table.TargetSchema, Table: table.TargetTable, PrimaryKeys: targetPKs, Columns: pkCols, Values: values}
			if len(targetPKs) == 1 {
				del.PrimaryKey, del.Column, del.Value = targetPKs[0], pkCols[0], values[0]
			}
			if e := target.DeleteByKey(ctx, del); e != nil {
				return result, fmt.Errorf("apply CDC DELETE to %s: %w", objectName(table.TargetSchema, table.TargetTable), e)
			}
		case domain.CDCTruncate:
			if tx == nil || !txOpen {
				return result, fmt.Errorf("CDC TRUNCATE for %s requires transactional target apply", objectName(table.TargetSchema, table.TargetTable))
			}
			truncater, ok := targetRaw.(connector.TruncateTableConnector)
			if !ok {
				return result, fmt.Errorf("target connector %s does not support transactional CDC TRUNCATE", targetDS.Type)
			}
			if e := truncater.TruncateTable(ctx, table.TargetSchema, table.TargetTable); e != nil {
				return result, fmt.Errorf("apply CDC TRUNCATE to %s: %w", objectName(table.TargetSchema, table.TargetTable), e)
			}
			if index != len(req.Events)-1 {
				return result, fmt.Errorf("CDC TRUNCATE for %s must be the final event in its source transaction", objectName(schema, event.SourceTable))
			}
		default:
			return result, fmt.Errorf("unsupported CDC operation %q", event.Operation)
		}
		result.Applied++
		result.LastEventID = event.ID
		last = event
	}
	var commitFence *cdcCommitFence
	if txOpen {
		commitFence, err = s.beginCDCCommitFence(ctx, id, direction, req)
		if err != nil {
			return result, fmt.Errorf("persist pre-COMMIT CDC ambiguity fence: %w", err)
		}
		if err := tx.CommitCDCTransaction(ctx); err != nil {
			// Once COMMIT has been sent, an error cannot prove whether the target
			// committed and only the response was lost. Never issue an automatic
			// rollback/replay here: both can duplicate trigger/side-effect behavior.
			txOpen = false
			return result, &cdcCommitOutcomeUnknownError{cause: fmt.Errorf("commit target CDC transaction: %w", err)}
		}
		txOpen = false
	}
	for i := range pendingConflicts {
		if err := s.repo.CreateCDCConflict(ctx, &pendingConflicts[i]); err != nil {
			return result, fmt.Errorf("persist CDC conflict decision after target commit: %w", err)
		}
	}
	result, err = s.persistCDCApplyResult(ctx, id, direction, sourceDS, result, last)
	if err != nil {
		// persistCDCApplyResult sets the returned durable position only after
		// checkpoint persistence succeeds. Therefore an error from the synthetic
		// after-checkpoint/before-source-ACK window can safely close the pre-COMMIT
		// fence before returning: redelivery is already duplicate-safe.
		if commitFence != nil && result.PositionValue == last.PositionValue && result.PositionValue != "" {
			if fenceErr := s.completeCDCCommitFence(ctx, commitFence); fenceErr != nil {
				return result, fmt.Errorf("%v; additionally complete durable CDC commit fence: %w", err, fenceErr)
			}
		}
		return result, err
	}
	if err := s.completeCDCCommitFence(ctx, commitFence); err != nil {
		return result, fmt.Errorf("complete durable CDC commit fence: %w", err)
	}
	return result, nil
}

func (s *Service) maybeAlertCDCLag(ctx context.Context, taskID, direction string, lagMS int64) {
	threshold := int64(60000)
	if raw := os.Getenv("QMIGRATION_CDC_LAG_ALERT_MS"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			threshold = n
		}
	}
	if threshold <= 0 || lagMS <= threshold {
		return
	}
	title := "CDC lag high"
	if direction == "reverse" {
		title = "Reverse CDC lag high"
	}
	alerts, _ := s.repo.ListAlerts(ctx)
	cutoff := time.Now().Add(-15 * time.Minute)
	for _, a := range alerts {
		if a.TaskID == taskID && a.Title == title && !a.Acknowledged && a.CreatedAt.After(cutoff) {
			return
		}
	}
	_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "warning", Title: title, Message: fmt.Sprintf("%s CDC lag is %dms (threshold %dms)", direction, lagMS, threshold), TaskID: taskID, CreatedAt: time.Now()})
}

func (s *Service) RecordCDCProgress(ctx context.Context, id string, p *domain.CDCPosition) error {
	return s.recordCDCProgress(ctx, id, p, true)
}

// recordCDCProgress persists the target-applied forward checkpoint. Internal
// target-apply paths pass triggerValidation=false because they already hold the
// per-task CDC spool/apply lock; their outer operation triggers validation only
// after releasing that lock. This avoids mutex re-entry while preserving the
// atomic barrier-vs-apply contract.
func (s *Service) recordCDCProgress(ctx context.Context, id string, p *domain.CDCPosition, triggerValidation bool) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusCDCCatchingUp && task.Status != domain.StatusReadyCutover {
		return fmt.Errorf("task cannot accept CDC progress from %s", task.Status)
	}
	p.ID = newID("cdc")
	p.TaskID = id
	p.Direction = "forward"
	p.RecordedAt = time.Now()
	previousLag := task.CDCLagMS
	task.CDCLagMS = p.LagMS
	task.UpdatedAt = time.Now()
	warnLag := int64(intEnv("QMIGRATION_CDC_LAG_WARN_MS", 30000))
	if p.LagMS >= warnLag && previousLag < warnLag {
		_ = s.repo.CreateAlert(ctx, &domain.Alert{ID: newID("alt"), Severity: "warning", Title: "CDC lag high", Message: fmt.Sprintf("CDC lag %dms exceeds warning threshold %dms", p.LagMS, warnLag), TaskID: id, CreatedAt: time.Now()})
		s.logTask(ctx, id, "", "", "", "WARN", fmt.Sprintf("CDC lag high: %dms", p.LagMS))
	}
	if err := s.repo.CreateCDCPosition(ctx, p); err != nil {
		return err
	}
	s.maybeAlertCDCLag(ctx, id, "forward", p.LagMS)
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	if triggerValidation {
		s.maybeStartValidationAfterCatchup(ctx, id)
	}
	return nil
}
func (s *Service) CDCPositions(ctx context.Context, id string) ([]domain.CDCPosition, error) {
	return s.repo.ListCDCPositions(ctx, id, 200)
}
func sequenceSyncFreshness() time.Duration {
	seconds := int64(60)
	if raw := strings.TrimSpace(os.Getenv("QMIGRATION_SEQUENCE_SYNC_MAX_AGE_SECONDS")); raw != "" {
		if value, err := strconv.ParseInt(raw, 10, 64); err == nil && value > 0 && value <= 3600 {
			seconds = value
		}
	}
	return time.Duration(seconds) * time.Second
}

func (s *Service) requireFreshSequenceSync(ctx context.Context, task *domain.MigrationTask) error {
	sourceDS, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return err
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return err
	}
	if !sourceDS.Type.IsPostgreSQLFamily() || !targetDS.Type.IsPostgreSQLFamily() {
		return nil
	}
	plan, err := s.PlanSchemaObjects(ctx, task.ID)
	if err != nil {
		return fmt.Errorf("sequence cutover assessment: %w", err)
	}
	needsSync := false
	for _, item := range plan.Items {
		if item.Action == domain.SchemaObjectSyncSequence {
			needsSync = true
			break
		}
	}
	if !needsSync {
		return nil
	}
	if task.SequenceSyncedAt.IsZero() {
		return errors.New("PostgreSQL sequences have not been synchronized; apply safe schema objects immediately before cutover")
	}
	maxAge := sequenceSyncFreshness()
	age := time.Since(task.SequenceSyncedAt)
	if age < 0 {
		age = 0
	}
	if age > maxAge {
		return fmt.Errorf("PostgreSQL sequence synchronization is stale (%s old, max %s); re-apply safe schema objects before cutover", age.Round(time.Second), maxAge)
	}
	return nil
}

func (s *Service) ReadyForCutover(ctx context.Context, id string, maxLagMS int64) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusCDCCatchingUp {
		return fmt.Errorf("task cannot enter cutover from %s", task.Status)
	}
	if maxLagMS <= 0 {
		maxLagMS = 5000
	}
	if task.CDCLagMS > maxLagMS {
		return fmt.Errorf("cdc lag %dms exceeds threshold %dms", task.CDCLagMS, maxLagMS)
	}
	spool, err := s.repo.CDCSpoolStats(ctx, id, "forward")
	if err != nil {
		return err
	}
	if spool.PendingTransactions > 0 {
		return fmt.Errorf("cdc spool still has %d pending transactions (%d events), first=%s last=%s", spool.PendingTransactions, spool.PendingEvents, spool.FirstPosition, spool.LastPosition)
	}
	latest, err := s.latestCDC(ctx, id, "forward")
	if err != nil {
		return err
	}
	if latest == nil {
		return errors.New("no forward CDC position has been reported")
	}
	if latest.PositionType == "EXTERNAL_MANAGED" && strings.HasPrefix(latest.PositionValue, "pending external") {
		return errors.New("external CDC engine has not reported an active source position")
	}
	if latest.EventsPending > 0 {
		return fmt.Errorf("cdc still has %d pending events", latest.EventsPending)
	}
	if latest.LagMS > maxLagMS {
		return fmt.Errorf("latest cdc lag %dms exceeds threshold %dms", latest.LagMS, maxLagMS)
	}
	if task.ValidationEnabled {
		if err := s.requireSuccessfulValidation(ctx, task); err != nil {
			return err
		}
	}
	if err := s.requireFreshSequenceSync(ctx, task); err != nil {
		return err
	}
	if err := Transition(task, domain.StatusReadyCutover); err != nil {
		return err
	}
	task.UpdatedAt = time.Now()
	return s.repo.UpdateMigration(ctx, task)
}

func (s *Service) latestCDC(ctx context.Context, taskID, direction string) (*domain.CDCPosition, error) {
	items, err := s.repo.ListCDCPositions(ctx, taskID, 200)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].Direction == direction || (direction == "forward" && items[i].Direction == "") {
			v := items[i]
			return &v, nil
		}
	}
	return nil, nil
}

func (s *Service) requireSuccessfulValidation(ctx context.Context, task *domain.MigrationTask) error {
	chunkID, status, err := repository.FirstInvalidSuccessfulChunk(ctx, s.repo, task.ID)
	if err != nil {
		return err
	}
	if chunkID == "" {
		return nil
	}
	if status == "" {
		return fmt.Errorf("validation result is missing for chunk %s", chunkID)
	}
	return fmt.Errorf("validation for chunk %s is %s", chunkID, status)
}

func cutoverEngineStopTimeout() time.Duration {
	seconds := envInt64("QMIGRATION_CUTOVER_ENGINE_STOP_TIMEOUT_SECONDS", 60)
	if seconds < 5 {
		seconds = 5
	}
	if seconds > 600 {
		seconds = 600
	}
	return time.Duration(seconds) * time.Second
}

func (s *Service) waitEngineJobsStopped(ctx context.Context, taskID, direction string) error {
	deadline := time.Now().Add(cutoverEngineStopTimeout())
	for {
		jobs, err := s.repo.ListEngineJobs(ctx, taskID)
		if err != nil {
			return err
		}
		active := 0
		for _, job := range jobs {
			if job.Kind != "CDC" || job.Direction != direction {
				continue
			}
			switch job.Status {
			case domain.EngineJobFailed:
				return fmt.Errorf("CDC engine job %s failed while stopping for cutover: %s", job.ID, job.LastError)
			case domain.EngineJobPending, domain.EngineJobRunning, domain.EngineJobStopRequested:
				active++
			}
		}
		if active == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out stopping %d forward CDC engine job(s) before cutover", active)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *Service) drainFinalCutoverSpool(ctx context.Context, taskID string) error {
	for pass := 0; pass < 100; pass++ {
		stats, err := s.DrainCDCSpool(ctx, taskID, "forward", 10000)
		if err != nil {
			return err
		}
		if stats.PendingTransactions == 0 && stats.PendingEvents == 0 {
			return nil
		}
	}
	stats, err := s.repo.CDCSpoolStats(ctx, taskID, "forward")
	if err != nil {
		return err
	}
	return fmt.Errorf("final cutover CDC spool did not drain: %d transactions / %d events remain", stats.PendingTransactions, stats.PendingEvents)
}

func (s *Service) Cutover(ctx context.Context, id string) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusReadyCutover {
		return fmt.Errorf("task cannot cut over from %s", task.Status)
	}

	// Freeze managed native capture first while the task is still READY_CUTOVER,
	// so any final transactions already staged in the durable spool can still be
	// applied safely. External CDC users are responsible for quiescing their
	// source before invoking cutover, as before.
	if err := s.requestStopEngineJobs(ctx, id, "forward"); err != nil {
		return fmt.Errorf("request forward CDC stop for cutover: %w", err)
	}
	if err := s.waitEngineJobsStopped(ctx, id, "forward"); err != nil {
		return err
	}
	if err := s.drainFinalCutoverSpool(ctx, id); err != nil {
		return fmt.Errorf("drain final CDC spool for cutover: %w", err)
	}

	// Once CUTOVER_RUNNING is durable, new ApplyCDCEvents calls are rejected.
	// This forms the connector-finalization critical section.
	if err := Transition(task, domain.StatusCutoverRunning); err != nil {
		return err
	}
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}

	tables, err := s.repo.ListMigrationTables(ctx, id)
	if err != nil {
		return err
	}
	// Re-sync after the last drained CDC transaction, then restore production
	// generated-column modes (for Db2 LUW this changes migration-stage BY
	// DEFAULT identity columns back to their original GENERATED ALWAYS mode).
	if err := s.syncGeneratedValueState(ctx, task, tables); err != nil {
		task.Status = domain.StatusFailed
		task.LastError = "cutover generated-value state synchronization failed: " + err.Error()
		_ = s.repo.UpdateMigration(ctx, task)
		return errors.New(task.LastError)
	}
	if err := s.finalizeGeneratedValueModes(ctx, task, tables); err != nil {
		task.Status = domain.StatusFailed
		task.LastError = "cutover generated-value mode finalization failed: " + err.Error()
		_ = s.repo.UpdateMigration(ctx, task)
		return errors.New(task.LastError)
	}

	if err := Transition(task, domain.StatusFinished); err != nil {
		return err
	}
	task.Progress = 100
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	s.archiveValidationBestEffort(ctx, task.ID)
	return nil
}

func (s *Service) RepairValidation(ctx context.Context, id string) (int, error) {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return 0, err
	}
	if p, ok := s.repo.(repository.ValidationArchiveProvider); ok {
		if archive, e := p.GetValidationArchive(ctx, id); e != nil {
			return 0, e
		} else if archive != nil {
			return 0, errors.New("validation evidence is sealed in an immutable archive; repair is no longer available")
		}
	}
	bad, err := repository.ListRepairableValidationChunkIDs(ctx, s.repo, id, 100000)
	if err != nil {
		return 0, err
	}
	if len(bad) == 0 {
		return 0, errors.New("no mismatched validation chunks")
	}
	reset := 0
	for _, cid := range bad {
		c, e := s.repo.GetChunk(ctx, cid)
		if e != nil {
			continue
		}
		c.Status = domain.ChunkPending
		c.WorkerID = ""
		c.LeaseUntil = time.Time{}
		c.LastError = "repair scheduled after validation mismatch"
		c.FinishedAt = time.Time{}
		if e = s.repo.UpdateChunk(ctx, c); e == nil {
			reset++
		}
	}
	task.Status = domain.StatusFullMigrating
	task.LastError = ""
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return reset, err
	}
	return reset, nil
}

// Resume delegates to Start so there is exactly one paused-task recovery path.
// Start restores PausedFromStatus and restarts managed CDC/validation runners when
// required; keeping a second state transition here previously forced CDC and
// rollback tasks back into FULL_MIGRATING.
func (s *Service) Resume(ctx context.Context, id string) error {
	return s.Start(ctx, id)
}

func (s *Service) cdcEngineForDirection(task *domain.MigrationTask, _ string) string {
	if task == nil || task.Mode == domain.ModeFull {
		return ""
	}
	return unifiedEngineName
}

func (s *Service) chooseRollbackCDCEngine(_ *domain.MigrationTask, reverseSource domain.DataSource) string {
	if s.connectors == nil {
		// Keep unit-test/backward-compatible behavior when a Service is created
		// without a registry; production Services always carry the registry.
		if reverseSource.Type.IsMySQLFamily() || reverseSource.Type.IsPostgreSQLFamily() {
			return unifiedEngineName
		}
		return ""
	}
	if s.connectors.Require(reverseSource.Type, connector.CapabilityCDCRead) == nil {
		return unifiedEngineName
	}
	return ""
}

// PrepareRollback records the current position on the post-cutover target. An external
// reverse CDC engine can then start at this exact position and stream target -> source.
func (s *Service) PrepareRollback(ctx context.Context, id string) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusFinished {
		return fmt.Errorf("rollback can only be prepared after cutover; current status=%s", task.Status)
	}
	if err := Transition(task, domain.StatusRollbackPreparing); err != nil {
		return err
	}
	targetDS, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return err
	}
	rollbackEngine := s.chooseRollbackCDCEngine(task, *targetDS)
	if rollbackEngine == "" {
		return errors.New("no reverse CDC engine is available for rollback")
	}
	task.RollbackCDCEngine = rollbackEngine
	target, err := s.connectors.New(*targetDS)
	if err != nil {
		return err
	}
	defer target.Close()
	var pos *domain.CDCPosition
	if selectedSource, ok := target.(connector.CDCSelectionPositionSource); ok {
		tables, listErr := s.repo.ListMigrationTables(ctx, task.ID)
		if listErr != nil {
			return listErr
		}
		mappings := make([]domain.TableMapping, 0, len(tables))
		for _, t := range tables {
			mappings = append(mappings, domain.TableMapping{SourceSchema: t.TargetSchema, SourceTable: t.TargetTable, TargetSchema: t.SourceSchema, TargetTable: t.SourceTable})
		}
		pos, err = selectedSource.CurrentCDCPositionForSelection(ctx, mappings)
		if err != nil {
			return fmt.Errorf("capture reverse selected-table CDC start position: %w", err)
		}
	} else if mysqlSource, ok := target.(connector.MySQLBinlogSource); ok && targetDS.Type.IsMySQLFamily() && targetDS.Type != domain.DataSourceTiDB && targetDS.Type != domain.DataSourceOceanBase {
		pos, err = mysqlSource.CurrentCDCPosition(ctx)
		if err != nil {
			return fmt.Errorf("capture reverse native MySQL CDC start position: %w", err)
		}
	} else if checkpoint, ok := target.(connector.CDCCheckpointSource); ok && targetDS.Type.IsPostgreSQLFamily() {
		slot := strings.ToLower(strings.ReplaceAll("qmigration_rb_"+task.ID, "-", "_"))
		if len(slot) > 63 {
			slot = slot[:63]
		}
		pos, err = checkpoint.CreateCDCCheckpoint(ctx, slot)
		if err != nil {
			return fmt.Errorf("create reverse PostgreSQL CDC replication slot: %w", err)
		}
	} else if cdc, ok := target.(connector.CDCSource); ok {
		pos, err = cdc.CurrentCDCPosition(ctx)
		if err != nil {
			return fmt.Errorf("capture reverse CDC start position: %w", err)
		}
	} else {
		return fmt.Errorf("target connector %s cannot provide a QMigration reverse CDC start position", targetDS.Type)
	}
	pos.ID = newID("cdc")
	pos.TaskID = id
	pos.Direction = "reverse"
	pos.RecordedAt = time.Now()
	if err := s.repo.CreateCDCPosition(ctx, pos); err != nil {
		return err
	}
	task.CDCLagMS = 0
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	return s.ensureManagedCDCJob(ctx, task, "reverse")
}

func (s *Service) MarkRollbackCDCStarted(ctx context.Context, id string, p *domain.CDCPosition) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusRollbackPreparing {
		return fmt.Errorf("reverse CDC cannot start from %s", task.Status)
	}
	if p != nil && (p.PositionValue != "" || p.LagMS != 0 || p.EventsTotal != 0) {
		p.ID = newID("cdc")
		p.TaskID = id
		p.Direction = "reverse"
		p.RecordedAt = time.Now()
		if err := s.repo.CreateCDCPosition(ctx, p); err != nil {
			return err
		}
	}
	if err := Transition(task, domain.StatusRollbackSyncing); err != nil {
		return err
	}
	task.UpdatedAt = time.Now()
	return s.repo.UpdateMigration(ctx, task)
}

func (s *Service) RecordRollbackCDCProgress(ctx context.Context, id string, p *domain.CDCPosition) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusRollbackSyncing && task.Status != domain.StatusRollbackReady {
		return fmt.Errorf("task cannot accept reverse CDC progress from %s", task.Status)
	}
	p.ID = newID("cdc")
	p.TaskID = id
	p.Direction = "reverse"
	p.RecordedAt = time.Now()
	task.CDCLagMS = p.LagMS
	task.UpdatedAt = time.Now()
	if err := s.repo.CreateCDCPosition(ctx, p); err != nil {
		return err
	}
	s.maybeAlertCDCLag(ctx, id, "reverse", p.LagMS)
	return s.repo.UpdateMigration(ctx, task)
}

func (s *Service) ReadyForRollback(ctx context.Context, id string, maxLagMS int64) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusRollbackSyncing {
		return fmt.Errorf("task cannot enter rollback-ready from %s", task.Status)
	}
	if maxLagMS <= 0 {
		maxLagMS = 5000
	}
	if task.CDCLagMS > maxLagMS {
		return fmt.Errorf("reverse cdc lag %dms exceeds threshold %dms", task.CDCLagMS, maxLagMS)
	}
	spool, err := s.repo.CDCSpoolStats(ctx, id, "reverse")
	if err != nil {
		return err
	}
	if spool.PendingTransactions > 0 {
		return fmt.Errorf("reverse cdc spool still has %d pending transactions", spool.PendingTransactions)
	}
	latest, err := s.latestCDC(ctx, id, "reverse")
	if err != nil {
		return err
	}
	if latest == nil {
		return errors.New("no reverse CDC position has been reported")
	}
	if latest.PositionType == "EXTERNAL_MANAGED" && strings.HasPrefix(latest.PositionValue, "pending external") {
		return errors.New("external reverse CDC engine has not reported an active source position")
	}
	if latest.EventsPending > 0 {
		return fmt.Errorf("reverse cdc still has %d pending events", latest.EventsPending)
	}
	if latest.LagMS > maxLagMS {
		return fmt.Errorf("latest reverse cdc lag %dms exceeds threshold %dms", latest.LagMS, maxLagMS)
	}
	if err := Transition(task, domain.StatusRollbackReady); err != nil {
		return err
	}
	task.UpdatedAt = time.Now()
	return s.repo.UpdateMigration(ctx, task)
}

func (s *Service) Rollback(ctx context.Context, id string) error {
	task, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if task.Status != domain.StatusRollbackReady {
		return fmt.Errorf("task cannot roll back from %s", task.Status)
	}
	if err := Transition(task, domain.StatusRollbackRunning); err != nil {
		return err
	}
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	if err := Transition(task, domain.StatusRolledBack); err != nil {
		return err
	}
	task.UpdatedAt = time.Now()
	if err := s.repo.UpdateMigration(ctx, task); err != nil {
		return err
	}
	s.archiveValidationBestEffort(ctx, task.ID)
	_ = s.requestStopEngineJobs(ctx, id, "reverse")
	return nil
}

const engineJobLease = 5 * time.Minute

func (s *Service) managedCDCAdapter(_ string) (engine.Adapter, bool) {
	if s.engines != nil {
		if a, ok := s.engines.Get(unifiedEngineName); ok {
			return a, true
		}
	}
	// The unified engine is built in, so managed CDC must not depend on an
	// optional external-engine registry being populated.
	return engine.NewUnified(), true
}

func (s *Service) ensureManagedCDCJob(ctx context.Context, task *domain.MigrationTask, direction string) error {
	if task == nil {
		return errors.New("nil migration task")
	}
	cdcEngine := s.cdcEngineForDirection(task, direction)
	if _, ok := s.managedCDCAdapter(cdcEngine); !ok {
		return nil
	}
	jobs, err := s.repo.ListEngineJobs(ctx, task.ID)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Kind == "CDC" && j.Direction == direction && j.Engine == cdcEngine && j.Status != domain.EngineJobStopped && j.Status != domain.EngineJobFailed {
			return nil
		}
	}
	now := time.Now()
	j := &domain.EngineJob{ID: newID("eng"), TaskID: task.ID, Kind: "CDC", Direction: direction, Engine: cdcEngine, Status: domain.EngineJobPending, UpdatedAt: now}
	return s.repo.CreateEngineJob(ctx, j)
}

func reverseMigrationTables(tables []domain.MigrationTable) []domain.MigrationTable {
	out := make([]domain.MigrationTable, 0, len(tables))
	for _, t := range tables {
		r := t
		r.SourceSchema, r.TargetSchema = t.TargetSchema, t.SourceSchema
		r.SourceTable, r.TargetTable = t.TargetTable, t.SourceTable
		r.PrimaryKey, r.TargetPrimaryKey = t.TargetPrimaryKey, t.PrimaryKey
		r.PrimaryKeys, r.TargetPrimaryKeys = append([]string(nil), t.TargetPrimaryKeys...), append([]string(nil), t.PrimaryKeys...)
		r.Columns, r.TargetColumns = t.TargetColumns, t.Columns
		out = append(out, r)
	}
	return out
}

func (s *Service) cdcRenderTask(ctx context.Context, task *domain.MigrationTask, direction string) (*domain.MigrationTask, domain.DataSource, domain.DataSource, []domain.MigrationTable, error) {
	src, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, domain.DataSource{}, domain.DataSource{}, nil, err
	}
	dst, err := s.repo.GetDataSource(ctx, task.TargetID)
	if err != nil {
		return nil, domain.DataSource{}, domain.DataSource{}, nil, err
	}
	tables, err := s.repo.ListMigrationTables(ctx, task.ID)
	if err != nil {
		return nil, domain.DataSource{}, domain.DataSource{}, nil, err
	}
	copyTask := *task
	copyTask.Mode = domain.ModeIncremental
	copyTask.CDCEngine = s.cdcEngineForDirection(task, direction)
	if direction == "reverse" {
		src, dst = dst, src
		tables = reverseMigrationTables(tables)
	}
	// Managed CDC jobs may be reclaimed by another Worker after a crash. Render
	// from the newest durable apply checkpoint, not always the original task
	// start, so native GTID/LSN/file-position readers resume close to the last
	// committed target transaction. Replaying from the initial start is safe but
	// can be prohibitively expensive after a long-running migration.
	if latest, e := s.latestCDC(ctx, task.ID, direction); e == nil && latest != nil &&
		strings.TrimSpace(latest.PositionValue) != "" && latest.PositionType != "EXTERNAL_MANAGED" {
		copyTask.CDCStartTimestampMS = latest.SourceTimestampMS
		copyTask.CDCStartPositionType = latest.PositionType
		copyTask.CDCStartPositionValue = latest.PositionValue
		copyTask.CDCStartResource = latest.Resource
	}
	// A source transaction may already have been ACKed after it was durably
	// staged, while target apply is still waiting for full snapshot completion.
	// On Worker failover resume from the newest pending spool position instead
	// of the older target-apply checkpoint, otherwise source retention gains are
	// lost and the same already-spooled range is decoded again.
	if staged, e := s.repo.LatestPendingCDCSpool(ctx, task.ID, direction); e == nil && staged != nil && strings.TrimSpace(staged.PositionValue) != "" {
		copyTask.CDCStartTimestampMS = staged.SourceTimestampMS
		copyTask.CDCStartPositionType = staged.PositionType
		copyTask.CDCStartPositionValue = staged.PositionValue
		copyTask.CDCStartResource = staged.Resource
	}
	return &copyTask, *src, *dst, tables, nil
}

func (s *Service) ListEngineJobs(ctx context.Context, taskID string) ([]domain.EngineJob, error) {
	return s.repo.ListEngineJobs(ctx, taskID)
}

func (s *Service) ClaimEngineJob(ctx context.Context, workerID string) (*domain.EngineJobClaim, error) {
	worker, err := s.repo.GetWorker(ctx, workerID)
	if err != nil {
		return nil, err
	}
	if workerOverloaded(worker) {
		return nil, repository.ErrNoChunk
	}
	job, err := s.repo.ClaimEngineJob(ctx, workerID, engineJobLease, worker.Capabilities)
	if err != nil {
		return nil, err
	}
	task, err := s.repo.GetMigration(ctx, job.TaskID)
	if err != nil {
		return nil, err
	}
	a, ok := s.managedCDCAdapter(job.Engine)
	if !ok {
		return nil, fmt.Errorf("engine %s is not a managed CDC adapter", job.Engine)
	}
	renderTask, src, dst, tables, err := s.cdcRenderTask(ctx, task, job.Direction)
	if err != nil {
		return nil, err
	}
	cfg, err := a.Render(ctx, renderTask, src, dst, tables)
	if err != nil {
		return nil, err
	}
	return &domain.EngineJobClaim{Job: *job, RuntimeConfig: *cfg}, nil
}

func (s *Service) StartEngineJob(ctx context.Context, workerID, jobID string) error {
	j, err := s.repo.GetEngineJob(ctx, jobID)
	if err != nil {
		return err
	}
	if j.Status != domain.EngineJobRunning || j.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	task, err := s.repo.GetMigration(ctx, j.TaskID)
	if err != nil {
		return err
	}
	j.UpdatedAt = time.Now()
	if err = s.repo.UpdateEngineJob(ctx, j); err != nil {
		return err
	}
	if j.Direction == "reverse" {
		if task.Status == domain.StatusRollbackPreparing {
			return s.MarkRollbackCDCStarted(ctx, task.ID, nil)
		}
		return nil
	}
	if task.Status == domain.StatusCDCInitializing {
		return s.MarkCDCStarted(ctx, task.ID, nil)
	}
	return nil
}
func (s *Service) RenewEngineJob(ctx context.Context, workerID, jobID string) error {
	return s.repo.RenewEngineJobLease(ctx, jobID, workerID, engineJobLease)
}
func (s *Service) EngineJobControl(ctx context.Context, workerID, jobID string) (bool, error) {
	j, err := s.repo.GetEngineJob(ctx, jobID)
	if err != nil {
		return false, err
	}
	if j.WorkerID != workerID {
		return false, repository.ErrLeaseOwner
	}
	return j.Status == domain.EngineJobStopRequested, nil
}

// EngineJobCDCReady tells a managed native CDC reader whether source capture may
// begin. For full+incremental tasks capture starts during FULL_MIGRATING: each
// transaction is first durably staged in the encrypted QMigration CDC spool and
// only then may the reader ACK the source. Once target apply is safe, the spool
// drains in sequence before newer live transactions can overtake it.
func (s *Service) EngineJobCDCReady(ctx context.Context, workerID, jobID string) (bool, domain.MigrationStatus, error) {
	j, err := s.repo.GetEngineJob(ctx, jobID)
	if err != nil {
		return false, "", err
	}
	if j.WorkerID != workerID {
		return false, "", repository.ErrLeaseOwner
	}
	if j.Status != domain.EngineJobRunning && j.Status != domain.EngineJobStopRequested {
		return false, "", fmt.Errorf("engine job %s cannot query CDC readiness from %s", jobID, j.Status)
	}
	task, err := s.repo.GetMigration(ctx, j.TaskID)
	if err != nil {
		return false, "", err
	}
	if cdcApplyReady(task, j.Direction) {
		go func(taskID, direction string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			_, _ = s.DrainCDCSpool(ctx, taskID, direction, 10000)
		}(task.ID, j.Direction)
	}
	return cdcCaptureReady(task, j.Direction), task.Status, nil
}
func (s *Service) CompleteEngineJob(ctx context.Context, workerID, jobID string, result domain.EngineJobResult) error {
	j, err := s.repo.GetEngineJob(ctx, jobID)
	if err != nil {
		return err
	}
	if j.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	j.WorkerID = ""
	j.LeaseUntil = time.Time{}
	j.FinishedAt = time.Now()
	j.UpdatedAt = time.Now()
	wasStopRequested := j.Status == domain.EngineJobStopRequested
	if wasStopRequested || result.Error == "" {
		j.Status = domain.EngineJobStopped
	} else {
		j.Status = domain.EngineJobFailed
		j.LastError = result.Error
	}
	if err := s.repo.UpdateEngineJob(ctx, j); err != nil {
		return err
	}
	if wasStopRequested {
		_ = s.cleanupCDCCheckpoint(ctx, j.TaskID, j.Direction)
	}
	return nil
}
func (s *Service) FailEngineJob(ctx context.Context, workerID, jobID string, result domain.EngineJobResult) error {
	j, err := s.repo.GetEngineJob(ctx, jobID)
	if err != nil {
		return err
	}
	if j.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	j.RetryCount++
	j.LastError = result.Error
	j.WorkerID = ""
	j.LeaseUntil = time.Time{}
	j.UpdatedAt = time.Now()
	task, _ := s.repo.GetMigration(ctx, j.TaskID)
	maxRetries := 3
	if task != nil && task.MaxRetries > 0 {
		maxRetries = task.MaxRetries
	}
	if j.RetryCount <= maxRetries {
		j.Status = domain.EngineJobPending
	} else {
		j.Status = domain.EngineJobFailed
		j.FinishedAt = time.Now()
		if task != nil {
			s.failTask(task.ID, fmt.Errorf("managed %s CDC engine failed after %d attempts: %s", j.Engine, j.RetryCount, result.Error))
		}
	}
	return s.repo.UpdateEngineJob(ctx, j)
}
func (s *Service) cleanupCDCCheckpoint(ctx context.Context, taskID, direction string) error {
	task, err := s.repo.GetMigration(ctx, taskID)
	if err != nil {
		return err
	}
	dsID := task.SourceID
	resource := task.CDCStartResource
	if direction == "reverse" {
		dsID = task.TargetID
		resource = ""
		positions, _ := s.repo.ListCDCPositions(ctx, taskID, 200)
		for _, p := range positions {
			if p.Direction == "reverse" && p.Resource != "" {
				resource = p.Resource
				break
			}
		}
	}
	if resource == "" {
		return nil
	}
	ds, err := s.repo.GetDataSource(ctx, dsID)
	if err != nil {
		return err
	}
	raw, err := s.connectors.New(*ds)
	if err != nil {
		return err
	}
	defer raw.Close()
	checkpoint, ok := raw.(connector.CDCCheckpointSource)
	if !ok {
		return nil
	}
	return checkpoint.DropCDCCheckpoint(ctx, resource)
}

func (s *Service) requestStopEngineJobs(ctx context.Context, taskID, direction string) error {
	jobs, err := s.repo.ListEngineJobs(ctx, taskID)
	if err != nil {
		return err
	}
	for i := range jobs {
		j := jobs[i]
		if j.Kind != "CDC" || j.Direction != direction {
			continue
		}
		switch j.Status {
		case domain.EngineJobRunning:
			j.Status = domain.EngineJobStopRequested
			j.UpdatedAt = time.Now()
		case domain.EngineJobPending:
			j.Status = domain.EngineJobStopped
			j.UpdatedAt = time.Now()
			j.FinishedAt = time.Now()
		default:
			continue
		}
		if err := s.repo.UpdateEngineJob(ctx, &j); err != nil {
			return err
		}
	}
	return nil
}

func routingTransparentRelocation(t domain.DataSourceType) bool {
	switch t {
	case domain.DataSourcePolarDBX, domain.DataSourceTiDB, domain.DataSourceOceanBase:
		return true
	default:
		return false
	}
}

func relocationCandidate(table *domain.MigrationTable, from string) *domain.TopologyPlacement {
	if table == nil {
		return nil
	}
	var best *domain.TopologyPlacement
	bestRank := 999
	for i := range table.Topology {
		p := &table.Topology[i]
		if strings.TrimSpace(p.ID) == "" || p.ID == from || len(topologyPlacementHint(*p)) == 0 {
			continue
		}
		health := strings.ToUpper(strings.TrimSpace(repository.TopologyProfileHealth(table, p.ID)))
		rank := 1
		switch health {
		case "HEALTHY":
			rank = 0
		case "DEGRADED", "HALF_OPEN", "":
			rank = 1
		case "CIRCUIT_OPEN":
			continue
		}
		if rank < bestRank || (rank == bestRank && best != nil && p.ID < best.ID) {
			cp := *p
			best = &cp
			bestRank = rank
		}
	}
	return best
}

// relocateYieldRemainders moves only newly-created, cursor-bounded remainders.
// The completed prefix never changes ownership. Coordinator-routed databases
// treat topology as an advisory placement hint; direct-DN readers must provide
// an explicit replica/epoch/position proof through ReplicaRelocationConnector.
func (s *Service) relocateYieldRemainders(ctx context.Context, task *domain.MigrationTask, table *domain.MigrationTable, from *domain.MigrationChunk, created []domain.MigrationChunk) ([]domain.MigrationChunk, error) {
	if len(created) == 0 || !envDefaultOn("QMIGRATION_RUNNING_CHUNK_RELOCATION") || strings.TrimSpace(from.TopologyID) == "" {
		return created, nil
	}
	candidate := relocationCandidate(table, from.TopologyID)
	if candidate == nil {
		return created, nil
	}
	source, err := s.repo.GetDataSource(ctx, task.SourceID)
	if err != nil {
		return nil, err
	}
	safe := routingTransparentRelocation(source.Type)
	reason := "coordinator-routed topology hint"
	if !safe {
		if s.connectors == nil {
			return created, nil
		}
		src, err := s.connectors.New(*source)
		if err != nil {
			return nil, err
		}
		defer src.Close()
		prover, ok := src.(connector.ReplicaRelocationConnector)
		if !ok {
			return created, nil
		}
		proof, err := prover.ProveChunkRelocation(ctx, connector.ChunkRelocationRequest{Schema: table.SourceSchema, Table: table.SourceTable, FromTopologyID: from.TopologyID, ToTopologyID: candidate.ID, SplitType: from.SplitType, CursorJSON: from.CursorJSON})
		if err != nil {
			return nil, err
		}
		if !proof.Safe || (!proof.RoutingTransparent && (strings.TrimSpace(proof.ShardID) == "" || strings.TrimSpace(proof.Epoch) == "" || strings.TrimSpace(proof.Position) == "")) {
			return created, nil
		}
		safe = true
		reason = proof.Reason
	}
	if !safe {
		return created, nil
	}
	for i := range created {
		created[i].TopologyID = candidate.ID
		created[i].TopologyKind = candidate.Kind
		created[i].PlacementHint = topologyPlacementHint(*candidate)
		created[i].FaultDomain = repository.CanonicalFaultDomain(candidate.Labels)
	}
	s.logTask(ctx, task.ID, "", table.ID, from.ID, "INFO", fmt.Sprintf("relocated yielded remainder topology %s -> %s reason=%s", from.TopologyID, candidate.ID, strings.TrimSpace(reason)))
	return created, nil
}
