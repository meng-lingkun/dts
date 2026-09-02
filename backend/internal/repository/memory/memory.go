package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"sort"
	"strings"
	"sync"
	"time"
)

var ErrNotFound = fmt.Errorf("not found")

var _ repository.ControlOperationLeaser = (*Store)(nil)

type Store struct {
	mu                       sync.RWMutex
	path                     string
	datasources              map[string]domain.DataSource
	migrations               map[string]domain.MigrationTask
	tables                   map[string]domain.MigrationTable
	chunks                   map[string]domain.MigrationChunk
	workers                  map[string]domain.Worker
	engineJobs               map[string]domain.EngineJob
	cdcPositions             []domain.CDCPosition
	cdcSpool                 []domain.CDCSpoolRecord
	cdcDeadLetters           map[string]domain.CDCDeadLetter
	cdcConflicts             []domain.CDCConflictRecord
	validations              map[string]domain.ValidationResult
	validationArchives       map[string]domain.ValidationArchive
	validationReportArchives map[string]domain.ValidationReportArchiveRecord
	alerts                   map[string]domain.Alert
	audits                   []domain.AuditEvent
	taskLogs                 []domain.TaskLog
	users                    map[string]domain.User
	controlOperations        map[string]persistedControlOperation
}

type persistedControlOperation struct {
	TaskID, Operation, Owner string
	LeaseUntil, UpdatedAt    time.Time
}

type persistedCDCSpool struct {
	ID, TaskID, Direction, PositionType, PositionValue, Resource string
	Sequence, SourceTimestampMS                                  int64
	EventCount                                                   int
	PayloadBytes                                                 int64
	Events                                                       []domain.CDCEvent
	EventsCiphertext                                             string
	Status                                                       domain.CDCSpoolStatus
	CreatedAt, AppliedAt                                         time.Time
}

type persistedCDCDeadLetter struct {
	ID, TaskID, Direction, PositionType, PositionValue, Resource string
	Events                                                       []domain.CDCEvent
	EventsCiphertext                                             string
	LastError                                                    string
	RetryCount                                                   int
	Status                                                       domain.CDCDeadLetterStatus
	CreatedAt, UpdatedAt, ResolvedAt                             time.Time
}

type persistedDataSource struct {
	ID, Name               string
	Type                   domain.DataSourceType
	Host                   string
	Port                   int
	Username               string
	PasswordCiphertext     string
	Database               string
	Schema                 string
	JDBCURL                string
	DriverClass            string
	CDCURL                 string
	TLSMode                domain.TLSMode
	TLSServerName          string
	TLSCACert              string
	TLSClientCert          string
	TLSClientKeyCiphertext string
	CreatedAt, UpdatedAt   time.Time
}
type snapshot struct {
	DataSources              map[string]persistedDataSource                  `json:"datasources"`
	Migrations               map[string]domain.MigrationTask                 `json:"migrations"`
	Tables                   map[string]domain.MigrationTable                `json:"tables"`
	Chunks                   map[string]domain.MigrationChunk                `json:"chunks"`
	Workers                  map[string]domain.Worker                        `json:"workers"`
	EngineJobs               map[string]domain.EngineJob                     `json:"engine_jobs"`
	CDCPositions             []domain.CDCPosition                            `json:"cdc_positions"`
	CDCSpool                 []persistedCDCSpool                             `json:"cdc_spool"`
	CDCDeadLetters           map[string]persistedCDCDeadLetter               `json:"cdc_dead_letters"`
	CDCConflicts             []domain.CDCConflictRecord                      `json:"cdc_conflicts"`
	Validations              map[string]domain.ValidationResult              `json:"validations"`
	ValidationArchives       map[string]domain.ValidationArchive             `json:"validation_archives"`
	ValidationReportArchives map[string]domain.ValidationReportArchiveRecord `json:"validation_report_archives"`
	Alerts                   map[string]domain.Alert                         `json:"alerts"`
	Audits                   []domain.AuditEvent                             `json:"audits"`
	TaskLogs                 []domain.TaskLog                                `json:"task_logs"`
	Users                    map[string]domain.User                          `json:"users"`
	ControlOperations        map[string]persistedControlOperation            `json:"control_operations"`
}

func New() *Store { return newStore("") }
func NewPersistent(path string) (*Store, error) {
	s := newStore(path)
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}
func newStore(path string) *Store {
	return &Store{path: path, datasources: map[string]domain.DataSource{}, migrations: map[string]domain.MigrationTask{}, tables: map[string]domain.MigrationTable{}, chunks: map[string]domain.MigrationChunk{}, workers: map[string]domain.Worker{}, engineJobs: map[string]domain.EngineJob{}, cdcDeadLetters: map[string]domain.CDCDeadLetter{}, validations: map[string]domain.ValidationResult{}, validationArchives: map[string]domain.ValidationArchive{}, validationReportArchives: map[string]domain.ValidationReportArchiveRecord{}, alerts: map[string]domain.Alert{}, audits: []domain.AuditEvent{}, taskLogs: []domain.TaskLog{}, users: map[string]domain.User{}, controlOperations: map[string]persistedControlOperation{}}
}
func (s *Store) load() error {
	if s.path == "" {
		return nil
	}
	b, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("load repository snapshot: %w", err)
	}
	for id, p := range snap.DataSources {
		s.datasources[id] = domain.DataSource{ID: p.ID, Name: p.Name, Type: p.Type, Host: p.Host, Port: p.Port, Username: p.Username, PasswordCiphertext: p.PasswordCiphertext, Database: p.Database, Schema: p.Schema, JDBCURL: p.JDBCURL, DriverClass: p.DriverClass, CDCURL: p.CDCURL, TLSMode: p.TLSMode, TLSServerName: p.TLSServerName, TLSCACert: p.TLSCACert, TLSClientCert: p.TLSClientCert, TLSClientKeyCiphertext: p.TLSClientKeyCiphertext, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}
	}
	if snap.Migrations != nil {
		s.migrations = snap.Migrations
	}
	if snap.Tables != nil {
		s.tables = snap.Tables
	}
	if snap.Chunks != nil {
		s.chunks = snap.Chunks
	}
	if snap.Workers != nil {
		s.workers = snap.Workers
	}
	if snap.EngineJobs != nil {
		s.engineJobs = snap.EngineJobs
	}
	if snap.CDCPositions != nil {
		s.cdcPositions = snap.CDCPositions
	}
	if snap.CDCSpool != nil {
		s.cdcSpool = make([]domain.CDCSpoolRecord, 0, len(snap.CDCSpool))
		for _, p := range snap.CDCSpool {
			s.cdcSpool = append(s.cdcSpool, domain.CDCSpoolRecord{ID: p.ID, TaskID: p.TaskID, Direction: p.Direction, Sequence: p.Sequence, PositionType: p.PositionType, PositionValue: p.PositionValue, Resource: p.Resource, SourceTimestampMS: p.SourceTimestampMS, EventCount: p.EventCount, PayloadBytes: p.PayloadBytes, Events: p.Events, EventsCiphertext: p.EventsCiphertext, Status: p.Status, CreatedAt: p.CreatedAt, AppliedAt: p.AppliedAt})
		}
	}
	if snap.CDCDeadLetters != nil {
		s.cdcDeadLetters = make(map[string]domain.CDCDeadLetter, len(snap.CDCDeadLetters))
		for id, p := range snap.CDCDeadLetters {
			s.cdcDeadLetters[id] = domain.CDCDeadLetter{ID: p.ID, TaskID: p.TaskID, Direction: p.Direction, PositionType: p.PositionType, PositionValue: p.PositionValue, Resource: p.Resource, Events: p.Events, EventsCiphertext: p.EventsCiphertext, LastError: p.LastError, RetryCount: p.RetryCount, Status: p.Status, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt, ResolvedAt: p.ResolvedAt}
		}
	}
	if snap.CDCConflicts != nil {
		s.cdcConflicts = snap.CDCConflicts
	}
	if snap.Validations != nil {
		s.validations = snap.Validations
	}
	if snap.ValidationArchives != nil {
		s.validationArchives = snap.ValidationArchives
	}
	if snap.ValidationReportArchives != nil {
		s.validationReportArchives = snap.ValidationReportArchives
	}
	if snap.Alerts != nil {
		s.alerts = snap.Alerts
	}
	if snap.Audits != nil {
		s.audits = snap.Audits
	}
	if snap.TaskLogs != nil {
		s.taskLogs = snap.TaskLogs
	}
	if snap.Users != nil {
		s.users = snap.Users
	}
	if snap.ControlOperations != nil {
		s.controlOperations = snap.ControlOperations
	}
	return nil
}
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	ds := make(map[string]persistedDataSource, len(s.datasources))
	for id, d := range s.datasources {
		ds[id] = persistedDataSource{ID: d.ID, Name: d.Name, Type: d.Type, Host: d.Host, Port: d.Port, Username: d.Username, PasswordCiphertext: d.PasswordCiphertext, Database: d.Database, Schema: d.Schema, JDBCURL: d.JDBCURL, DriverClass: d.DriverClass, CDCURL: d.CDCURL, TLSMode: d.TLSMode, TLSServerName: d.TLSServerName, TLSCACert: d.TLSCACert, TLSClientCert: d.TLSClientCert, TLSClientKeyCiphertext: d.TLSClientKeyCiphertext, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
	}
	spool := make([]persistedCDCSpool, 0, len(s.cdcSpool))
	for _, v := range s.cdcSpool {
		spool = append(spool, persistedCDCSpool{ID: v.ID, TaskID: v.TaskID, Direction: v.Direction, Sequence: v.Sequence, PositionType: v.PositionType, PositionValue: v.PositionValue, Resource: v.Resource, SourceTimestampMS: v.SourceTimestampMS, EventCount: v.EventCount, PayloadBytes: v.PayloadBytes, Events: v.Events, EventsCiphertext: v.EventsCiphertext, Status: v.Status, CreatedAt: v.CreatedAt, AppliedAt: v.AppliedAt})
	}
	deadLetters := make(map[string]persistedCDCDeadLetter, len(s.cdcDeadLetters))
	for id, v := range s.cdcDeadLetters {
		deadLetters[id] = persistedCDCDeadLetter{ID: v.ID, TaskID: v.TaskID, Direction: v.Direction, PositionType: v.PositionType, PositionValue: v.PositionValue, Resource: v.Resource, Events: v.Events, EventsCiphertext: v.EventsCiphertext, LastError: v.LastError, RetryCount: v.RetryCount, Status: v.Status, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt, ResolvedAt: v.ResolvedAt}
	}
	snap := snapshot{DataSources: ds, Migrations: s.migrations, Tables: s.tables, Chunks: s.chunks, Workers: s.workers, EngineJobs: s.engineJobs, CDCPositions: s.cdcPositions, CDCSpool: spool, CDCDeadLetters: deadLetters, CDCConflicts: s.cdcConflicts, Validations: s.validations, ValidationArchives: s.validationArchives, ValidationReportArchives: s.validationReportArchives, Alerts: s.alerts, Audits: s.audits, TaskLogs: s.taskLogs, Users: s.users, ControlOperations: s.controlOperations}
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func cloneDS(v domain.DataSource) domain.DataSource { return v }
func cloneMigration(v domain.MigrationTask) domain.MigrationTask {
	if v.Tables != nil {
		v.Tables = append([]domain.TableMapping(nil), v.Tables...)
	}
	if v.WorkerSelector != nil {
		v.WorkerSelector = maps.Clone(v.WorkerSelector)
	}
	if v.RateLimitWindows != nil {
		v.RateLimitWindows = append([]domain.RateLimitWindow(nil), v.RateLimitWindows...)
	}
	return v
}
func cloneTable(v domain.MigrationTable) domain.MigrationTable {
	if v.Columns != nil {
		v.Columns = append([]domain.ColumnInfo(nil), v.Columns...)
	}
	if v.TargetColumns != nil {
		v.TargetColumns = append([]domain.ColumnInfo(nil), v.TargetColumns...)
	}
	if v.TopologyPerformance != nil {
		v.TopologyPerformance = maps.Clone(v.TopologyPerformance)
	}
	return v
}

func (s *Store) CreateDataSource(_ context.Context, d *domain.DataSource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.datasources[d.ID] = cloneDS(*d)
	return s.persistLocked()
}
func (s *Store) ListDataSources(_ context.Context) ([]domain.DataSource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.DataSource, 0, len(s.datasources))
	for _, v := range s.datasources {
		out = append(out, cloneDS(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
func (s *Store) GetDataSource(_ context.Context, id string) (*domain.DataSource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.datasources[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := cloneDS(v)
	return &c, nil
}

func (s *Store) CreateMigration(_ context.Context, m *domain.MigrationTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.migrations[m.ID] = cloneMigration(*m)
	return s.persistLocked()
}
func (s *Store) ListMigrations(_ context.Context) ([]domain.MigrationTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.MigrationTask, 0, len(s.migrations))
	for _, v := range s.migrations {
		out = append(out, cloneMigration(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}
func (s *Store) GetMigration(_ context.Context, id string) (*domain.MigrationTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.migrations[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := cloneMigration(v)
	return &c, nil
}
func (s *Store) UpdateMigration(_ context.Context, m *domain.MigrationTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.migrations[m.ID]; !ok {
		return ErrNotFound
	}
	s.migrations[m.ID] = cloneMigration(*m)
	return s.persistLocked()
}

func (s *Store) AcquireControlOperation(_ context.Context, taskID, operation, owner string, lease time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	current, exists := s.controlOperations[taskID]
	if exists && current.LeaseUntil.After(now) {
		return false, nil
	}
	s.controlOperations[taskID] = persistedControlOperation{TaskID: taskID, Operation: operation, Owner: owner, LeaseUntil: now.Add(lease), UpdatedAt: now}
	if err := s.persistLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) RenewControlOperation(_ context.Context, taskID, operation, owner string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.controlOperations[taskID]
	if !exists || current.Owner != owner || current.Operation != operation {
		return repository.ErrLeaseOwner
	}
	now := time.Now()
	current.LeaseUntil = now.Add(lease)
	current.UpdatedAt = now
	s.controlOperations[taskID] = current
	return s.persistLocked()
}

func (s *Store) ReleaseControlOperation(_ context.Context, taskID, operation, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.controlOperations[taskID]
	if !exists {
		return nil
	}
	if current.Owner != owner || current.Operation != operation {
		return repository.ErrLeaseOwner
	}
	delete(s.controlOperations, taskID)
	return s.persistLocked()
}

func (s *Store) CreateMigrationTable(_ context.Context, t *domain.MigrationTable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[t.ID] = cloneTable(*t)
	return s.persistLocked()
}
func (s *Store) ListMigrationTables(_ context.Context, taskID string) ([]domain.MigrationTable, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.MigrationTable{}
	for _, v := range s.tables {
		if v.TaskID == taskID {
			out = append(out, cloneTable(v))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceSchema == out[j].SourceSchema {
			return out[i].SourceTable < out[j].SourceTable
		}
		return out[i].SourceSchema < out[j].SourceSchema
	})
	return out, nil
}
func (s *Store) GetMigrationTable(_ context.Context, id string) (*domain.MigrationTable, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.tables[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := cloneTable(v)
	return &c, nil
}
func (s *Store) UpdateMigrationTable(_ context.Context, t *domain.MigrationTable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[t.ID]; !ok {
		return ErrNotFound
	}
	s.tables[t.ID] = cloneTable(*t)
	return s.persistLocked()
}
func (s *Store) FindMigrationTableProfile(_ context.Context, sourceID, targetID, schema, table string) (*domain.MigrationTable, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var best *domain.MigrationTable
	var bestUpdated time.Time
	for _, t := range s.tables {
		m, ok := s.migrations[t.TaskID]
		if !ok || m.SourceID != sourceID || m.TargetID != targetID || !strings.EqualFold(t.SourceSchema, schema) || !strings.EqualFold(t.SourceTable, table) || t.PerformanceSamples <= 0 {
			continue
		}
		if best == nil || m.UpdatedAt.After(bestUpdated) || (m.UpdatedAt.Equal(bestUpdated) && t.PerformanceSamples > best.PerformanceSamples) {
			c := cloneTable(t)
			best = &c
			bestUpdated = m.UpdatedAt
		}
	}
	if best == nil {
		return nil, ErrNotFound
	}
	return best, nil
}

func (s *Store) CreateChunks(_ context.Context, chunks []domain.MigrationChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range chunks {
		s.chunks[c.ID] = c
	}
	return s.persistLocked()
}
func (s *Store) ListChunks(_ context.Context, taskID string) ([]domain.MigrationChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.MigrationChunk{}
	for _, v := range s.chunks {
		if v.TaskID == taskID {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID == out[j].TableID {
			return out[i].ChunkNo < out[j].ChunkNo
		}
		return out[i].TableID < out[j].TableID
	})
	return out, nil
}
func (s *Store) GetChunk(_ context.Context, id string) (*domain.MigrationChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.chunks[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &v, nil
}
func (s *Store) UpdateChunk(_ context.Context, c *domain.MigrationChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.chunks[c.ID]; !ok {
		return ErrNotFound
	}
	s.chunks[c.ID] = *c
	return s.persistLocked()
}

func (s *Store) YieldChunk(_ context.Context, worker string, completed *domain.MigrationChunk, created []domain.MigrationChunk) error {
	if completed == nil {
		return fmt.Errorf("yielded chunk is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.chunks[completed.ID]
	if !ok {
		return ErrNotFound
	}
	if current.Status != domain.ChunkRunning || current.WorkerID != worker {
		return repository.ErrLeaseOwner
	}
	for _, ch := range created {
		if _, exists := s.chunks[ch.ID]; exists {
			return fmt.Errorf("chunk %s already exists", ch.ID)
		}
	}
	s.chunks[completed.ID] = *completed
	for _, ch := range created {
		s.chunks[ch.ID] = ch
	}
	return s.persistLocked()
}

func labelsMatch(labels, selector map[string]string) bool {
	for key, want := range selector {
		if labels[key] != want {
			return false
		}
	}
	return true
}

func affinityRank(m domain.MigrationTask, labels map[string]string) int {
	if len(m.WorkerSelector) == 0 {
		return 1
	}
	if labelsMatch(labels, m.WorkerSelector) {
		return 0
	}
	if strings.EqualFold(m.WorkerAffinity, "REQUIRED") {
		return 100
	}
	return 2
}

func placementRank(c domain.MigrationChunk, labels map[string]string) int {
	if len(c.PlacementHint) == 0 {
		return 1
	}
	if labelsMatch(labels, c.PlacementHint) {
		return 0
	}
	return 2
}
func (s *Store) ClaimChunk(_ context.Context, workerID string, lease time.Duration, capabilities []string) (*domain.MigrationChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	changed := false
	for id, c := range s.chunks {
		if c.Status == domain.ChunkRunning && !c.LeaseUntil.IsZero() && now.After(c.LeaseUntil) {
			c.Status = domain.ChunkPending
			c.WorkerID = ""
			c.LeaseUntil = time.Time{}
			c.RetryCount++
			c.LastError = "worker lease expired"
			s.chunks[id] = c
			changed = true
		}
	}
	ids := make([]string, 0, len(s.chunks))
	for id := range s.chunks {
		ids = append(ids, id)
	}
	workerLabels := map[string]string{}
	if worker, ok := s.workers[workerID]; ok && worker.Labels != nil {
		workerLabels = worker.Labels
	}
	tableRunning := func(tableID string) int {
		n := 0
		for _, ch := range s.chunks {
			if ch.TableID == tableID && ch.Status == domain.ChunkRunning {
				n++
			}
		}
		return n
	}
	topologyRunning := func(taskID, topologyID string) int {
		if topologyID == "" {
			return 0
		}
		n := 0
		for _, ch := range s.chunks {
			if ch.TaskID == taskID && ch.TopologyID == topologyID && ch.Status == domain.ChunkRunning {
				n++
			}
		}
		return n
	}
	faultDomainState := func(candidate domain.MigrationChunk) (int, int) {
		if !repository.FaultDomainProtectionEnabled() || len(candidate.FaultDomain) == 0 {
			return 0, 0
		}
		rackRisk, zoneRisk, regionRisk := 0, 0, 0
		regionZones := map[string]bool{}
		for _, ch := range s.chunks {
			if ch.TaskID != candidate.TaskID || ch.TopologyID == "" || ch.TopologyID == candidate.TopologyID {
				continue
			}
			table := s.tables[ch.TableID]
			rank := repository.TopologyClaimRank(&table, ch.TopologyID)
			if rank <= 0 {
				continue
			}
			if candidate.FaultDomain["rack"] != "" && ch.FaultDomain["rack"] == candidate.FaultDomain["rack"] && rank > rackRisk {
				rackRisk = rank
			}
			if candidate.FaultDomain["zone"] != "" && ch.FaultDomain["zone"] == candidate.FaultDomain["zone"] && rank > zoneRisk {
				zoneRisk = rank
			}
			if candidate.FaultDomain["region"] != "" && ch.FaultDomain["region"] == candidate.FaultDomain["region"] {
				zoneKey := ch.FaultDomain["zone"]
				if zoneKey == "" {
					zoneKey = "topology:" + ch.TopologyID
				}
				regionZones[zoneKey] = true
				if rank > regionRisk {
					regionRisk = rank
				}
			}
		}
		if len(regionZones) < repository.FaultDomainRegionMinUnhealthyZones() {
			regionRisk = 0
		}
		risk, scope := 0, ""
		if rackRisk > 0 {
			risk, scope = rackRisk, "rack"
		}
		if zoneRisk > 0 {
			if zoneRisk > risk {
				risk = zoneRisk
			}
			scope = "zone"
		}
		if regionRisk > 0 {
			if regionRisk > risk {
				risk = regionRisk
			}
			scope = "region"
		}
		if scope == "" {
			return 0, 0
		}
		running := 0
		for _, ch := range s.chunks {
			if ch.TaskID == candidate.TaskID && ch.Status == domain.ChunkRunning && candidate.FaultDomain[scope] != "" && ch.FaultDomain[scope] == candidate.FaultDomain[scope] {
				running++
			}
		}
		return risk, running
	}
	faultDomainRunning := func(candidate domain.MigrationChunk) int { _, running := faultDomainState(candidate); return running }
	faultDomainPeerRisk := func(candidate domain.MigrationChunk) int { risk, _ := faultDomainState(candidate); return risk }
	sort.Slice(ids, func(i, j int) bool {
		ci, cj := s.chunks[ids[i]], s.chunks[ids[j]]
		mi, iok := s.migrations[ci.TaskID]
		mj, jok := s.migrations[cj.TaskID]
		ri, rj := 50, 50
		if iok {
			ri = affinityRank(mi, workerLabels)
		}
		if jok {
			rj = affinityRank(mj, workerLabels)
		}
		if ri != rj {
			return ri < rj
		}
		tiProfile, tjProfile := s.tables[ci.TableID], s.tables[cj.TableID]
		hri, hrj := repository.TopologyClaimRank(&tiProfile, ci.TopologyID), repository.TopologyClaimRank(&tjProfile, cj.TopologyID)
		if hri != hrj {
			return hri < hrj
		}
		fri, frj := faultDomainPeerRisk(ci), faultDomainPeerRisk(cj)
		if fri != frj {
			return fri < frj
		}
		fdi, fdj := faultDomainRunning(ci), faultDomainRunning(cj)
		if fdi != fdj {
			return fdi < fdj
		}
		pi, pj := placementRank(ci, workerLabels), placementRank(cj, workerLabels)
		if pi != pj {
			return pi < pj
		}
		hi, hj := topologyRunning(ci.TaskID, ci.TopologyID), topologyRunning(cj.TaskID, cj.TopologyID)
		if hi != hj {
			return hi < hj
		}
		ti, tj := tableRunning(ci.TableID), tableRunning(cj.TableID)
		if ti != tj {
			return ti < tj
		}
		if ci.TaskID != cj.TaskID {
			return ci.TaskID < cj.TaskID
		}
		if ci.TableID != cj.TableID {
			return ci.TableID < cj.TableID
		}
		return ci.ChunkNo < cj.ChunkNo
	})
	for _, id := range ids {
		c := s.chunks[id]
		if c.Status != domain.ChunkPending {
			continue
		}
		m, ok := s.migrations[c.TaskID]
		if !ok || m.Status != domain.StatusFullMigrating {
			continue
		}
		if affinityRank(m, workerLabels) >= 100 {
			continue
		}
		engineName := m.FullEngine
		if table, exists := s.tables[c.TableID]; exists && table.Engine != "" {
			engineName = table.Engine
		}
		if engineName == "" || engineName == "auto" {
			engineName = "qmigration"
		}
		capable := false
		for _, cap := range capabilities {
			if cap == engineName || (engineName == "qmigration" && cap == "native") {
				// "native" is accepted only as a rolling-upgrade compatibility
				// alias for pre-unified Workers. New Workers advertise qmigration.
				capable = true
				break
			}
		}
		if !capable {
			continue
		}
		if table, exists := s.tables[c.TableID]; exists {
			if !repository.TopologyClaimAllowed(&table, c.TopologyID, topologyRunning(c.TaskID, c.TopologyID)) {
				continue
			}
		}
		if cap := repository.FaultDomainConcurrencyCap(faultDomainPeerRisk(c)); cap > 0 && faultDomainRunning(c) >= cap {
			continue
		}
		running := 0
		for _, o := range s.chunks {
			if o.TaskID == c.TaskID && o.Status == domain.ChunkRunning {
				running++
			}
		}
		limit := m.EffectiveParallelism
		if limit <= 0 || limit > m.Parallelism {
			limit = m.Parallelism
		}
		if limit > 0 && running >= limit {
			continue
		}
		c.Status = domain.ChunkRunning
		c.WorkerID = workerID
		c.LeaseUntil = now.Add(lease)
		if c.StartedAt.IsZero() {
			c.StartedAt = now
		}
		c.LastError = ""
		s.chunks[id] = c
		_ = s.persistLocked()
		return &c, nil
	}
	if changed {
		_ = s.persistLocked()
	}
	return nil, repository.ErrNoChunk
}
func (s *Store) RenewChunkLease(_ context.Context, id, workerID string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.chunks[id]
	if !ok {
		return ErrNotFound
	}
	if c.Status != domain.ChunkRunning || c.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	c.LeaseUntil = time.Now().Add(lease)
	s.chunks[id] = c
	return s.persistLocked()
}
func (s *Store) UpdateChunkProgress(_ context.Context, id, workerID string, progress domain.ChunkProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.chunks[id]
	if !ok {
		return ErrNotFound
	}
	if c.Status != domain.ChunkRunning || c.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	if progress.CursorJSON != "" {
		c.CursorJSON = progress.CursorJSON
	}
	c.RowsRead = progress.RowsRead
	c.RowsWritten = progress.RowsWritten
	c.BytesRead = progress.BytesRead
	c.BytesWritten = progress.BytesWritten
	c.LastReadMS = progress.LastReadMS
	c.LastWriteMS = progress.LastWriteMS
	c.LastBatchRows = progress.LastBatchRows
	c.BackpressureLevel = progress.BackpressureLevel
	s.chunks[id] = c
	return s.persistLocked()
}

func (s *Store) CreateEngineJob(_ context.Context, j *domain.EngineJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.engineJobs[j.ID]; exists {
		return fmt.Errorf("engine job already exists")
	}
	s.engineJobs[j.ID] = *j
	return s.persistLocked()
}
func (s *Store) GetEngineJob(_ context.Context, id string) (*domain.EngineJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.engineJobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := j
	return &c, nil
}
func (s *Store) ListEngineJobs(_ context.Context, taskID string) ([]domain.EngineJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.EngineJob{}
	for _, j := range s.engineJobs {
		if taskID == "" || j.TaskID == taskID {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}
func (s *Store) UpdateEngineJob(_ context.Context, j *domain.EngineJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.engineJobs[j.ID]; !ok {
		return ErrNotFound
	}
	s.engineJobs[j.ID] = *j
	return s.persistLocked()
}
func (s *Store) ClaimEngineJob(_ context.Context, workerID string, lease time.Duration, capabilities []string) (*domain.EngineJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	caps := map[string]bool{}
	for _, c := range capabilities {
		caps[c] = true
		if c == "native" || c == "native-mysql-cdc" || c == "native-postgres-cdc" {
			caps["qmigration"] = true
		}
	}
	changed := false
	for id, j := range s.engineJobs {
		if j.Status == domain.EngineJobRunning && !j.LeaseUntil.IsZero() && j.LeaseUntil.Before(now) {
			j.Status = domain.EngineJobPending
			j.WorkerID = ""
			j.LeaseUntil = time.Time{}
			j.RetryCount++
			j.LastError = "worker lease expired"
			j.UpdatedAt = now
			s.engineJobs[id] = j
			changed = true
		}
	}
	ids := make([]string, 0, len(s.engineJobs))
	for id := range s.engineJobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		j := s.engineJobs[id]
		if j.Status != domain.EngineJobPending || !caps[j.Engine] {
			continue
		}
		j.Status = domain.EngineJobRunning
		j.WorkerID = workerID
		j.LeaseUntil = now.Add(lease)
		if j.StartedAt.IsZero() {
			j.StartedAt = now
		}
		j.UpdatedAt = now
		j.LastError = ""
		s.engineJobs[id] = j
		_ = s.persistLocked()
		return &j, nil
	}
	if changed {
		_ = s.persistLocked()
	}
	return nil, repository.ErrNoChunk
}
func (s *Store) RenewEngineJobLease(_ context.Context, id, workerID string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.engineJobs[id]
	if !ok {
		return ErrNotFound
	}
	if j.Status != domain.EngineJobRunning || j.WorkerID != workerID {
		return repository.ErrLeaseOwner
	}
	j.LeaseUntil = time.Now().Add(lease)
	j.UpdatedAt = time.Now()
	s.engineJobs[id] = j
	return s.persistLocked()
}

func (s *Store) UpsertWorker(_ context.Context, w *domain.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := *w
	if w.Capabilities != nil {
		c.Capabilities = append([]string(nil), w.Capabilities...)
	}
	if w.Labels != nil {
		c.Labels = maps.Clone(w.Labels)
	}
	s.workers[w.ID] = c
	return s.persistLocked()
}
func (s *Store) ListWorkers(_ context.Context) ([]domain.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Worker, 0, len(s.workers))
	for _, v := range s.workers {
		c := v
		if v.Capabilities != nil {
			c.Capabilities = append([]string(nil), v.Capabilities...)
		}
		if v.Labels != nil {
			c.Labels = maps.Clone(v.Labels)
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, nil
}
func (s *Store) GetWorker(_ context.Context, id string) (*domain.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.workers[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := v
	if v.Capabilities != nil {
		c.Capabilities = append([]string(nil), v.Capabilities...)
	}
	if v.Labels != nil {
		c.Labels = maps.Clone(v.Labels)
	}
	return &c, nil
}

func (s *Store) CreateCDCPosition(_ context.Context, v *domain.CDCPosition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cdcPositions = append(s.cdcPositions, *v)
	// RC41 replaces the old global 10k slice truncation. A busy task must never
	// evict another paused task's newest durable CDC checkpoint. The emergency
	// in-memory bound preserves at least the newest item for every task+direction.
	if len(s.cdcPositions) > 60000 {
		s.cdcPositions = compactCDCPositionsHardLimit(s.cdcPositions, 50000)
	}
	return s.persistLocked()
}

func compactCDCPositionsHardLimit(in []domain.CDCPosition, limit int) []domain.CDCPosition {
	if limit <= 0 || len(in) <= limit {
		return in
	}
	latest := map[string]int{}
	for i := range in {
		key := in[i].TaskID + "\x00" + in[i].Direction
		prev, ok := latest[key]
		if !ok || in[i].RecordedAt.After(in[prev].RecordedAt) || (in[i].RecordedAt.Equal(in[prev].RecordedAt) && i > prev) {
			latest[key] = i
		}
	}
	keep := make(map[int]bool, len(latest)+limit)
	for _, idx := range latest {
		keep[idx] = true
	}
	for i := len(in) - 1; i >= 0 && len(keep) < limit; i-- {
		keep[i] = true
	}
	out := make([]domain.CDCPosition, 0, len(keep))
	for i := range in {
		if keep[i] {
			out = append(out, in[i])
		}
	}
	return out
}
func (s *Store) ListCDCPositions(_ context.Context, taskID string, limit int) ([]domain.CDCPosition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	out := []domain.CDCPosition{}
	for i := len(s.cdcPositions) - 1; i >= 0 && len(out) < limit; i-- {
		if s.cdcPositions[i].TaskID == taskID {
			out = append(out, s.cdcPositions[i])
		}
	}
	return out, nil
}

func cloneSpool(v domain.CDCSpoolRecord) domain.CDCSpoolRecord {
	if v.Events != nil {
		v.Events = append([]domain.CDCEvent(nil), v.Events...)
	}
	return v
}

func (s *Store) CreateCDCSpool(_ context.Context, v *domain.CDCSpoolRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.cdcSpool {
		if s.cdcSpool[i].ID == v.ID {
			v.Sequence = s.cdcSpool[i].Sequence
			return nil
		}
	}
	var maxSeq int64
	for i := range s.cdcSpool {
		if s.cdcSpool[i].TaskID == v.TaskID && s.cdcSpool[i].Direction == v.Direction && s.cdcSpool[i].Sequence > maxSeq {
			maxSeq = s.cdcSpool[i].Sequence
		}
	}
	v.Sequence = maxSeq + 1
	if v.EventCount == 0 {
		v.EventCount = len(v.Events)
	}
	if v.PayloadBytes == 0 && len(v.Events) > 0 {
		if b, err := json.Marshal(v.Events); err == nil {
			v.PayloadBytes = int64(len(b))
		}
	}
	s.cdcSpool = append(s.cdcSpool, cloneSpool(*v))
	return s.persistLocked()
}

func (s *Store) ListCDCSpool(_ context.Context, taskID, direction string, limit int) ([]domain.CDCSpoolRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 10000 {
		limit = 200
	}
	out := make([]domain.CDCSpoolRecord, 0, limit)
	for _, v := range s.cdcSpool {
		if v.TaskID == taskID && v.Direction == direction && v.Status == domain.CDCSpoolPending {
			out = append(out, cloneSpool(v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) LatestPendingCDCSpool(_ context.Context, taskID, direction string) (*domain.CDCSpoolRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var found *domain.CDCSpoolRecord
	for _, v := range s.cdcSpool {
		if v.TaskID != taskID || v.Direction != direction || v.Status != domain.CDCSpoolPending {
			continue
		}
		if found == nil || v.Sequence > found.Sequence {
			c := cloneSpool(v)
			found = &c
		}
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

func (s *Store) MarkCDCSpoolApplied(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.cdcSpool {
		if s.cdcSpool[i].ID == id {
			s.cdcSpool[i].Status = domain.CDCSpoolApplied
			s.cdcSpool[i].AppliedAt = at
			return s.persistLocked()
		}
	}
	return ErrNotFound
}

func (s *Store) DeleteAppliedCDCSpool(_ context.Context, taskID, direction string, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if keep < 0 {
		keep = 0
	}
	applied := make([]int, 0)
	for i, v := range s.cdcSpool {
		if v.TaskID == taskID && v.Direction == direction && v.Status == domain.CDCSpoolApplied {
			applied = append(applied, i)
		}
	}
	remove := len(applied) - keep
	if remove <= 0 {
		return nil
	}
	drop := map[int]bool{}
	for _, idx := range applied[:remove] {
		drop[idx] = true
	}
	out := s.cdcSpool[:0]
	for i, v := range s.cdcSpool {
		if !drop[i] {
			out = append(out, v)
		}
	}
	s.cdcSpool = out
	return s.persistLocked()
}

func (s *Store) CDCSpoolStats(_ context.Context, taskID, direction string) (domain.CDCSpoolStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out domain.CDCSpoolStats
	for _, v := range s.cdcSpool {
		if v.TaskID != taskID || v.Direction != direction || v.Status != domain.CDCSpoolPending {
			continue
		}
		out.PendingTransactions++
		out.PendingEvents += int64(v.EventCount)
		out.PendingBytes += v.PayloadBytes
		if out.FirstPosition == "" {
			out.FirstPosition = v.PositionValue
		}
		out.LastPosition = v.PositionValue
	}
	return out, nil
}

func cloneDeadLetter(v domain.CDCDeadLetter) domain.CDCDeadLetter {
	if v.Events != nil {
		v.Events = append([]domain.CDCEvent(nil), v.Events...)
	}
	return v
}
func (s *Store) CreateCDCDeadLetter(_ context.Context, v *domain.CDCDeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cdcDeadLetters[v.ID] = cloneDeadLetter(*v)
	return s.persistLocked()
}
func (s *Store) UpdateCDCDeadLetter(_ context.Context, v *domain.CDCDeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cdcDeadLetters[v.ID]; !ok {
		return ErrNotFound
	}
	s.cdcDeadLetters[v.ID] = cloneDeadLetter(*v)
	return s.persistLocked()
}
func (s *Store) GetCDCDeadLetter(_ context.Context, id string) (*domain.CDCDeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.cdcDeadLetters[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := cloneDeadLetter(v)
	return &c, nil
}
func (s *Store) ListCDCDeadLetters(_ context.Context, taskID string) ([]domain.CDCDeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.CDCDeadLetter{}
	for _, v := range s.cdcDeadLetters {
		if taskID == "" || v.TaskID == taskID {
			out = append(out, cloneDeadLetter(v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}
func (s *Store) DeleteCDCDeadLetter(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cdcDeadLetters, id)
	return s.persistLocked()
}

func (s *Store) CreateCDCConflict(_ context.Context, v *domain.CDCConflictRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.cdcConflicts {
		if existing.ID == v.ID {
			return nil
		}
	}
	s.cdcConflicts = append(s.cdcConflicts, *v)
	if len(s.cdcConflicts) > 100000 {
		s.cdcConflicts = s.cdcConflicts[len(s.cdcConflicts)-100000:]
	}
	return s.persistLocked()
}
func (s *Store) ListCDCConflicts(_ context.Context, taskID string, limit int) ([]domain.CDCConflictRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	out := []domain.CDCConflictRecord{}
	for i := len(s.cdcConflicts) - 1; i >= 0 && len(out) < limit; i-- {
		if taskID == "" || s.cdcConflicts[i].TaskID == taskID {
			out = append(out, s.cdcConflicts[i])
		}
	}
	return out, nil
}

func (s *Store) CreateValidationResult(_ context.Context, v *domain.ValidationResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validations[v.ID] = *v
	return s.persistLocked()
}
func (s *Store) ListValidationResults(_ context.Context, taskID string) ([]domain.ValidationResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []domain.ValidationResult{}
	for _, v := range s.validations {
		if taskID == "" || v.TaskID == taskID {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out, nil
}
func (s *Store) DeleteValidationResults(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, v := range s.validations {
		if taskID == "" || v.TaskID == taskID {
			delete(s.validations, id)
		}
	}
	return s.persistLocked()
}
func (s *Store) CreateAlert(_ context.Context, a *domain.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts[a.ID] = *a
	return s.persistLocked()
}
func (s *Store) ListAlerts(_ context.Context) ([]domain.Alert, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Alert, 0, len(s.alerts))
	for _, a := range s.alerts {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}
func (s *Store) AcknowledgeAlert(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.alerts[id]
	if !ok {
		return ErrNotFound
	}
	a.Acknowledged = true
	s.alerts[id] = a
	return s.persistLocked()
}
func (s *Store) CreateAuditEvent(_ context.Context, a *domain.AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, *a)
	if len(s.audits) > 10000 {
		s.audits = s.audits[len(s.audits)-10000:]
	}
	return s.persistLocked()
}
func (s *Store) ListAuditEvents(_ context.Context, limit int) ([]domain.AuditEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	n := len(s.audits)
	start := n - limit
	if start < 0 {
		start = 0
	}
	out := append([]domain.AuditEvent(nil), s.audits[start:]...)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (s *Store) CreateTaskLog(_ context.Context, v *domain.TaskLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taskLogs = append(s.taskLogs, *v)
	if len(s.taskLogs) > 20000 {
		s.taskLogs = append([]domain.TaskLog(nil), s.taskLogs[len(s.taskLogs)-20000:]...)
	}
	return s.persistLocked()
}
func (s *Store) ListTaskLogs(_ context.Context, taskID string, limit int) ([]domain.TaskLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	out := make([]domain.TaskLog, 0, limit)
	for i := len(s.taskLogs) - 1; i >= 0 && len(out) < limit; i-- {
		v := s.taskLogs[i]
		if taskID == "" || v.TaskID == taskID {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *Store) UpdateDataSource(_ context.Context, d *domain.DataSource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.datasources[d.ID]; !ok {
		return ErrNotFound
	}
	s.datasources[d.ID] = cloneDS(*d)
	return s.persistLocked()
}
func (s *Store) DeleteDataSource(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.datasources[id]; !ok {
		return ErrNotFound
	}
	for _, m := range s.migrations {
		if m.SourceID == id || m.TargetID == id {
			return fmt.Errorf("datasource is referenced by migration %s", m.ID)
		}
	}
	delete(s.datasources, id)
	return s.persistLocked()
}

func (s *Store) CreateUser(_ context.Context, u *domain.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, x := range s.users {
		if x.Username == u.Username {
			return fmt.Errorf("username already exists")
		}
	}
	s.users[u.ID] = *u
	return s.persistLocked()
}
func (s *Store) UpdateUser(_ context.Context, u *domain.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.ID]; !ok {
		return ErrNotFound
	}
	for id, x := range s.users {
		if id != u.ID && x.Username == u.Username {
			return fmt.Errorf("username already exists")
		}
	}
	s.users[u.ID] = *u
	return s.persistLocked()
}
func (s *Store) GetUser(_ context.Context, id string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := u
	return &c, nil
}
func (s *Store) GetUserByUsername(_ context.Context, username string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if u.Username == username {
			c := u
			return &c, nil
		}
	}
	return nil, ErrNotFound
}
func (s *Store) ListUsers(_ context.Context) ([]domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.User, 0, len(s.users))
	for _, u := range s.users {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	return out, nil
}

func (s *Store) GetValidationArchive(_ context.Context, taskID string) (*domain.ValidationArchive, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.validationArchives[taskID]
	if !ok {
		return nil, nil
	}
	c := v
	c.Tables = append([]domain.ValidationTableArchive(nil), v.Tables...)
	return &c, nil
}

func (s *Store) CreateValidationArchive(_ context.Context, a *domain.ValidationArchive) (bool, error) {
	if a == nil {
		return false, fmt.Errorf("nil validation archive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.validationArchives[a.TaskID]; exists {
		return false, nil
	}
	c := *a
	c.Tables = append([]domain.ValidationTableArchive(nil), a.Tables...)
	s.validationArchives[a.TaskID] = c
	return true, s.persistLocked()
}

func validationReportArchiveKey(taskID, evidenceDigest string) string {
	return taskID + "|" + strings.ToLower(strings.TrimSpace(evidenceDigest))
}

func (s *Store) GetValidationReportArchive(_ context.Context, taskID, evidenceDigest string) (*domain.ValidationReportArchiveRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.validationReportArchives[validationReportArchiveKey(taskID, evidenceDigest)]
	if !ok {
		return nil, nil
	}
	c := v
	return &c, nil
}

func (s *Store) CreateValidationReportArchive(_ context.Context, a *domain.ValidationReportArchiveRecord) (bool, error) {
	if a == nil {
		return false, fmt.Errorf("nil validation report archive record")
	}
	if strings.TrimSpace(a.TaskID) == "" || strings.TrimSpace(a.EvidenceDigest) == "" || strings.TrimSpace(a.ManifestSHA256) == "" || strings.TrimSpace(a.URI) == "" {
		return false, fmt.Errorf("validation report archive requires task, evidence digest, URI and manifest SHA-256")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := validationReportArchiveKey(a.TaskID, a.EvidenceDigest)
	if existing, ok := s.validationReportArchives[key]; ok {
		if !repository.ValidationReportArchiveEqual(&existing, a) {
			return false, repository.ErrValidationReportArchiveConflict
		}
		return false, nil
	}
	c := *a
	s.validationReportArchives[key] = c
	return true, s.persistLocked()
}

func (s *Store) ListValidationEvidencePage(_ context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]repository.ValidationEvidenceRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > 5000 {
		limit = 512
	}
	chunks := make([]domain.MigrationChunk, 0)
	for _, ch := range s.chunks {
		if ch.TaskID != taskID || ch.TableID != tableID || ch.Status != domain.ChunkSuccess {
			continue
		}
		if ch.ChunkNo < afterChunkNo || (ch.ChunkNo == afterChunkNo && ch.ID <= afterID) {
			continue
		}
		chunks = append(chunks, ch)
	}
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].ChunkNo == chunks[j].ChunkNo {
			return chunks[i].ID < chunks[j].ID
		}
		return chunks[i].ChunkNo < chunks[j].ChunkNo
	})
	if len(chunks) > limit {
		chunks = chunks[:limit]
	}
	wanted := make(map[string]int, len(chunks))
	out := make([]repository.ValidationEvidenceRow, len(chunks))
	for i, ch := range chunks {
		wanted[ch.ID] = i
		out[i] = repository.ValidationEvidenceRow{ChunkID: ch.ID, ChunkNo: ch.ChunkNo, SplitType: ch.SplitType}
	}
	latest := make(map[string]domain.ValidationResult, len(chunks))
	for _, v := range s.validations {
		if v.TaskID != taskID {
			continue
		}
		if _, ok := wanted[v.ChunkID]; !ok {
			continue
		}
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) || (v.FinishedAt.Equal(old.FinishedAt) && v.ID > old.ID) {
			latest[v.ChunkID] = v
		}
	}
	for cid, v := range latest {
		i := wanted[cid]
		out[i].ValidationID, out[i].Status = v.ID, v.Status
		out[i].SourceRows, out[i].TargetRows = v.SourceRows, v.TargetRows
		out[i].SourceChecksum, out[i].TargetChecksum = v.SourceChecksum, v.TargetChecksum
		out[i].LastError, out[i].StartedAt, out[i].FinishedAt = v.LastError, v.StartedAt, v.FinishedAt
	}
	return out, nil
}

func (s *Store) LatestValidationStatusCounts(_ context.Context, taskID string) (success, mismatch, validationError, missing int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latest := map[string]domain.ValidationResult{}
	for _, v := range s.validations {
		if v.TaskID != taskID {
			continue
		}
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) || (v.FinishedAt.Equal(old.FinishedAt) && v.ID > old.ID) {
			latest[v.ChunkID] = v
		}
	}
	for _, ch := range s.chunks {
		if ch.TaskID != taskID || ch.Status != domain.ChunkSuccess {
			continue
		}
		v, ok := latest[ch.ID]
		if !ok {
			missing++
			continue
		}
		switch v.Status {
		case domain.ValidationSuccess:
			success++
		case domain.ValidationMismatch:
			mismatch++
		case domain.ValidationError:
			validationError++
		}
	}
	return
}

var _ repository.ValidationArchiveProvider = (*Store)(nil)
var _ repository.ValidationReportArchiveProvider = (*Store)(nil)

func (s *Store) terminalValidationArchiveCandidates(now time.Time, policy repository.MetadataRetentionPolicy) []string {
	if policy.ValidationTerminalMaxAge <= 0 {
		return nil
	}
	limit := policy.ValidationArchiveTasksPerRun
	if limit <= 0 {
		limit = 8
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	hasValidation := map[string]bool{}
	for _, v := range s.validations {
		hasValidation[v.TaskID] = true
	}
	ids := make([]string, 0)
	for taskID := range hasValidation {
		if _, archived := s.validationArchives[taskID]; archived {
			continue
		}
		task, ok := s.migrations[taskID]
		if !ok {
			continue
		}
		terminal := task.Status == domain.StatusFinished || task.Status == domain.StatusFailed || task.Status == domain.StatusCancelled || task.Status == domain.StatusRolledBack
		if !terminal || task.UpdatedAt.IsZero() || now.Sub(task.UpdatedAt) <= policy.ValidationTerminalMaxAge {
			continue
		}
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

func (s *Store) PruneMetadata(ctx context.Context, policy repository.MetadataRetentionPolicy) (repository.MetadataPruneResult, error) {
	now := time.Now()
	var archivesCreated int64
	pageSize := policy.ValidationArchivePageSize
	if pageSize <= 0 || pageSize > 5000 {
		pageSize = 512
	}
	for _, taskID := range s.terminalValidationArchiveCandidates(now, policy) {
		_, created, err := repository.EnsureValidationArchive(ctx, s, taskID, pageSize)
		if err != nil && !errors.Is(err, repository.ErrNoValidationEvidence) {
			return repository.MetadataPruneResult{}, err
		}
		if created {
			archivesCreated++
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := repository.MetadataPruneResult{ValidationArchivesCreated: archivesCreated}

	if len(s.taskLogs) > 0 && (policy.TaskLogMaxAge > 0 || policy.TaskLogMaxRowsPerTask > 0) {
		sort.SliceStable(s.taskLogs, func(i, j int) bool { return s.taskLogs[i].CreatedAt.Before(s.taskLogs[j].CreatedAt) })
		kept := make([]domain.TaskLog, 0, len(s.taskLogs))
		counts := map[string]int{}
		keep := make([]bool, len(s.taskLogs))
		for i := len(s.taskLogs) - 1; i >= 0; i-- {
			v := s.taskLogs[i]
			tooOld := policy.TaskLogMaxAge > 0 && !v.CreatedAt.IsZero() && now.Sub(v.CreatedAt) > policy.TaskLogMaxAge
			overRows := policy.TaskLogMaxRowsPerTask > 0 && counts[v.TaskID] >= policy.TaskLogMaxRowsPerTask
			if tooOld || overRows {
				result.TaskLogsDeleted++
				continue
			}
			keep[i] = true
			counts[v.TaskID]++
		}
		for i, v := range s.taskLogs {
			if keep[i] {
				kept = append(kept, v)
			}
		}
		s.taskLogs = kept
	}

	if len(s.audits) > 0 && (policy.AuditMaxAge > 0 || policy.AuditMaxRows > 0) {
		sort.SliceStable(s.audits, func(i, j int) bool { return s.audits[i].CreatedAt.Before(s.audits[j].CreatedAt) })
		kept := make([]domain.AuditEvent, 0, len(s.audits))
		keep := make([]bool, len(s.audits))
		count := 0
		for i := len(s.audits) - 1; i >= 0; i-- {
			v := s.audits[i]
			tooOld := policy.AuditMaxAge > 0 && !v.CreatedAt.IsZero() && now.Sub(v.CreatedAt) > policy.AuditMaxAge
			overRows := policy.AuditMaxRows > 0 && count >= policy.AuditMaxRows
			if tooOld || overRows {
				result.AuditEventsDeleted++
				continue
			}
			keep[i] = true
			count++
		}
		for i, v := range s.audits {
			if keep[i] {
				kept = append(kept, v)
			}
		}
		s.audits = kept
	}

	if len(s.cdcPositions) > 0 && (policy.CDCPositionMaxAge > 0 || policy.CDCPositionMaxRowsPerStream > 0) {
		sort.SliceStable(s.cdcPositions, func(i, j int) bool { return s.cdcPositions[i].RecordedAt.Before(s.cdcPositions[j].RecordedAt) })
		kept := make([]domain.CDCPosition, 0, len(s.cdcPositions))
		keep := make([]bool, len(s.cdcPositions))
		seen := map[string]bool{}
		counts := map[string]int{}
		for i := len(s.cdcPositions) - 1; i >= 0; i-- {
			v := s.cdcPositions[i]
			key := v.TaskID + "\x00" + v.Direction
			isNewest := !seen[key]
			seen[key] = true
			tooOld := policy.CDCPositionMaxAge > 0 && !v.RecordedAt.IsZero() && now.Sub(v.RecordedAt) > policy.CDCPositionMaxAge
			overRows := policy.CDCPositionMaxRowsPerStream > 0 && counts[key] >= policy.CDCPositionMaxRowsPerStream
			if !isNewest && (tooOld || overRows) {
				result.CDCPositionsDeleted++
				continue
			}
			keep[i] = true
			counts[key]++
		}
		for i, v := range s.cdcPositions {
			if keep[i] {
				kept = append(kept, v)
			}
		}
		s.cdcPositions = kept
	}

	if len(s.validations) > 0 && (policy.ValidationMaxAttemptsPerChunk > 0 || policy.ValidationAttemptMaxAge > 0 || policy.ValidationTerminalMaxAge > 0) {
		groups := map[string][]domain.ValidationResult{}
		for _, v := range s.validations {
			groups[v.TaskID+"\x00"+v.ChunkID] = append(groups[v.TaskID+"\x00"+v.ChunkID], v)
		}
		for _, attempts := range groups {
			if len(attempts) == 0 {
				continue
			}
			task := s.migrations[attempts[0].TaskID]
			terminal := task.Status == domain.StatusFinished || task.Status == domain.StatusFailed || task.Status == domain.StatusCancelled || task.Status == domain.StatusRolledBack
			terminalExpired := terminal && policy.ValidationTerminalMaxAge > 0 && !task.UpdatedAt.IsZero() && now.Sub(task.UpdatedAt) > policy.ValidationTerminalMaxAge
			_, archiveExists := s.validationArchives[attempts[0].TaskID]
			terminalExpired = terminalExpired && archiveExists
			sort.SliceStable(attempts, func(i, j int) bool {
				if attempts[i].FinishedAt.Equal(attempts[j].FinishedAt) {
					return attempts[i].ID > attempts[j].ID
				}
				return attempts[i].FinishedAt.After(attempts[j].FinishedAt)
			})
			for i, v := range attempts {
				deleteAttempt := terminalExpired
				if !deleteAttempt && i > 0 && policy.ValidationAttemptMaxAge > 0 && !v.FinishedAt.IsZero() && now.Sub(v.FinishedAt) > policy.ValidationAttemptMaxAge {
					deleteAttempt = true
				}
				if !deleteAttempt && policy.ValidationMaxAttemptsPerChunk > 0 && i >= policy.ValidationMaxAttemptsPerChunk {
					deleteAttempt = true
				}
				if deleteAttempt {
					delete(s.validations, v.ID)
					result.ValidationDeleted++
				}
			}
		}
	}

	if result.TotalDeleted() == 0 {
		return result, nil
	}
	return result, s.persistLocked()
}

func (s *Store) SummarizeTaskChunks(_ context.Context, taskID string) (repository.TaskChunkSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := repository.TaskChunkSummary{Tables: map[string]repository.ChunkTableSummary{}}
	for _, c := range s.chunks {
		if c.TaskID != taskID {
			continue
		}
		t := out.Tables[c.TableID]
		out.Total++
		t.Total++
		switch c.Status {
		case domain.ChunkSuccess:
			out.Success++
			t.Success++
			out.RowsWritten += c.RowsWritten
			t.RowsWritten += c.RowsWritten
			out.BytesWritten += c.BytesWritten
			t.BytesWritten += c.BytesWritten
		case domain.ChunkPending:
			out.Pending++
			t.Pending++
		case domain.ChunkRunning:
			out.Running++
			t.Running++
		case domain.ChunkFailed:
			out.Failed++
			t.Failed++
		}
		out.ReadMS += c.LastReadMS
		t.ReadMS += c.LastReadMS
		out.WriteMS += c.LastWriteMS
		t.WriteMS += c.LastWriteMS
		if c.LastReadMS > 0 {
			out.LatencySamples++
			t.LatencySamples++
		}
		out.Tables[c.TableID] = t
	}
	return out, nil
}

// RC42 hot-path queries keep worker lease/control decisions bounded without
// copying the whole task chunk set.
func (s *Store) MaxTaskChunkNo(_ context.Context, taskID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	maxNo := 0
	for _, c := range s.chunks {
		if c.TaskID == taskID && c.ChunkNo > maxNo {
			maxNo = c.ChunkNo
		}
	}
	return maxNo, nil
}

func (s *Store) CountTableRunnable(_ context.Context, taskID, tableID string) (repository.TableRunnableCounts, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out repository.TableRunnableCounts
	for _, c := range s.chunks {
		if c.TaskID != taskID || c.TableID != tableID {
			continue
		}
		switch c.Status {
		case domain.ChunkPending:
			out.Pending++
		case domain.ChunkRunning:
			out.Running++
		}
	}
	return out, nil
}

func cloneHotChunk(c domain.MigrationChunk) domain.MigrationChunk {
	c.PlacementHint = maps.Clone(c.PlacementHint)
	c.FaultDomain = maps.Clone(c.FaultDomain)
	return c
}

func (s *Store) ListPendingTableChunks(_ context.Context, taskID, tableID string) ([]domain.MigrationChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.MigrationChunk, 0)
	for _, c := range s.chunks {
		if c.TaskID == taskID && c.TableID == tableID && c.Status == domain.ChunkPending {
			out = append(out, cloneHotChunk(c))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChunkNo < out[j].ChunkNo })
	return out, nil
}

func (s *Store) ListRunningTopologyChunks(_ context.Context, taskID, topologyID string) ([]domain.MigrationChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.MigrationChunk, 0)
	for _, c := range s.chunks {
		if c.TaskID == taskID && c.Status == domain.ChunkRunning && c.TopologyID == topologyID {
			out = append(out, cloneHotChunk(c))
		}
	}
	return out, nil
}

func (s *Store) ListRunningFaultDomainChunks(_ context.Context, taskID, scope, value string) ([]domain.MigrationChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.MigrationChunk, 0)
	for _, c := range s.chunks {
		if c.TaskID == taskID && c.Status == domain.ChunkRunning && c.FaultDomain[scope] == value {
			out = append(out, cloneHotChunk(c))
		}
	}
	return out, nil
}

func (s *Store) ListTableChunksPage(_ context.Context, taskID, tableID string, afterChunkNo int, afterID string, limit int) ([]domain.MigrationChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 512
	}
	out := make([]domain.MigrationChunk, 0)
	for _, ch := range s.chunks {
		if ch.TaskID != taskID || ch.TableID != tableID {
			continue
		}
		if ch.ChunkNo < afterChunkNo || (ch.ChunkNo == afterChunkNo && ch.ID <= afterID) {
			continue
		}
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ChunkNo == out[j].ChunkNo {
			return out[i].ID < out[j].ID
		}
		return out[i].ChunkNo < out[j].ChunkNo
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *Store) HasValidationResults(_ context.Context, taskID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.validations {
		if v.TaskID == taskID {
			return true, nil
		}
	}
	return false, nil
}
func (s *Store) FirstInvalidSuccessfulChunk(_ context.Context, taskID string) (string, domain.ValidationStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latest := map[string]domain.ValidationResult{}
	for _, v := range s.validations {
		if v.TaskID != taskID {
			continue
		}
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) {
			latest[v.ChunkID] = v
		}
	}
	chunks := make([]domain.MigrationChunk, 0)
	for _, ch := range s.chunks {
		if ch.TaskID == taskID && ch.Status == domain.ChunkSuccess {
			chunks = append(chunks, ch)
		}
	}
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].TableID != chunks[j].TableID {
			return chunks[i].TableID < chunks[j].TableID
		}
		if chunks[i].ChunkNo != chunks[j].ChunkNo {
			return chunks[i].ChunkNo < chunks[j].ChunkNo
		}
		return chunks[i].ID < chunks[j].ID
	})
	for _, ch := range chunks {
		v, ok := latest[ch.ID]
		if !ok {
			return ch.ID, "", nil
		}
		if v.Status != domain.ValidationSuccess {
			return ch.ID, v.Status, nil
		}
	}
	return "", "", nil
}
func (s *Store) ListRepairableValidationChunkIDs(_ context.Context, taskID string, limit int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latest := map[string]domain.ValidationResult{}
	for _, v := range s.validations {
		if v.TaskID != taskID {
			continue
		}
		old, ok := latest[v.ChunkID]
		if !ok || v.FinishedAt.After(old.FinishedAt) {
			latest[v.ChunkID] = v
		}
	}
	out := []string{}
	for id, v := range latest {
		if v.Status == domain.ValidationMismatch || v.Status == domain.ValidationError {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

var _ repository.ValidationHotPathProvider = (*Store)(nil)
