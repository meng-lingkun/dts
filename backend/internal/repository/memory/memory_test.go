package memory

import (
	"context"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"strings"
	"testing"
	"time"
)

func TestClaimChunkHonorsParallelismAndLease(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	m := domain.MigrationTask{ID: "m1", Status: domain.StatusFullMigrating, Parallelism: 1, CreatedAt: now}
	if err := s.CreateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	chunks := []domain.MigrationChunk{{ID: "c1", TaskID: "m1", TableID: "t1", ChunkNo: 1, Status: domain.ChunkPending}, {ID: "c2", TaskID: "m1", TableID: "t1", ChunkNo: 2, Status: domain.ChunkPending}}
	if err := s.CreateChunks(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	c, err := s.ClaimChunk(ctx, "w1", 20*time.Millisecond, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if c.ID != "c1" || c.WorkerID != "w1" {
		t.Fatalf("unexpected claim %+v", c)
	}
	if _, err := s.ClaimChunk(ctx, "w2", time.Minute, []string{"native"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("expected no chunk due parallelism, got %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	c2, err := s.ClaimChunk(ctx, "w2", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if c2.ID != "c1" || c2.RetryCount != 1 {
		t.Fatalf("expired chunk should be reclaimed first: %+v", c2)
	}
}

func TestRenewLeaseChecksOwner(t *testing.T) {
	ctx := context.Background()
	s := New()
	m := domain.MigrationTask{ID: "m1", Status: domain.StatusFullMigrating, Parallelism: 1}
	_ = s.CreateMigration(ctx, &m)
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{{ID: "c1", TaskID: "m1", Status: domain.ChunkPending}})
	_, _ = s.ClaimChunk(ctx, "w1", time.Minute, []string{"native"})
	if err := s.RenewChunkLease(ctx, "c1", "w2", time.Minute); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("expected owner error, got %v", err)
	}
}

func TestClaimChunkFiltersWorkerCapabilities(t *testing.T) {
	ctx := context.Background()
	s := New()
	m := domain.MigrationTask{ID: "m-cap", Status: domain.StatusFullMigrating, FullEngine: "datax", Parallelism: 1}
	_ = s.CreateMigration(ctx, &m)
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{{ID: "c-cap", TaskID: m.ID, Status: domain.ChunkPending}})
	if _, err := s.ClaimChunk(ctx, "native-worker", time.Minute, []string{"native"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("native worker should not claim datax: %v", err)
	}
	c, err := s.ClaimChunk(ctx, "datax-worker", time.Minute, []string{"datax"})
	if err != nil || c.ID != "c-cap" {
		t.Fatalf("datax worker should claim: %+v %v", c, err)
	}
}

func TestPersistentStoreRestoresCDCPositions(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/state.json"
	s, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	m := domain.MigrationTask{ID: "m-cdc", Status: domain.StatusCDCCatchingUp, CreatedAt: time.Now()}
	if err := s.CreateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	p := domain.CDCPosition{ID: "p1", TaskID: m.ID, Direction: "forward", PositionType: "GTID", PositionValue: "uuid:1-9", LagMS: 12, RecordedAt: time.Now()}
	if err := s.CreateCDCPosition(ctx, &p); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewPersistent(path)
	if err != nil {
		t.Fatal(err)
	}
	items, err := reopened.ListCDCPositions(ctx, m.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].PositionValue != p.PositionValue || items[0].Direction != "forward" {
		t.Fatalf("CDC position was not restored: %+v", items)
	}
}

func TestChunkCursorPersistsAcrossLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	m := domain.MigrationTask{ID: "m-keyset", Status: domain.StatusFullMigrating, Parallelism: 1, CreatedAt: now, UpdatedAt: now}
	if err := s.CreateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "t-keyset", TaskID: m.ID, Engine: "native", Status: "READY"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateChunks(ctx, []domain.MigrationChunk{{ID: "c-keyset", TaskID: m.ID, TableID: "t-keyset", ChunkNo: 1, SplitType: "PRIMARY_KEY_KEYSET", Status: domain.ChunkPending}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimChunk(ctx, "w1", time.Minute, []string{"native"}); err != nil {
		t.Fatal(err)
	}
	cursor := `[{"Null":false,"Raw":"YWJj"}]`
	if err := s.UpdateChunkProgress(ctx, "c-keyset", "w1", domain.ChunkProgress{CursorJSON: cursor, RowsRead: 10, RowsWritten: 10, BytesRead: 100, BytesWritten: 100}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetChunk(ctx, "c-keyset")
	if err != nil {
		t.Fatal(err)
	}
	if got.CursorJSON != cursor || got.RowsWritten != 10 || got.BytesWritten != 100 {
		t.Fatalf("chunk progress=%+v", got)
	}
	if err := s.UpdateChunkProgress(ctx, "c-keyset", "w2", domain.ChunkProgress{CursorJSON: "bad"}); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("expected lease owner error, got %v", err)
	}
}

func TestClaimChunkPrefersWorkerAffinityLabels(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	if err := s.UpsertWorker(ctx, &domain.Worker{ID: "w-zone-a", Hostname: "w", Status: "ONLINE", Labels: map[string]string{"zone": "az-a"}, Capabilities: []string{"native"}, LastHeartbeat: now}); err != nil {
		t.Fatal(err)
	}
	// Intentionally make the non-local task sort first by ID. Affinity must win.
	for _, task := range []domain.MigrationTask{
		{ID: "a-remote", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 1, WorkerSelector: map[string]string{"zone": "az-b"}, WorkerAffinity: "PREFERRED"},
		{ID: "z-local", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 1, WorkerSelector: map[string]string{"zone": "az-a"}, WorkerAffinity: "PREFERRED"},
	} {
		copyTask := task
		if err := s.CreateMigration(ctx, &copyTask); err != nil {
			t.Fatal(err)
		}
		tableID := task.ID + "-table"
		if err := s.CreateMigrationTable(ctx, &domain.MigrationTable{ID: tableID, TaskID: task.ID, Engine: "native"}); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateChunks(ctx, []domain.MigrationChunk{{ID: task.ID + "-chunk", TaskID: task.ID, TableID: tableID, ChunkNo: 1, Status: domain.ChunkPending}}); err != nil {
			t.Fatal(err)
		}
	}
	chunk, err := s.ClaimChunk(ctx, "w-zone-a", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if chunk.TaskID != "z-local" {
		t.Fatalf("expected local-affinity task, got %+v", chunk)
	}
}

func TestClaimChunkRequiredAffinityRejectsMismatch(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	_ = s.UpsertWorker(ctx, &domain.Worker{ID: "w-zone-a", Hostname: "w", Status: "ONLINE", Labels: map[string]string{"zone": "az-a"}, Capabilities: []string{"native"}, LastHeartbeat: now})
	m := domain.MigrationTask{ID: "required", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 1, WorkerSelector: map[string]string{"zone": "az-b"}, WorkerAffinity: "REQUIRED"}
	_ = s.CreateMigration(ctx, &m)
	_ = s.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "required-table", TaskID: m.ID, Engine: "native"})
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{{ID: "required-chunk", TaskID: m.ID, TableID: "required-table", ChunkNo: 1, Status: domain.ChunkPending}})
	if _, err := s.ClaimChunk(ctx, "w-zone-a", time.Minute, []string{"native"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("expected required affinity to reject worker, got %v", err)
	}
}

func TestClaimChunkHonorsEffectiveParallelism(t *testing.T) {
	ctx := context.Background()
	s := New()
	m := domain.MigrationTask{ID: "m-effective", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 1}
	if err := s.CreateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "t-effective", TaskID: m.ID, Engine: "native"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "c-effective-1", TaskID: m.ID, TableID: "t-effective", ChunkNo: 1, Status: domain.ChunkPending},
		{ID: "c-effective-2", TaskID: m.ID, TableID: "t-effective", ChunkNo: 2, Status: domain.ChunkPending},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimChunk(ctx, "w1", time.Minute, []string{"native"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimChunk(ctx, "w2", time.Minute, []string{"native"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("effective parallelism should limit new claims, got %v", err)
	}
}

func TestClaimChunkPrefersTopologyPlacementHint(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	if err := s.UpsertWorker(ctx, &domain.Worker{ID: "w-ob-a", Status: "ONLINE", Labels: map[string]string{"ob_zone": "zone-a"}, Capabilities: []string{"native"}, LastHeartbeat: now}); err != nil {
		t.Fatal(err)
	}
	m := domain.MigrationTask{ID: "m-topology", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 2, EffectiveParallelism: 2}
	if err := s.CreateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMigrationTable(ctx, &domain.MigrationTable{ID: "t-topology", TaskID: m.ID, Engine: "native"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "a-remote", TaskID: m.ID, TableID: "t-topology", ChunkNo: 1, Status: domain.ChunkPending, PlacementHint: map[string]string{"ob_zone": "zone-b"}},
		{ID: "z-local", TaskID: m.ID, TableID: "t-topology", ChunkNo: 2, Status: domain.ChunkPending, PlacementHint: map[string]string{"ob_zone": "zone-a"}},
	}); err != nil {
		t.Fatal(err)
	}
	chunk, err := s.ClaimChunk(ctx, "w-ob-a", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if chunk.ID != "z-local" {
		t.Fatalf("expected topology-local chunk, got %+v", chunk)
	}
}

func TestYieldChunkAtomicallyReplacesOwnedRunningChunk(t *testing.T) {
	ctx := context.Background()
	s := New()
	orig := domain.MigrationChunk{ID: "yield-running", TaskID: "t", TableID: "tb", ChunkNo: 1, Status: domain.ChunkRunning, WorkerID: "w1", Start: 1, End: 100}
	if err := s.CreateChunks(ctx, []domain.MigrationChunk{orig}); err != nil {
		t.Fatal(err)
	}
	completed := orig
	completed.Status = domain.ChunkSuccess
	completed.WorkerID = ""
	completed.End = 40
	created := []domain.MigrationChunk{{ID: "yield-rest", TaskID: "t", TableID: "tb", ChunkNo: 2, Status: domain.ChunkPending, Start: 41, End: 100}}
	if err := s.YieldChunk(ctx, "wrong", &completed, created); !errors.Is(err, repository.ErrLeaseOwner) {
		t.Fatalf("owner error=%v", err)
	}
	if err := s.YieldChunk(ctx, "w1", &completed, created); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ListChunks(ctx, "t")
	if len(got) != 2 || got[0].Status != domain.ChunkSuccess || got[1].Start != 41 {
		t.Fatalf("chunks=%+v", got)
	}
}

func TestRC35ClaimChunkPrefersHealthyTopology(t *testing.T) {
	ctx := context.Background()
	s := New()
	m := domain.MigrationTask{ID: "rc35-health-task", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 4}
	if err := s.CreateMigration(ctx, &m); err != nil {
		t.Fatal(err)
	}
	table := domain.MigrationTable{ID: "rc35-health-table", TaskID: m.ID, Engine: "native", TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-slow": {Health: "DEGRADED", Samples: 10},
		"dn-fast": {Health: "HEALTHY", Samples: 10},
	}}
	if err := s.CreateMigrationTable(ctx, &table); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "a-slow", TaskID: m.ID, TableID: table.ID, ChunkNo: 1, Status: domain.ChunkPending, TopologyID: "dn-slow"},
		{ID: "z-fast", TaskID: m.ID, TableID: table.ID, ChunkNo: 2, Status: domain.ChunkPending, TopologyID: "dn-fast"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ClaimChunk(ctx, "w", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "z-fast" {
		t.Fatalf("expected healthy topology first, got %+v", got)
	}
}

func TestRC35TopologyConcurrencyAndCircuitGate(t *testing.T) {
	ctx := context.Background()
	s := New()
	m := domain.MigrationTask{ID: "rc35-cap-task", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 8, EffectiveParallelism: 8}
	_ = s.CreateMigration(ctx, &m)
	table := domain.MigrationTable{ID: "rc35-cap-table", TaskID: m.ID, Engine: "native", TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-degraded": {Health: "DEGRADED", Samples: 10},
		"dn-open":     {Health: "CIRCUIT_OPEN", Samples: 10},
		"dn-probe":    {Health: "HALF_OPEN", Samples: 10},
	}}
	_ = s.CreateMigrationTable(ctx, &table)
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "running-degraded", TaskID: m.ID, TableID: table.ID, ChunkNo: 1, Status: domain.ChunkRunning, WorkerID: "w0", LeaseUntil: time.Now().Add(time.Minute), TopologyID: "dn-degraded"},
		{ID: "pending-degraded", TaskID: m.ID, TableID: table.ID, ChunkNo: 2, Status: domain.ChunkPending, TopologyID: "dn-degraded"},
		{ID: "pending-open", TaskID: m.ID, TableID: table.ID, ChunkNo: 3, Status: domain.ChunkPending, TopologyID: "dn-open"},
		{ID: "pending-probe", TaskID: m.ID, TableID: table.ID, ChunkNo: 4, Status: domain.ChunkPending, TopologyID: "dn-probe"},
	})
	got, err := s.ClaimChunk(ctx, "w1", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "pending-probe" {
		t.Fatalf("expected half-open probe as only eligible topology, got %+v", got)
	}
	if _, err := s.ClaimChunk(ctx, "w2", time.Minute, []string{"native"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("second claim should be blocked by degraded/open/half-open caps, got %v", err)
	}
}

func TestRC35HealthyRemoteOutranksDegradedLocal(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	_ = s.UpsertWorker(ctx, &domain.Worker{ID: "rc35-zone-a", Status: "ONLINE", Labels: map[string]string{"zone": "a"}, Capabilities: []string{"native"}, LastHeartbeat: now})
	m := domain.MigrationTask{ID: "rc35-health-placement", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 4, EffectiveParallelism: 4}
	_ = s.CreateMigration(ctx, &m)
	table := domain.MigrationTable{ID: "rc35-health-placement-table", TaskID: m.ID, Engine: "native", TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-local":  {Health: "DEGRADED", Samples: 10},
		"dn-remote": {Health: "HEALTHY", Samples: 10},
	}}
	_ = s.CreateMigrationTable(ctx, &table)
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "local-degraded", TaskID: m.ID, TableID: table.ID, ChunkNo: 1, Status: domain.ChunkPending, TopologyID: "dn-local", PlacementHint: map[string]string{"zone": "a"}},
		{ID: "remote-healthy", TaskID: m.ID, TableID: table.ID, ChunkNo: 2, Status: domain.ChunkPending, TopologyID: "dn-remote", PlacementHint: map[string]string{"zone": "b"}},
	})
	got, err := s.ClaimChunk(ctx, "rc35-zone-a", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "remote-healthy" {
		t.Fatalf("expected healthy remote topology to outrank degraded local placement, got %+v", got)
	}
}

func TestRC39FaultDomainRiskPrefersIndependentDomainAndCapsCascade(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION", "true")
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_MAX_CONCURRENCY", "1")
	ctx := context.Background()
	s := New()
	m := domain.MigrationTask{ID: "rc39-domain-task", Status: domain.StatusFullMigrating, FullEngine: "native", Parallelism: 8, EffectiveParallelism: 8}
	_ = s.CreateMigration(ctx, &m)
	table := domain.MigrationTable{ID: "rc39-domain-table", TaskID: m.ID, Engine: "native", TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-a1": {Health: "HEALTHY"},
		"dn-a2": {Health: "CIRCUIT_OPEN"},
		"dn-b1": {Health: "HEALTHY"},
	}}
	_ = s.CreateMigrationTable(ctx, &table)
	zoneA := map[string]string{"region": "sg", "zone": "sg/a"}
	zoneB := map[string]string{"region": "us", "zone": "us/b"}
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "a-candidate", TaskID: m.ID, TableID: table.ID, ChunkNo: 1, Status: domain.ChunkPending, TopologyID: "dn-a1", FaultDomain: zoneA},
		{ID: "a-open-peer", TaskID: m.ID, TableID: table.ID, ChunkNo: 2, Status: domain.ChunkPending, TopologyID: "dn-a2", FaultDomain: zoneA},
		{ID: "z-independent", TaskID: m.ID, TableID: table.ID, ChunkNo: 3, Status: domain.ChunkPending, TopologyID: "dn-b1", FaultDomain: zoneB},
	})
	got, err := s.ClaimChunk(ctx, "w1", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "z-independent" {
		t.Fatalf("expected independent fault domain first, got %+v", got)
	}
	// Once the independent domain is occupied, zone A can still make bounded progress.
	got, err = s.ClaimChunk(ctx, "w2", time.Minute, []string{"native"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "a-candidate" {
		t.Fatalf("expected healthy topology in risky domain as bounded fallback, got %+v", got)
	}
	// Critical-domain cap=1 blocks additional work in the same correlated domain.
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{{ID: "a-second", TaskID: m.ID, TableID: table.ID, ChunkNo: 4, Status: domain.ChunkPending, TopologyID: "dn-a1", FaultDomain: zoneA}})
	if _, err := s.ClaimChunk(ctx, "w3", time.Minute, []string{"native"}); !errors.Is(err, repository.ErrNoChunk) {
		t.Fatalf("expected correlated domain cap to block second claim, got %v", err)
	}
}

func TestRC41MetadataPruneBoundsHistoryAndPreservesNewestCDCCheckpoint(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	for i := 0; i < 5; i++ {
		_ = s.CreateTaskLog(ctx, &domain.TaskLog{ID: fmt.Sprintf("log-%d", i), TaskID: "task-a", Level: "INFO", Message: "x", CreatedAt: now.Add(time.Duration(i-10) * time.Hour)})
		_ = s.CreateAuditEvent(ctx, &domain.AuditEvent{ID: fmt.Sprintf("audit-%d", i), Actor: "u", Action: "A", ResourceType: "migration", CreatedAt: now.Add(time.Duration(i-10) * time.Hour)})
	}
	// Both checkpoints are older than retention, but the newest stream checkpoint
	// is correctness state and must survive.
	for i := 0; i < 4; i++ {
		_ = s.CreateCDCPosition(ctx, &domain.CDCPosition{ID: fmt.Sprintf("pos-%d", i), TaskID: "task-a", Direction: "forward", PositionType: "GTID", PositionValue: fmt.Sprintf("p%d", i), RecordedAt: now.Add(time.Duration(i-20) * time.Hour)})
	}
	res, err := s.PruneMetadata(ctx, repository.MetadataRetentionPolicy{
		TaskLogMaxAge: 8 * time.Hour, TaskLogMaxRowsPerTask: 2,
		AuditMaxAge: 8 * time.Hour, AuditMaxRows: 2,
		CDCPositionMaxAge: time.Hour, CDCPositionMaxRowsPerStream: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.TaskLogsDeleted != 3 || res.AuditEventsDeleted != 3 || res.CDCPositionsDeleted != 3 {
		t.Fatalf("unexpected prune result: %+v", res)
	}
	logs, _ := s.ListTaskLogs(ctx, "task-a", 10)
	if len(logs) != 2 {
		t.Fatalf("logs=%d %+v", len(logs), logs)
	}
	audits, _ := s.ListAuditEvents(ctx, 10)
	if len(audits) != 2 {
		t.Fatalf("audits=%d %+v", len(audits), audits)
	}
	positions, _ := s.ListCDCPositions(ctx, "task-a", 10)
	if len(positions) != 1 || positions[0].PositionValue != "p3" {
		t.Fatalf("newest CDC checkpoint must survive retention: %+v", positions)
	}
}

func TestRC41CDCPositionEmergencyCompactionPreservesEveryStreamHead(t *testing.T) {
	old := time.Now().Add(-24 * time.Hour)
	in := make([]domain.CDCPosition, 0, 20)
	// A paused task has only one old checkpoint; a busy task contributes all of
	// the newest records. A naive global tail slice would evict paused/forward.
	in = append(in, domain.CDCPosition{ID: "paused", TaskID: "paused", Direction: "forward", PositionValue: "must-survive", RecordedAt: old})
	for i := 0; i < 19; i++ {
		in = append(in, domain.CDCPosition{ID: fmt.Sprintf("busy-%d", i), TaskID: "busy", Direction: "forward", RecordedAt: old.Add(time.Duration(i+1) * time.Minute)})
	}
	out := compactCDCPositionsHardLimit(in, 5)
	found := false
	for _, p := range out {
		if p.ID == "paused" {
			found = true
		}
	}
	if !found {
		t.Fatalf("paused stream head was evicted: %+v", out)
	}
}

func TestRC41ChunkSummaryMatchesTaskAndTableCounts(t *testing.T) {
	ctx := context.Background()
	s := New()
	_ = s.CreateChunks(ctx, []domain.MigrationChunk{
		{ID: "s1", TaskID: "sum", TableID: "t1", Status: domain.ChunkSuccess, RowsWritten: 10, BytesWritten: 100, LastReadMS: 4, LastWriteMS: 6},
		{ID: "s2", TaskID: "sum", TableID: "t1", Status: domain.ChunkRunning, LastReadMS: 8, LastWriteMS: 10},
		{ID: "s3", TaskID: "sum", TableID: "t2", Status: domain.ChunkPending},
		{ID: "other", TaskID: "other", TableID: "t1", Status: domain.ChunkFailed},
	})
	summary, err := s.SummarizeTaskChunks(ctx, "sum")
	if err != nil {
		t.Fatal(err)
	}
	if summary.Total != 3 || summary.Success != 1 || summary.Running != 1 || summary.Pending != 1 || summary.RowsWritten != 10 || summary.BytesWritten != 100 {
		t.Fatalf("summary=%+v", summary)
	}
	if summary.Tables["t1"].Total != 2 || summary.Tables["t2"].Pending != 1 || summary.LatencySamples != 2 {
		t.Fatalf("table summary=%+v", summary.Tables)
	}
}

func TestRC44ValidationRetentionCompactsAttemptsAndExpiresOnlyTerminalHistory(t *testing.T) {
	ctx := context.Background()
	s := New()
	now := time.Now()
	active := &domain.MigrationTask{ID: "active-validation", Name: "active", Status: domain.StatusValidating, CreatedAt: now.Add(-72 * time.Hour), UpdatedAt: now.Add(-72 * time.Hour)}
	terminal := &domain.MigrationTask{ID: "terminal-validation", Name: "terminal", Status: domain.StatusFinished, CreatedAt: now.Add(-240 * time.Hour), UpdatedAt: now.Add(-200 * time.Hour)}
	if err := s.CreateMigration(ctx, active); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMigration(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		finished := now.Add(time.Duration(i-48) * time.Hour)
		if err := s.CreateValidationResult(ctx, &domain.ValidationResult{ID: fmt.Sprintf("active-v%d", i), TaskID: active.ID, TableID: "t", ChunkID: "c", Status: domain.ValidationSuccess, StartedAt: finished.Add(-time.Minute), FinishedAt: finished}); err != nil {
			t.Fatal(err)
		}
	}
	terminalFinished := now.Add(-190 * time.Hour)
	if err := s.CreateValidationResult(ctx, &domain.ValidationResult{ID: "terminal-latest", TaskID: terminal.ID, TableID: "t", ChunkID: "c", Status: domain.ValidationSuccess, StartedAt: terminalFinished.Add(-time.Minute), FinishedAt: terminalFinished}); err != nil {
		t.Fatal(err)
	}
	res, err := s.PruneMetadata(ctx, repository.MetadataRetentionPolicy{
		ValidationMaxAttemptsPerChunk: 1,
		ValidationAttemptMaxAge:       time.Hour,
		ValidationTerminalMaxAge:      168 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ValidationDeleted != 3 {
		t.Fatalf("expected two superseded active attempts plus one expired terminal result, got %+v", res)
	}
	items, err := s.ListValidationResults(ctx, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != "active-v2" {
		t.Fatalf("active latest validation result must survive compaction: %+v", items)
	}
	terminalItems, err := s.ListValidationResults(ctx, terminal.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminalItems) != 0 {
		t.Fatalf("expired terminal validation detail should be pruned: %+v", terminalItems)
	}
}

func TestRC47ValidationReportArchiveRegistryIsImmutable(t *testing.T) {
	ctx := context.Background()
	s := New()
	rec := &domain.ValidationReportArchiveRecord{
		TaskID: "m-report", EvidenceDigest: strings.Repeat("a", 64), URI: "s3://reports/m-report/a", Bucket: "reports", Prefix: "m-report/a",
		ManifestSHA256: strings.Repeat("b", 64), PublicSignatureAlgorithm: "Ed25519", PublicSignatureKeyID: "key-1",
		PublicKeyEd25519: "cHVi", PublicKeyFingerprintSHA256: strings.Repeat("c", 64), ObjectLockMode: "COMPLIANCE", RetainUntil: "2027-09-02T00:00:00Z", LegalHold: true, CommittedAt: time.Now().UTC(),
	}
	created, err := s.CreateValidationReportArchive(ctx, rec)
	if err != nil || !created {
		t.Fatalf("create registry: created=%v err=%v", created, err)
	}
	got, err := s.GetValidationReportArchive(ctx, rec.TaskID, strings.ToUpper(rec.EvidenceDigest))
	if err != nil || got == nil || !repository.ValidationReportArchiveEqual(got, rec) {
		t.Fatalf("registry round trip: got=%+v err=%v", got, err)
	}
	created, err = s.CreateValidationReportArchive(ctx, rec)
	if err != nil || created {
		t.Fatalf("idempotent registry: created=%v err=%v", created, err)
	}
	bad := *rec
	bad.URI = "s3://other/changed"
	if _, err := s.CreateValidationReportArchive(ctx, &bad); !errors.Is(err, repository.ErrValidationReportArchiveConflict) {
		t.Fatalf("immutable registry conflict not detected: %v", err)
	}
}
