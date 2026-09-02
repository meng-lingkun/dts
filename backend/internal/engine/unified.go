package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"qmigration/backend/internal/domain"
)

// UnifiedAdapter is the only execution engine exposed by QMigration.
//
// It deliberately does not invoke DataX, SeaTunnel, Flink CDC, Debezium or
// Canal.  Their useful design ideas are implemented inside QMigration's own
// data plane: connector SPI, bounded channels/backpressure, chunk splitting,
// transactional CDC apply, durable offsets/checkpoints and schema history.
// Vendor log readers remain QMigration-owned binaries/processes so they can be
// supervised independently by Workers without introducing a third-party
// runtime dependency.
type UnifiedAdapter struct{}

func NewUnified() *UnifiedAdapter    { return &UnifiedAdapter{} }
func (*UnifiedAdapter) Name() string { return "qmigration" }
func (*UnifiedAdapter) Info(_ context.Context) domain.EngineInfo {
	return domain.EngineInfo{
		Name:      "qmigration",
		Available: true,
		Modes:     []string{"FULL", "FULL_AND_INCREMENTAL", "INCREMENTAL"},
		Note:      "QMigration Unified Engine: built-in full-load pipeline, checkpoint/backpressure, validation and native CDC; no DataX/SeaTunnel/Flink/Debezium/Canal runtime required",
	}
}

func (*UnifiedAdapter) Render(ctx context.Context, task *domain.MigrationTask, src, dst domain.DataSource, tables []domain.MigrationTable) (*domain.RuntimeSpec, error) {
	if task == nil {
		return nil, fmt.Errorf("nil migration task")
	}
	// FULL execution is claimed as native QMigration chunks and does not spawn
	// an external process.  Render a diagnostic plan for the API only.
	if task.Mode == domain.ModeFull {
		plan := map[string]any{
			"engine":      "qmigration",
			"data_plane":  "chunk-pipeline",
			"source_type": src.Type,
			"target_type": dst.Type,
			"parallelism": task.Parallelism,
			"chunk_rows":  task.ChunkRows,
			"batch_rows":  task.BatchRows,
			"tables":      tables,
			"features": []string{
				"connector-spi", "bounded-channel-backpressure", "adaptive-chunk",
				"durable-checkpoint", "rate-limit", "validation", "topology-affinity",
			},
		}
		b, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return nil, err
		}
		return &domain.RuntimeSpec{Engine: "qmigration", Format: "json", Filename: "qmigration-plan.json", Content: string(b)}, nil
	}

	// CDC is selected by source protocol, not by a user-selected third-party
	// engine.  These readers are part of QMigration and feed the same unified
	// CDCEvent/apply/checkpoint pipeline.
	var cfg *domain.RuntimeSpec
	var err error
	switch {
	case src.Type == domain.DataSourceTiDB:
		cfg, err = NewNativeTiDBCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceOceanBase:
		cfg, err = NewNativeOceanBaseCDC().Render(ctx, task, src, dst, tables)
	case src.Type.IsMySQLFamily():
		cfg, err = NewNativeMySQLCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceGaussDB:
		cfg, err = NewNativeGaussDBCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceOpenGauss:
		cfg, err = NewNativeOpenGaussCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceKingbase:
		cfg, err = NewNativeKingbaseCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceGBase:
		cfg, err = NewNativeGBase8aCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceGBase8s:
		cfg, err = NewNativeGBase8sCDC().Render(ctx, task, src, dst, tables)
	case src.Type.IsPostgreSQLFamily():
		cfg, err = NewNativePostgresCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceSQLServer:
		cfg, err = NewNativeSQLServerCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceDB2:
		cfg, err = NewNativeDB2CDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceDameng:
		cfg, err = NewNativeDamengCDC().Render(ctx, task, src, dst, tables)
	case src.Type == domain.DataSourceOracle:
		cfg, err = NewNativeOracleCDC().Render(ctx, task, src, dst, tables)
	default:
		return nil, fmt.Errorf("QMigration unified CDC reader is not implemented yet for source %s", src.Type)
	}
	if err != nil {
		return nil, err
	}
	cfg.Engine = "qmigration"
	cfg.Filename = "qmigration-cdc.json"
	return cfg, nil
}
