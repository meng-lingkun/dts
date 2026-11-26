package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
	"dts/backend/internal/repository"
)

var ErrFreezeRequired = errors.New("unexpired write-freeze evidence and source/target watermark proof are required")
var ErrWatermarkBehind = errors.New("target has not applied the final source watermark; keep capture running and retry after catch-up")

func lifecycleOperation(m *domain.MigrationTask, direction string) *domain.LifecycleOperation {
	if direction == "reverse" {
		return m.RollbackOperation
	}
	return m.CutoverOperation
}
func putLifecycleOperation(m *domain.MigrationTask, direction string, o *domain.LifecycleOperation) {
	if direction == "reverse" {
		m.RollbackOperation = o
	} else {
		m.CutoverOperation = o
	}
}

func (s *Service) mutateMigration(ctx context.Context, id string, change func(*domain.MigrationTask) error) (*domain.MigrationTask, error) {
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m, err := s.repo.GetMigration(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = change(m); err != nil {
			return nil, err
		}
		m.UpdatedAt = time.Now().UTC()
		if err = s.repo.UpdateMigration(ctx, m); errors.Is(err, repository.ErrConflict) {
			continue
		} else if err != nil {
			return nil, err
		}
		return m, nil
	}
	return nil, repository.ErrConflict
}

func sourceFingerprint(ds domain.DataSource, m *domain.MigrationTask) string {
	// Password rotation is allowed; endpoint/selection changes invalidate proof.
	b, _ := json.Marshal([]any{ds.ID, ds.Type, ds.Host, ds.Port, ds.Database, ds.Schema, ds.CDCURL, ds.JDBCURL, ds.TLSMode, ds.TLSServerName, m.Tables})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (s *Service) finalSourcePosition(ctx context.Context, m *domain.MigrationTask, direction string) (domain.CDCPosition, string, error) {
	return s.finalPositionWithProof(ctx, m, direction, "")
}

func (s *Service) finalPositionWithProof(ctx context.Context, m *domain.MigrationTask, direction, proof string) (domain.CDCPosition, string, error) {
	id := m.SourceID
	if direction == "reverse" {
		id = m.TargetID
	}
	ds, err := s.repo.GetDataSource(ctx, id)
	if err != nil {
		return domain.CDCPosition{}, "", err
	}
	raw, err := s.connectors.New(*ds)
	if err != nil {
		return domain.CDCPosition{}, "", err
	}
	defer raw.Close()
	source, ok := raw.(connector.CDCSource)
	if !ok {
		return domain.CDCPosition{}, "", errors.New("source cannot independently report a final CDC watermark")
	}
	quiescent, ok := raw.(connector.CDCQuiescenceVerifier)
	if !ok {
		return domain.CDCPosition{}, "", errors.New("source cannot verify in-flight write transactions for final cutover")
	}
	if proof != "" {
		freezer, ok := raw.(connector.CDCSelectedTableFreezer)
		if !ok {
			return domain.CDCPosition{}, "", ErrFreezeRequired
		}
		_, selection, err := s.freezeSelection(ctx, m.ID, direction)
		if err != nil {
			return domain.CDCPosition{}, "", err
		}
		lockID, digest, valid := strings.Cut(proof, ":")
		if !valid || selection != digest {
			return domain.CDCPosition{}, "", fmt.Errorf("%w: prepared table selection changed", ErrFreezeRequired)
		}
		if err := freezer.VerifyTableFreeze(ctx, lockID); err != nil {
			return domain.CDCPosition{}, "", fmt.Errorf("%w: %v", ErrFreezeRequired, err)
		}
	} else if err := quiescent.VerifyCDCQuiescence(ctx); err != nil {
		return domain.CDCPosition{}, "", err
	}
	var p *domain.CDCPosition
	if head, ok := raw.(connector.CDCFreezeHeadSource); ok {
		p, err = head.CurrentFreezeHead(ctx)
	} else {
		p, err = source.CurrentCDCPosition(ctx)
	}
	if err != nil {
		return domain.CDCPosition{}, "", err
	}
	if p == nil {
		return domain.CDCPosition{}, "", errors.New("source returned no final watermark")
	}
	if _, err := watermarkCovers(*p, *p); err != nil {
		return domain.CDCPosition{}, "", err
	}
	identity, ok := raw.(connector.CDCSourceIdentity)
	if !ok {
		return domain.CDCPosition{}, "", errors.New("source cannot prove its database incarnation")
	}
	incarnation, err := identity.CDCSourceIdentity(ctx)
	if err != nil {
		return domain.CDCPosition{}, "", fmt.Errorf("read source incarnation: %w", err)
	}
	if incarnation == "" {
		return domain.CDCPosition{}, "", errors.New("source returned no database incarnation")
	}
	h := sha256.Sum256([]byte(sourceFingerprint(*ds, m) + "/" + incarnation))
	return *p, hex.EncodeToString(h[:]), nil
}

func (s *Service) freezeSelection(ctx context.Context, id, direction string) ([]domain.MigrationTable, string, error) {
	tables, err := s.repo.ListMigrationTables(ctx, id)
	if err != nil {
		return nil, "", err
	}
	if len(tables) == 0 {
		return nil, "", errors.New("table freeze requires prepared migration tables")
	}
	if direction == "reverse" {
		tables = reverseMigrationTables(tables)
	}
	names := make([][2]string, len(tables))
	for i, table := range tables {
		names[i] = [2]string{table.SourceSchema, table.SourceTable}
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i][0] == names[j][0] {
			return names[i][1] < names[j][1]
		}
		return names[i][0] < names[j][0]
	})
	b, _ := json.Marshal(names)
	digest := sha256.Sum256(b)
	return tables, hex.EncodeToString(digest[:]), nil
}

func (s *Service) holdSelectedTables(ctx context.Context, m *domain.MigrationTask, direction string) (connector.Connector, string, error) {
	id := m.SourceID
	if direction == "reverse" {
		id = m.TargetID
	}
	ds, err := s.repo.GetDataSource(ctx, id)
	if err != nil {
		return nil, "", err
	}
	tables, selection, err := s.freezeSelection(ctx, m.ID, direction)
	if err != nil {
		return nil, "", err
	}
	raw, err := s.connectors.New(*ds)
	if err != nil {
		return nil, "", err
	}
	freezer, ok := raw.(connector.CDCSelectedTableFreezer)
	if !ok {
		raw.Close()
		return nil, "", errors.New("this source cannot enforce a selected-table write freeze")
	}
	proof, err := freezer.HoldFreezeTables(ctx, tables)
	if err != nil || proof == "" {
		raw.Close()
		if err == nil {
			err = errors.New("missing table freeze proof")
		}
		return nil, "", err
	}
	return raw, proof + ":" + selection, nil
}

func (s *Service) releaseTableFreezeWhenDone(holder connector.Connector, id, direction, proof string, expires time.Time) {
	defer holder.Close()
	ctx, cancel := context.WithDeadline(context.Background(), expires)
	defer cancel()
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m, err := s.repo.GetMigration(ctx, id)
			if errors.Is(err, repository.ErrNotFound) {
				return
			}
			if err == nil {
				op := lifecycleOperation(m, direction)
				if op == nil || op.TableLockProof != proof || op.Phase == "COMPLETED" || op.Phase == "NEEDS_REVIEW" || m.Status == domain.StatusFailed {
					return
				}
			}
		}
	}
}

func (s *Service) ConfirmWriteFreeze(ctx context.Context, id, direction, actor string, req domain.WriteFreezeRequest) (*domain.LifecycleOperation, error) {
	direction, err := cdcDirection(direction)
	if err != nil {
		return nil, err
	}
	if !req.WritesFrozen || len(strings.TrimSpace(req.Evidence)) < 10 || len(req.Evidence) > 2000 || len(req.IdempotencyKey) < 8 || len(req.IdempotencyKey) > 128 {
		return nil, errors.New("confirm writes_frozen, provide 10-2000 characters of freeze evidence and an 8-128 character idempotency key")
	}
	var result *domain.LifecycleOperation
	err = s.exclusiveControlOperation(ctx, id, "confirm-freeze", func(opCtx context.Context, _ string) error {
		return s.withCDCExecution(opCtx, id, direction, func(gateCtx context.Context) error {
			m, e := s.unfilteredCDCTask(gateCtx, id)
			if e != nil {
				return e
			}
			want := domain.StatusReadyCutover
			if direction == "reverse" {
				want = domain.StatusRollbackReady
			}
			if m.Status != want {
				return fmt.Errorf("write freeze requires %s, current=%s", want, m.Status)
			}
			if previous := lifecycleOperation(m, direction); previous != nil && previous.ID == req.IdempotencyKey {
				if previous.FreezeEvidence != req.Evidence || previous.Actor != actor || (previous.TableLockProof != "") != req.HoldTableLocks {
					return errors.New("idempotency key belongs to different freeze evidence")
				}
				result = previous.Clone()
				return nil
			}
			now := time.Now().UTC()
			var proof string
			var holder connector.Connector
			if req.HoldTableLocks {
				holder, proof, e = s.holdSelectedTables(gateCtx, m, direction)
				if e != nil {
					return e
				}
				defer func() {
					if holder != nil {
						holder.Close()
					}
				}()
			}
			position, fingerprint, e := s.finalPositionWithProof(gateCtx, m, direction, proof)
			if e != nil {
				return e
			}
			barrier := position
			if strings.EqualFold(position.PositionType, "LSN") || strings.EqualFold(position.PositionType, "PG_LSN") {
				sourceID := m.SourceID
				if direction == "reverse" {
					sourceID = m.TargetID
				}
				ds, e := s.repo.GetDataSource(gateCtx, sourceID)
				if e != nil {
					return e
				}
				raw, e := s.connectors.New(*ds)
				if e != nil {
					return e
				}
				defer raw.Close()
				marker, ok := raw.(connector.CDCFreezeBarrierSource)
				if !ok {
					return errors.New("PostgreSQL final freeze requires a decoded logical transaction marker")
				}
				if proof != "" {
					freezer, ok := raw.(connector.CDCSelectedTableFreezer)
					if !ok {
						return ErrFreezeRequired
					}
					lockID, _, _ := strings.Cut(proof, ":")
					if e := freezer.VerifyTableFreeze(gateCtx, lockID); e != nil {
						return e
					}
				}
				b, e := marker.CaptureFreezeBarrier(gateCtx)
				if e != nil {
					return e
				}
				if b == nil {
					return errors.New("missing logical freeze marker")
				}
				barrier = *b
				var sampledFingerprint string
				position, sampledFingerprint, e = s.finalPositionWithProof(gateCtx, m, direction, proof)
				if e != nil {
					return e
				}
				covers, e := watermarkCovers(position, barrier)
				if e != nil || !covers || sampledFingerprint != fingerprint {
					return fmt.Errorf("%w: source changed while capturing freeze barrier", ErrFreezeRequired)
				}
			}
			o := &domain.LifecycleOperation{ID: req.IdempotencyKey, Direction: direction, Phase: "FROZEN", Actor: actor, FreezeEvidence: req.Evidence, SourceFingerprint: fingerprint, ConfirmedAt: now, ExpiresAt: now.Add(15 * time.Minute), Barrier: barrier, SourceHead: position, TableLockProof: proof}
			if previous := lifecycleOperation(m, direction); previous != nil {
				o.RollbackStart = previous.RollbackStart
				o.Steps = append([]domain.ControlStep(nil), previous.Steps...)
			}
			_, e = s.mutateMigration(gateCtx, id, func(current *domain.MigrationTask) error {
				if current.Status != want {
					return repository.ErrConflict
				}
				putLifecycleOperation(current, direction, o)
				return nil
			})
			if e == nil {
				result = o.Clone()
				if holder != nil {
					go s.releaseTableFreezeWhenDone(holder, id, direction, proof, o.ExpiresAt)
					holder = nil
				}
			}
			return e
		})
	})
	return result, err
}

func (s *Service) verifyFinalWatermark(ctx context.Context, id, direction string) error {
	return s.withCDCExecution(ctx, id, direction, func(gateCtx context.Context) error { return s.verifyFinalWatermarkLocked(gateCtx, id, direction) })
}
func (s *Service) verifyFinalWatermarkLocked(gateCtx context.Context, id, direction string) error {
	m, err := s.repo.GetMigration(gateCtx, id)
	if err != nil {
		return err
	}
	op := lifecycleOperation(m, direction)
	if op == nil || op.FreezeEvidence == "" || !op.ExpiresAt.After(time.Now()) || op.Phase == "NEEDS_REVIEW" {
		return ErrFreezeRequired
	}
	current, fingerprint, err := s.finalPositionWithProof(gateCtx, m, direction, op.TableLockProof)
	if err != nil {
		return err
	}
	head := op.SourceHead
	if head.PositionValue == "" {
		head = op.Barrier
	}
	equal1, e1 := watermarkCovers(current, head)
	equal2, e2 := watermarkCovers(head, current)
	if e1 != nil || e2 != nil || !equal1 || (!equal2 && op.TableLockProof == "") || fingerprint != op.SourceFingerprint {
		return fmt.Errorf("%w: source changed after write freeze; maintain business freeze and confirm a new watermark", ErrFreezeRequired)
	}
	latest, err := s.latestCDC(gateCtx, id, direction)
	if err != nil {
		return err
	}
	if latest == nil {
		return ErrWatermarkBehind
	}
	required := op.Barrier
	if op.TableLockProof == "" {
		// A selected write can commit between emission of the logical marker
		// and the subsequent physical-head read in manual freeze mode.
		required = head
	}
	covered, err := watermarkCovers(*latest, required)
	if err != nil {
		return err
	}
	if !covered || latest.EventsPending > 0 {
		return ErrWatermarkBehind
	}
	_, err = s.mutateMigration(gateCtx, id, func(task *domain.MigrationTask) error {
		live := lifecycleOperation(task, direction)
		if live == nil || live.ID != op.ID {
			return repository.ErrConflict
		}
		live.Applied = *latest
		live.VerifiedSource = current
		live.VerifiedAt = time.Now().UTC()
		return nil
	})
	return err
}

func (s *Service) lifecycleStep(ctx context.Context, id, direction, name string, work func() error) error {
	_, err := s.mutateMigration(ctx, id, func(m *domain.MigrationTask) error {
		o := lifecycleOperation(m, direction)
		if o == nil {
			return ErrFreezeRequired
		}
		o.Phase = name
		if len(o.Steps) > 0 && o.Steps[len(o.Steps)-1].Status == "RUNNING" {
			o.Steps[len(o.Steps)-1].Status = "INTERRUPTED"
			o.Steps[len(o.Steps)-1].FinishedAt = time.Now().UTC()
		}
		o.Steps = append(o.Steps, domain.ControlStep{Name: name, Status: "RUNNING", StartedAt: time.Now().UTC()})
		return nil
	})
	if err != nil {
		return err
	}
	workErr := work()
	_, saveErr := s.mutateMigration(ctx, id, func(m *domain.MigrationTask) error {
		o := lifecycleOperation(m, direction)
		if o == nil || len(o.Steps) == 0 {
			return ErrFreezeRequired
		}
		step := &o.Steps[len(o.Steps)-1]
		if step.Name != name {
			return repository.ErrConflict
		}
		step.FinishedAt = time.Now().UTC()
		step.Status = "DONE"
		if workErr != nil {
			step.Status = "FAILED"
			step.Error = workErr.Error()
			o.LastError = workErr.Error()
		}
		return nil
	})
	return errors.Join(workErr, saveErr)
}

func (s *Service) recoverLifecycle(ctx context.Context, listed domain.MigrationTask) error {
	direction := "forward"
	if listed.Status == domain.StatusRollbackReady || listed.Status == domain.StatusRollbackPreparing {
		direction = "reverse"
	}
	o := lifecycleOperation(&listed, direction)
	if o == nil {
		return nil
	}
	if listed.Status != domain.StatusRollbackPreparing && o.Phase != "STOP_READER" && o.Phase != "DRAIN_SPOOL" {
		return nil
	}
	acquired, err := s.acquireControlOperation(ctx, listed.ID, "lifecycle-recovery")
	if err != nil || !acquired {
		return err
	}
	s.runControlOperation(ctx, listed.ID, "lifecycle-recovery", func(runCtx context.Context) {
		m, e := s.repo.GetMigration(runCtx, listed.ID)
		if e != nil {
			err = e
			return
		}
		if m.Status != listed.Status {
			return
		}
		if m.Status == domain.StatusRollbackPreparing {
			_, err = s.mutateMigration(runCtx, m.ID, func(current *domain.MigrationTask) error {
				if current.RollbackOperation != nil {
					current.RollbackOperation.Phase = "NEEDS_REVIEW"
					current.RollbackOperation.LastError = "interrupted reverse checkpoint creation; verify retained source resource before retry"
				}
				current.Status = domain.StatusFailed
				current.LastError = "reverse CDC preparation interrupted; inspect checkpoint evidence before manual recovery"
				return nil
			})
			return
		}
		if direction == "reverse" {
			err = s.rollback(runCtx, m.ID)
		} else {
			err = s.cutover(runCtx, m.ID)
		}
		if err != nil {
			s.logTask(runCtx, m.ID, "", "", "", "WARN", "lifecycle recovery requires operator attention: "+err.Error())
		}
	})
	return err
}

func (s *Service) LifecycleOperations(ctx context.Context, id string) ([]domain.LifecycleOperation, error) {
	m, err := s.repo.GetMigration(ctx, id)
	if err != nil {
		return nil, err
	}
	out := []domain.LifecycleOperation{}
	for _, o := range []*domain.LifecycleOperation{m.CutoverOperation, m.RollbackOperation} {
		if o != nil {
			out = append(out, *o.Clone())
		}
	}
	return out, nil
}
