package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/repository"
	"strconv"
	"strings"
	"time"
)

type Service struct{ repo repository.Repository }

func NewService(r repository.Repository) *Service { return &Service{repo: r} }
func id() string                                  { b := make([]byte, 4); _, _ = rand.Read(b); return "wrk_" + hex.EncodeToString(b) }
func (s *Service) Register(ctx context.Context, w *domain.Worker) error {
	if w.ID == "" {
		w.ID = id()
	}
	w.Status = "ONLINE"
	w.LastHeartbeat = time.Now()
	return s.repo.UpsertWorker(ctx, w)
}
func (s *Service) Heartbeat(ctx context.Context, w *domain.Worker) error {
	old, err := s.repo.GetWorker(ctx, w.ID)
	if err == nil {
		if w.Hostname == "" {
			w.Hostname = old.Hostname
		}
		if w.CPU == 0 {
			w.CPU = old.CPU
		}
		if w.MemoryMB == 0 {
			w.MemoryMB = old.MemoryMB
		}
		if len(w.Capabilities) == 0 {
			w.Capabilities = old.Capabilities
		}
		if len(w.Labels) == 0 {
			w.Labels = old.Labels
		}
	}
	w.Status = "ONLINE"
	w.LastHeartbeat = time.Now()
	return s.repo.UpsertWorker(ctx, w)
}

func schedulerLoadScore(w *domain.Worker) float64 {
	if w == nil {
		return 0
	}
	cores := w.CPU
	if cores <= 0 {
		cores = 1
	}
	jobsPct := float64(w.RunningJobs) * 100 / float64(cores)
	capacity := 1000.0
	if raw := strings.TrimSpace(os.Getenv("QMIGRATION_WORKER_NETWORK_CAPACITY_MBPS")); raw != "" {
		if n, e := strconv.ParseFloat(raw, 64); e == nil && n > 0 {
			capacity = n
		}
	}
	capBPS := capacity * 1000 * 1000 / 8
	net := w.NetworkRxBps
	if w.NetworkTxBps > net {
		net = w.NetworkTxBps
	}
	netPct := 0.0
	if capBPS > 0 {
		netPct = float64(net) * 100 / capBPS
	}
	if netPct > 200 {
		netPct = 200
	}
	return w.CPUUsagePct*.45 + w.MemoryUsagePct*.25 + jobsPct*.20 + netPct*.10
}

func (s *Service) List(ctx context.Context) ([]domain.Worker, error) {
	ws, err := s.repo.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	for i := range ws {
		ws[i].SchedulerLoadScore = schedulerLoadScore(&ws[i])
		d := now.Sub(ws[i].LastHeartbeat)
		if d > 30*time.Second {
			ws[i].Status = "OFFLINE"
		} else if d > 15*time.Second {
			ws[i].Status = "SUSPECT"
		}
	}
	return ws, nil
}
