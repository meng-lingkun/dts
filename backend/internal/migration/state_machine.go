package migration

import (
	"fmt"
	"qmigration/backend/internal/domain"
)

var transitions = map[domain.MigrationStatus]map[domain.MigrationStatus]bool{
	domain.StatusCreated:           {domain.StatusPrechecking: true, domain.StatusCancelled: true},
	domain.StatusPrechecking:       {domain.StatusPrecheckSuccess: true, domain.StatusFailed: true, domain.StatusCancelled: true},
	domain.StatusPrecheckSuccess:   {domain.StatusPreparing: true, domain.StatusCancelled: true},
	domain.StatusPreparing:         {domain.StatusCDCInitializing: true, domain.StatusFullMigrating: true, domain.StatusFailed: true, domain.StatusCancelled: true},
	domain.StatusCDCInitializing:   {domain.StatusFullMigrating: true, domain.StatusCDCCatchingUp: true, domain.StatusPaused: true, domain.StatusFailed: true, domain.StatusCancelled: true},
	domain.StatusFullMigrating:     {domain.StatusPaused: true, domain.StatusFullFinished: true, domain.StatusFailed: true, domain.StatusCancelled: true},
	domain.StatusPaused:            {domain.StatusCDCInitializing: true, domain.StatusFullMigrating: true, domain.StatusCDCCatchingUp: true, domain.StatusValidating: true, domain.StatusRollbackSyncing: true, domain.StatusCancelled: true},
	domain.StatusFullFinished:      {domain.StatusCDCCatchingUp: true, domain.StatusValidating: true, domain.StatusFinished: true},
	domain.StatusCDCCatchingUp:     {domain.StatusPaused: true, domain.StatusValidating: true, domain.StatusReadyCutover: true, domain.StatusFailed: true},
	domain.StatusValidating:        {domain.StatusPaused: true, domain.StatusReadyCutover: true, domain.StatusFinished: true, domain.StatusFailed: true},
	domain.StatusReadyCutover:      {domain.StatusCutoverRunning: true, domain.StatusFailed: true},
	domain.StatusCutoverRunning:    {domain.StatusFinished: true, domain.StatusFailed: true},
	domain.StatusFinished:          {domain.StatusRollbackPreparing: true},
	domain.StatusRollbackPreparing: {domain.StatusRollbackSyncing: true, domain.StatusFailed: true, domain.StatusCancelled: true},
	domain.StatusRollbackSyncing:   {domain.StatusPaused: true, domain.StatusRollbackReady: true, domain.StatusFailed: true, domain.StatusCancelled: true},
	domain.StatusRollbackReady:     {domain.StatusRollbackRunning: true, domain.StatusFailed: true},
	domain.StatusRollbackRunning:   {domain.StatusRolledBack: true, domain.StatusFailed: true},
}

func CanTransition(from, to domain.MigrationStatus) bool { return transitions[from][to] }
func Transition(m *domain.MigrationTask, to domain.MigrationStatus) error {
	if !CanTransition(m.Status, to) {
		return fmt.Errorf("invalid state transition: %s -> %s", m.Status, to)
	}
	m.Status = to
	return nil
}
