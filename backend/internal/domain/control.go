package domain

import "time"

type ControlStep struct {
	Name       string    `json:"name"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// LifecycleOperation persists both operator evidence and step intent/results.
// A RUNNING non-idempotent step must be reviewed after loss of its executor.
type LifecycleOperation struct {
	ID                string        `json:"id"`
	Direction         string        `json:"direction"`
	Phase             string        `json:"phase"`
	Actor             string        `json:"actor"`
	FreezeEvidence    string        `json:"freeze_evidence"`
	SourceFingerprint string        `json:"source_fingerprint"`
	ConfirmedAt       time.Time     `json:"confirmed_at"`
	ExpiresAt         time.Time     `json:"expires_at"`
	Barrier           CDCPosition   `json:"barrier"`
	SourceHead        CDCPosition   `json:"source_head"`
	TableLockProof    string        `json:"table_lock_proof,omitempty"`
	Applied           CDCPosition   `json:"applied"`
	VerifiedSource    CDCPosition   `json:"verified_source"`
	VerifiedAt        time.Time     `json:"verified_at,omitempty"`
	RollbackStart     CDCPosition   `json:"rollback_start,omitempty"`
	Steps             []ControlStep `json:"steps"`
	LastError         string        `json:"last_error,omitempty"`
	CleanupStatus     string        `json:"cleanup_status,omitempty"`
	CleanupError      string        `json:"cleanup_error,omitempty"`
}

func (o *LifecycleOperation) Clone() *LifecycleOperation {
	if o == nil {
		return nil
	}
	v := *o
	v.Steps = append([]ControlStep(nil), o.Steps...)
	return &v
}

type WriteFreezeRequest struct {
	WritesFrozen   bool   `json:"writes_frozen"`
	HoldTableLocks bool   `json:"hold_table_locks,omitempty"`
	Evidence       string `json:"evidence"`
	IdempotencyKey string `json:"idempotency_key"`
}
