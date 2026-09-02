package migration

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestTransition(t *testing.T) {
	m := &domain.MigrationTask{Status: domain.StatusCreated}
	if err := Transition(m, domain.StatusPrechecking); err != nil {
		t.Fatal(err)
	}
	if err := Transition(m, domain.StatusFinished); err == nil {
		t.Fatal("expected invalid transition")
	}
}
