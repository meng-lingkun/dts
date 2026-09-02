package faultinject

import (
	"errors"
	"syscall"
	"testing"
)

func TestDisabledNeverFires(t *testing.T) {
	t.Setenv(EnvEnable, "")
	t.Setenv(EnvPlan, "x=1")
	ResetForTest()
	if err := Check("x"); err != nil {
		t.Fatal(err)
	}
}

func TestExactOccurrenceAndPlanReset(t *testing.T) {
	t.Setenv(EnvEnable, "1")
	t.Setenv(EnvPlan, "x=2")
	ResetForTest()
	if err := Check("x"); err != nil {
		t.Fatal(err)
	}
	if err := Check("x"); err == nil {
		t.Fatal("second call must fire")
	}
	if err := Check("x"); err != nil {
		t.Fatalf("one-shot failpoint fired twice: %v", err)
	}
	t.Setenv(EnvPlan, "x=1")
	if err := Check("x"); err == nil {
		t.Fatal("plan change must reset counters")
	}
}

func TestPlanAcceptsSIGKILLWithoutExecutingIt(t *testing.T) {
	plan, err := parsePlan("cdc.apply.after_target_before_checkpoint=2@SIGKILL,x=1@ERROR,spool=3@ENOSPC")
	if err != nil {
		t.Fatal(err)
	}
	if plan["cdc.apply.after_target_before_checkpoint"].Occurrence != 2 || plan["cdc.apply.after_target_before_checkpoint"].Action != "SIGKILL" {
		t.Fatalf("unexpected SIGKILL trigger: %+v", plan)
	}
	if plan["spool"].Occurrence != 3 || plan["spool"].Action != "ENOSPC" {
		t.Fatalf("unexpected ENOSPC trigger: %+v", plan)
	}
	if plan["x"].Action != "ERROR" {
		t.Fatalf("unexpected ERROR trigger: %+v", plan)
	}
}

func TestInvalidPlanFailsClosed(t *testing.T) {
	t.Setenv(EnvEnable, "1")
	t.Setenv(EnvPlan, "broken")
	ResetForTest()
	if err := Validate(); err == nil {
		t.Fatal("invalid plan accepted")
	}
	if err := Check("x"); err == nil {
		t.Fatal("invalid enabled plan must fail closed")
	}
	if _, err := parsePlan("x=1@REBOOT"); err == nil {
		t.Fatal("unknown fault action accepted")
	}
}

func TestCheckReturnsENOSPCIdentity(t *testing.T) {
	t.Setenv(EnvEnable, "1")
	t.Setenv(EnvPlan, "spool.write=1@ENOSPC")
	ResetForTest()
	err := Check("spool.write")
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Check() error=%v, want syscall.ENOSPC identity", err)
	}
}
