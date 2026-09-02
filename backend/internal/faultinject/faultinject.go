// Package faultinject provides deterministic, opt-in failpoints for QMigration
// qualification and chaos testing. Production behavior is unchanged unless the
// operator explicitly enables fault injection.
package faultinject

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const (
	EnvEnable = "QMIGRATION_ENABLE_FAULT_INJECTION"
	EnvPlan   = "QMIGRATION_FAULT_PLAN"
)

type trigger struct {
	Occurrence int
	Action     string
}

var state struct {
	sync.Mutex
	signature string
	calls     map[string]int
}

func enabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvEnable))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// parsePlan accepts comma-separated "name=N" entries. RC29 additionally
// accepts "name=N@SIGKILL" for process-level crash qualification. SIGKILL is
// only honored when fault injection is explicitly enabled, so production
// behavior cannot change accidentally.
func parsePlan(raw string) (map[string]trigger, error) {
	out := map[string]trigger{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return out, nil
	}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid fault plan entry %q; expected name=N or name=N@SIGKILL", item)
		}
		name := strings.TrimSpace(parts[0])
		if name == "" || strings.ContainsAny(name, " \t\r\n") {
			return nil, fmt.Errorf("invalid fault name %q", name)
		}
		rhs := strings.TrimSpace(parts[1])
		action := "ERROR"
		if at := strings.LastIndex(rhs, "@"); at >= 0 {
			action = strings.ToUpper(strings.TrimSpace(rhs[at+1:]))
			rhs = strings.TrimSpace(rhs[:at])
		}
		if action != "ERROR" && action != "SIGKILL" && action != "ENOSPC" {
			return nil, fmt.Errorf("invalid fault action %q in %q; supported actions are ERROR, SIGKILL and ENOSPC", action, item)
		}
		n, err := strconv.Atoi(rhs)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid fault trigger %q; N must be a positive integer", item)
		}
		if _, exists := out[name]; exists {
			return nil, fmt.Errorf("duplicate fault name %q", name)
		}
		out[name] = trigger{Occurrence: n, Action: action}
	}
	return out, nil
}

// Check fires a deterministic injected failure at the configured occurrence.
// ERROR preserves the RC27 behavior. SIGKILL terminates the current process
// after the failpoint is reached, allowing the external qualifier to prove
// durable recovery across a real process death rather than a returned error.
// ENOSPC returns syscall.ENOSPC so storage paths can qualify the same error
// identity the kernel reports when a filesystem has no remaining space.
func Check(name string) error {
	if !enabled() {
		return nil
	}
	raw := strings.TrimSpace(os.Getenv(EnvPlan))
	plan, err := parsePlan(raw)
	if err != nil {
		return fmt.Errorf("fault injection configuration: %w", err)
	}
	spec, ok := plan[name]
	if !ok {
		return nil
	}

	state.Lock()
	sig := raw
	if state.signature != sig || state.calls == nil {
		state.signature = sig
		state.calls = map[string]int{}
	}
	state.calls[name]++
	call := state.calls[name]
	fire := call == spec.Occurrence
	state.Unlock()
	if !fire {
		return nil
	}
	if spec.Action == "ENOSPC" {
		return fmt.Errorf("injected QMigration ENOSPC at %s occurrence %d: %w", name, spec.Occurrence, syscall.ENOSPC)
	}
	if spec.Action == "SIGKILL" {
		p, e := os.FindProcess(os.Getpid())
		if e != nil {
			return fmt.Errorf("injected QMigration SIGKILL at %s occurrence %d: find process: %w", name, spec.Occurrence, e)
		}
		if e := p.Kill(); e != nil {
			return fmt.Errorf("injected QMigration SIGKILL at %s occurrence %d: %w", name, spec.Occurrence, e)
		}
		// Kill should not return control to this process. Keep a fail-closed
		// fallback for unusual platforms/runtime behavior.
		return fmt.Errorf("injected QMigration SIGKILL at %s occurrence %d returned unexpectedly", name, spec.Occurrence)
	}
	return fmt.Errorf("injected QMigration fault at %s occurrence %d", name, spec.Occurrence)
}

// Validate is used by readiness/qualification code to reject malformed plans
// before a migration starts.
func Validate() error {
	if !enabled() {
		return nil
	}
	if strings.TrimSpace(os.Getenv(EnvPlan)) == "" {
		return errors.New("fault injection is enabled but QMIGRATION_FAULT_PLAN is empty")
	}
	_, err := parsePlan(os.Getenv(EnvPlan))
	return err
}

// ResetForTest clears process-local counters. It is intentionally exported only
// for deterministic package/service tests; production code never needs it.
func ResetForTest() {
	state.Lock()
	defer state.Unlock()
	state.signature = ""
	state.calls = nil
}
