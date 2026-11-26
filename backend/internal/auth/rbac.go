package auth

import (
	"crypto/subtle"
	"strings"
)

type Role string

const (
	RoleAdmin    Role = "admin"
	RoleDBA      Role = "dba"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

type TokenSet struct{ tokens map[Role][]string }

// ParseTokens accepts "admin:token1,dba:token2,operator:token3,viewer:token4".
// Semicolons may be used instead of commas. Unknown roles are ignored.
func ParseTokens(spec string) *TokenSet {
	t := &TokenSet{tokens: map[Role][]string{}}
	spec = strings.ReplaceAll(spec, ";", ",")
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 || parts[1] == "" {
			continue
		}
		role := Role(strings.ToLower(strings.TrimSpace(parts[0])))
		switch role {
		case RoleAdmin, RoleDBA, RoleOperator, RoleViewer:
			t.tokens[role] = append(t.tokens[role], strings.TrimSpace(parts[1]))
		}
	}
	return t
}

func (t *TokenSet) Empty() bool {
	if t == nil {
		return true
	}
	for _, v := range t.tokens {
		if len(v) > 0 {
			return false
		}
	}
	return true
}
func (t *TokenSet) Authenticate(token string) (Role, bool) {
	if t == nil || token == "" {
		return "", false
	}
	for _, role := range []Role{RoleAdmin, RoleDBA, RoleOperator, RoleViewer} {
		for _, expected := range t.tokens[role] {
			if len(token) == len(expected) && subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1 {
				return role, true
			}
		}
	}
	return "", false
}

func Allowed(role Role, method, path string) bool {
	switch role {
	case RoleAdmin, RoleDBA, RoleOperator, RoleViewer:
	default:
		return false
	}
	// User administration is intentionally admin-only, including reads.
	if strings.HasPrefix(path, "/api/v1/users") {
		return role == RoleAdmin
	}
	if method == "DELETE" && strings.HasPrefix(path, "/api/v1/auth/sessions/") {
		return true // The handler restricts the operation to the signed-in user.
	}
	if path == "/api/v1/auth/personal-tokens" || method == "DELETE" && strings.HasPrefix(path, "/api/v1/auth/personal-tokens/") {
		return true // A signed user session and ownership are checked in the handler.
	}
	// Validation-report signing-key lifecycle issues organization trust certificates.
	// Keep transition/revocation signing admin-only even though ordinary report reads are public to authenticated roles.
	if path == "/api/v1/validation-report/key-transition" || path == "/api/v1/validation-report/key-revocation" {
		return role == RoleAdmin
	}
	// CDC dead letters may contain business row images. Keep both reads and
	// replay actions restricted to privileged database administrators.
	if strings.Contains(path, "/cdc/dlq") {
		return role == RoleAdmin || role == RoleDBA
	}
	// Applying schema objects executes DDL on the migration target. Keep the
	// mutation restricted to privileged database administrators; plan reads
	// remain available through the normal GET policy.
	if strings.Contains(path, "/schema-objects/apply") {
		return role == RoleAdmin || role == RoleDBA
	}
	if role == RoleAdmin {
		return true
	}
	if method == "GET" {
		return true
	}
	if role == RoleViewer {
		return false
	}
	// DBA owns datasource changes, cutover and rollback operations.
	if role == RoleDBA {
		return true
	}
	// Operator can operate ordinary migration lifecycle and validation but cannot
	// change datasource credentials or perform cutover/rollback.
	if role == RoleOperator {
		if method != "POST" || !strings.HasPrefix(path, "/api/v1/") {
			return false
		}
		parts := strings.Split(strings.TrimPrefix(path, "/api/v1/"), "/")
		if len(parts) == 3 && parts[0] == "alerts" && parts[1] != "" && parts[2] == "ack" {
			return true
		}
		if parts[0] != "migrations" {
			return false
		}
		if len(parts) == 1 {
			return true // Create a migration; starting it remains a separate action.
		}
		if len(parts) < 3 || parts[1] == "" {
			return false
		}
		// Explicitly allow ordinary lifecycle actions. A prefix policy also
		// allowed ready-cutover and arbitrary CDC row/checkpoint injection.
		switch strings.Join(parts[2:], "/") {
		case "start", "pause", "resume", "cancel", "validate", "repair", "cdc/spool/drain", "validation-report/archive":
			return true
		}
		return false
	}
	return false
}
