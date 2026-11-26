package auth

import "testing"

func TestRBAC(t *testing.T) {
	ts := ParseTokens("admin:a,dba:d,operator:o,viewer:v")
	if r, ok := ts.Authenticate("o"); !ok || r != RoleOperator {
		t.Fatal(r, ok)
	}
	if Allowed(RoleViewer, "POST", "/api/v1/migrations/x/start") {
		t.Fatal("viewer may not mutate")
	}
	if Allowed(RoleOperator, "POST", "/api/v1/migrations/x/cutover") {
		t.Fatal("operator may not cut over")
	}
	if !Allowed(RoleDBA, "POST", "/api/v1/migrations/x/cutover") {
		t.Fatal("dba should cut over")
	}
	if Allowed(RoleViewer, "GET", "/api/v1/migrations/x/cdc/dlq") || Allowed(RoleOperator, "GET", "/api/v1/migrations/x/cdc/dlq") {
		t.Fatal("CDC DLQ row images must not be readable by viewer/operator")
	}
	if !Allowed(RoleDBA, "GET", "/api/v1/migrations/x/cdc/dlq") || !Allowed(RoleAdmin, "POST", "/api/v1/migrations/x/cdc/dlq/d1/replay") {
		t.Fatal("admin/dba should manage CDC DLQ")
	}
	if Allowed(RoleOperator, "POST", "/api/v1/migrations/x/schema-objects/apply") || Allowed(RoleViewer, "POST", "/api/v1/migrations/x/schema-objects/apply") {
		t.Fatal("schema object DDL apply must be restricted to admin/dba")
	}
	if !Allowed(RoleDBA, "POST", "/api/v1/migrations/x/schema-objects/apply") || !Allowed(RoleViewer, "GET", "/api/v1/migrations/x/schema-objects/plan") {
		t.Fatal("dba should apply schema objects and viewer should be able to read the plan")
	}
	if Allowed(RoleDBA, "POST", "/api/v1/validation-report/key-transition") || Allowed(RoleOperator, "POST", "/api/v1/validation-report/key-revocation") {
		t.Fatal("validation-report signing-key lifecycle must be admin-only")
	}
	if !Allowed(RoleAdmin, "POST", "/api/v1/validation-report/key-transition") {
		t.Fatal("admin should manage validation-report signing-key lifecycle")
	}
}

func TestOperatorCannotBypassPrivilegedMigrationActions(t *testing.T) {
	for _, action := range []string{"ready-cutover", "cutover", "rollback/prepare", "cdc", "cdc/started", "cdc/events", "cdc/debezium", "cdc/canal", "cdc/dlq/d1/replay", "schema-objects/apply", "future-dangerous-action"} {
		path := "/api/v1/migrations/task/" + action
		if Allowed(RoleOperator, "POST", path) {
			t.Errorf("operator can invoke privileged or unknown mutation: %s", path)
		}
	}
	for _, action := range []string{"start", "pause", "resume", "cancel", "validate", "repair", "cdc/spool/drain", "validation-report/archive"} {
		if !Allowed(RoleOperator, "POST", "/api/v1/migrations/task/"+action) {
			t.Errorf("ordinary operator action denied: %s", action)
		}
	}
	if Allowed(Role("invalid"), "GET", "/api/v1/datasources") {
		t.Fatal("unknown role must fail closed")
	}
}
