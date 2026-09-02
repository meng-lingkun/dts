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
