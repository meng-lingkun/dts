package connector

import "testing"

func TestValidateCustomWhereAcceptsRestrictedPredicates(t *testing.T) {
	valid := []string{
		"status = 'READY'",
		"tenant_id = 42 AND (status IN ('READY','DONE') OR deleted_at IS NULL)",
		"score BETWEEN -1.5 AND 9.5",
		"name NOT LIKE 'test%'",
		"source.id >= 10 AND enabled IS TRUE",
		"note = 'it''s safe'",
	}
	for _, expr := range valid {
		if _, err := CompileCustomWhere(expr, "source", nil); err != nil {
			t.Errorf("validateCustomWhere(%q): %v", expr, err)
		}
	}
}

func TestValidateCustomWhereRejectsExecutableSQL(t *testing.T) {
	invalid := []string{
		"",
		"1 = 1; DROP TABLE users",
		"id = 1 UNION\nSELECT secret FROM users",
		"id = 1 # mysql comment",
		"id = 1 -- comment",
		"id = 1 /* comment */",
		"EXISTS (SELECT 1 FROM users)",
		"SLEEP(10) = 0",
		"id = (SELECT id FROM users)",
		"name = 'unsafe\\' OR 1=1'",
		"id = 1 OR",
		"id IN ()",
		"id = 3e+",
		"another.id = 2",
	}
	for _, expr := range invalid {
		if _, err := CompileCustomWhere(expr, "source", nil); err == nil {
			t.Errorf("validateCustomWhere(%q) unexpectedly succeeded", expr)
		}
	}
}

func TestCompileCustomWhereRendersSourceColumn(t *testing.T) {
	sql, err := CompileCustomWhere("app.status='it''s fine' AND id>42", "app", func(col string) string { return "[" + col + "]" })
	if err != nil || sql != "([status] = 'it''s fine' AND [id] > 42)" {
		t.Fatalf("compiled %q: %v", sql, err)
	}
}

func FuzzCustomWhereRoundTrip(f *testing.F) {
	for _, seed := range []string{"orders.id BETWEEN -1 AND 3", "name IN ('中文','O''Brien')", "(status IS NULL OR status='OK')", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		sql, err := CompileCustomWhere(input, "orders", nil)
		if err != nil {
			return
		}
		// The size limit applies to caller input, while rendering inserts spaces.
		canonical := sql[1 : len(sql)-1]
		if len(canonical) > maxCustomWhereBytes {
			return
		}
		again, err := CompileCustomWhere(canonical, "orders", nil)
		if err != nil || again != sql {
			t.Fatalf("unstable canonical predicate: %q -> %q (%v)", sql, again, err)
		}
	})
}

func TestDecodeAfterPKRequiresExplicitValue(t *testing.T) {
	for _, invalid := range []string{"null", "{}", `{"after_pk":null}`} {
		if _, ok := DecodeAfterPK(invalid); ok {
			t.Errorf("accepted %s", invalid)
		}
	}
	if key, ok := DecodeAfterPK(`{"after_pk":0}`); !ok || key != 0 {
		t.Fatal("lost valid zero cursor")
	}
}
