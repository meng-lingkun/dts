package main

import "testing"

func TestEnvFallback(t *testing.T) {
	if env("QMIGRATION_TEST_MISSING_XYZ", "x") != "x" {
		t.Fatal("fallback")
	}
}
