package main

import (
	"context"
	"errors"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

type fakeBase struct{ err error }

func (f *fakeBase) TestConnection(context.Context) error                           { return f.err }
func (f *fakeBase) GetVersion(context.Context) (string, error)                     { return "fake", f.err }
func (f *fakeBase) ListSchemas(context.Context) ([]domain.SchemaInfo, error)       { return nil, f.err }
func (f *fakeBase) ListTables(context.Context, string) ([]domain.TableInfo, error) { return nil, f.err }
func (f *fakeBase) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, f.err
}
func (f *fakeBase) Close() error { return nil }

func TestRunnerCountsPassFailSkip(t *testing.T) {
	report := &qualificationReport{}
	r := &runner{ctx: context.Background(), conn: &fakeBase{}, report: report}
	if !r.run("ok", func() (string, map[string]any, error) { return "yes", nil, nil }) {
		t.Fatal("pass step returned false")
	}
	if r.run("bad", func() (string, map[string]any, error) { return "", nil, errors.New("boom") }) {
		t.Fatal("fail step returned true")
	}
	r.skip("skip", "not requested")
	if report.Passed != 1 || report.Failed != 1 || report.Skipped != 1 || len(report.Checks) != 3 {
		t.Fatalf("report counts=%+v", report)
	}
	if report.Checks[1].Status != statusFail || report.Checks[1].Message != "boom" {
		t.Fatalf("failed check=%+v", report.Checks[1])
	}
}

func TestTargetQualificationSkipsWithoutTargetInterfaces(t *testing.T) {
	report := &qualificationReport{}
	r := &runner{ctx: context.Background(), conn: &fakeBase{}, report: report}
	runTargetWriteQualification(r, r.conn, "APP")
	if report.Skipped != 1 || len(report.Checks) != 1 || report.Checks[0].Status != statusSkip {
		t.Fatalf("report=%+v", report)
	}
}

func TestDescriptorJSONDoesNotContainCredentialFields(t *testing.T) {
	report := qualificationReport{
		Target:     map[string]any{"host": "db.example", "port": 1521, "service": "ORCL", "user": "APP", "tls_mode": domain.TLSModeDisable},
		Descriptor: connector.Descriptor{Type: domain.DataSourceOracle, Protocol: "oracle-tns", Native: true},
	}
	for k := range report.Target {
		if k == "password" || k == "tls_client_key" {
			t.Fatalf("secret field leaked into report target: %s", k)
		}
	}
}
