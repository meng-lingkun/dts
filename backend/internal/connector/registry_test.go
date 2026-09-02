package connector_test

import (
	"qmigration/backend/internal/connector"
	gbaseconnector "qmigration/backend/internal/connector/gbase"
	gbase8sconnector "qmigration/backend/internal/connector/gbase8s"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	oracleconnector "qmigration/backend/internal/connector/oracle"
	postgresconnector "qmigration/backend/internal/connector/postgres"
	sqlserverconnector "qmigration/backend/internal/connector/sqlserver"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestRegistryCapabilitiesDriveUnifiedEngine(t *testing.T) {
	r := connector.NewRegistry()
	r.Register(domain.DataSourceMySQL, mysqlconnector.NewFactory())
	r.Register(domain.DataSourcePostgreSQL, postgresconnector.NewFactory())
	r.Register(domain.DataSourceOracle, oracleconnector.NewFactory())
	r.Register(domain.DataSourceSQLServer, sqlserverconnector.NewFactory())

	if !r.Supports(domain.DataSourceMySQL, connector.CapabilityFullRead) || !r.Supports(domain.DataSourceMySQL, connector.CapabilityCDCRead) {
		t.Fatal("mysql native capabilities missing")
	}
	if !r.Supports(domain.DataSourcePostgreSQL, connector.CapabilityCDCCheckpoint) {
		t.Fatal("postgres checkpoint capability missing")
	}
	if !r.Supports(domain.DataSourceOracle, connector.CapabilityProtocolProbe) || !r.Supports(domain.DataSourceSQLServer, connector.CapabilityProtocolProbe) {
		t.Fatal("Oracle/SQL Server native protocol probes must be advertised")
	}
	if r.Supports(domain.DataSourceOracle, connector.CapabilityFullRead) || r.Supports(domain.DataSourceSQLServer, connector.CapabilityFullRead) {
		t.Fatal("protocol probes must not be advertised as full migration connectors")
	}
	if err := r.Require(domain.DataSourceOracle, connector.CapabilityFullRead); err == nil {
		t.Fatal("expected capability error")
	}
}

func TestPostgreSQLWireCompatibleDerivativesExposeFullNotPgoutput(t *testing.T) {
	r := connector.NewRegistry()
	pf := postgresconnector.NewFactory()
	r.Register(domain.DataSourceOpenGauss, pf)
	r.Register(domain.DataSourceKingbase, pf)
	for _, typ := range []domain.DataSourceType{domain.DataSourceOpenGauss, domain.DataSourceKingbase} {
		if !r.Supports(typ, connector.CapabilityFullRead) || !r.Supports(typ, connector.CapabilityFullWrite) {
			t.Fatalf("%s should expose native full-load capabilities", typ)
		}
		if r.Supports(typ, connector.CapabilityCDCRead) || r.Supports(typ, connector.CapabilityCDCCheckpoint) {
			t.Fatalf("%s must not be advertised as pgoutput-compatible", typ)
		}
	}
}

func TestSQLServerExperimentalCapabilityGate(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	r := connector.NewRegistry()
	r.Register(domain.DataSourceSQLServer, sqlserverconnector.NewFactory())
	if !r.Supports(domain.DataSourceSQLServer, connector.CapabilityFullRead) || !r.Supports(domain.DataSourceSQLServer, connector.CapabilityFullWrite) {
		t.Fatal("experimental SQL Server gate should expose native full-load capabilities")
	}
	if r.Supports(domain.DataSourceSQLServer, connector.CapabilityCDCRead) {
		t.Fatal("SQL Server CDC must remain separately gated")
	}
}

func TestSQLServerExperimentalCDCCapabilityGate(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "1")
	r := connector.NewRegistry()
	r.Register(domain.DataSourceSQLServer, sqlserverconnector.NewFactory())
	if !r.Supports(domain.DataSourceSQLServer, connector.CapabilityCDCRead) || !r.Supports(domain.DataSourceSQLServer, connector.CapabilityCDCPosition) {
		t.Fatal("experimental SQL Server CDC gate should expose native LSN reader capabilities")
	}
}

func TestSQLServerCDCGateRequiresNativeGate(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "")
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_CDC", "1")
	r := connector.NewRegistry()
	r.Register(domain.DataSourceSQLServer, sqlserverconnector.NewFactory())
	if r.Supports(domain.DataSourceSQLServer, connector.CapabilityCDCRead) {
		t.Fatal("SQL Server CDC must not be exposed without the native TDS data-plane gate")
	}
}

func TestConnectorMaturityIsTruthful(t *testing.T) {
	mf := mysqlconnector.NewFactory()
	if d := mf.Capabilities(domain.DataSourceMySQL); d.Maturity != connector.MaturityNative || d.QualificationRequired {
		t.Fatalf("unexpected MySQL maturity: %#v", d)
	}
	if d := mf.Capabilities(domain.DataSourceTiDB); d.Maturity != connector.MaturityExperimental || !d.QualificationRequired || !d.Has(connector.CapabilityCDCRead) {
		t.Fatalf("unexpected TiDB maturity/cdc: %#v", d)
	}
	if d := mf.Capabilities(domain.DataSourceOceanBase); d.Maturity != connector.MaturityExperimental || !d.QualificationRequired || !d.Has(connector.CapabilityCDCRead) {
		t.Fatalf("unexpected OceanBase maturity/cdc: %#v", d)
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE", "1")
	if d := oracleconnector.NewFactory().Capabilities(domain.DataSourceOracle); d.Maturity != connector.MaturityExperimental || !d.QualificationRequired {
		t.Fatalf("unexpected Oracle maturity: %#v", d)
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE", "1")
	if d := sqlserverconnector.NewFactory().Capabilities(domain.DataSourceSQLServer); d.Maturity != connector.MaturityExperimental || !d.QualificationRequired {
		t.Fatalf("unexpected SQL Server maturity: %#v", d)
	}
}

func TestGBase8aExperimentalFullCapabilityGate(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE", "")
	r := connector.NewRegistry()
	r.Register(domain.DataSourceGBase, gbaseconnector.NewFactory())
	if !r.Supports(domain.DataSourceGBase, connector.CapabilityProtocolProbe) || r.Supports(domain.DataSourceGBase, connector.CapabilityFullRead) {
		t.Fatal("GBase 8a must remain probe-only without its qualification gate")
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE", "1")
	if !r.Supports(domain.DataSourceGBase, connector.CapabilityFullRead) || !r.Supports(domain.DataSourceGBase, connector.CapabilityFullWrite) {
		t.Fatal("GBase 8a native gate must expose Full Read/Write")
	}
	if r.Supports(domain.DataSourceGBase, connector.CapabilityCDCRead) || r.Supports(domain.DataSourceGBase, connector.CapabilityCDCApply) {
		t.Fatal("RC18 must not inherit MySQL CDC capabilities for GBase 8a")
	}
}

func TestGBase8sExperimentalTargetCapabilityGate(t *testing.T) {
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE", "")
	r := connector.NewRegistry()
	r.Register(domain.DataSourceGBase8s, gbase8sconnector.NewFactory())
	if !r.Supports(domain.DataSourceGBase8s, connector.CapabilityProtocolProbe) || r.Supports(domain.DataSourceGBase8s, connector.CapabilityFullRead) {
		t.Fatal("GBase 8s must remain probe-only without its qualification gate")
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE", "1")
	if !r.Supports(domain.DataSourceGBase8s, connector.CapabilityFullRead) || !r.Supports(domain.DataSourceGBase8s, connector.CapabilityFullWrite) || !r.Supports(domain.DataSourceGBase8s, connector.CapabilityCDCTransactional) {
		t.Fatal("GBase 8s native gate must expose Full and transactional target apply")
	}
	if r.Supports(domain.DataSourceGBase8s, connector.CapabilityCDCRead) || r.Supports(domain.DataSourceGBase8s, connector.CapabilityCDCPosition) {
		t.Fatal("RC19 must not advertise GBase 8s source CDC")
	}
}
