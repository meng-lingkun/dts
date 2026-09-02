package db2connector

import (
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestDB2CapabilitiesAreGatedAndNeverAdvertiseSourceCDC(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_DB2_NATIVE", "")
	d := f.Capabilities(domain.DataSourceDB2)
	if !d.Has(connector.CapabilityProtocolProbe) || d.Has(connector.CapabilityFullRead) {
		t.Fatalf("DB2 default descriptor must be probe only: %+v", d)
	}
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) {
		t.Fatalf("DB2 source CDC must not be advertised: %+v", d)
	}

	t.Setenv("QMIGRATION_EXPERIMENTAL_DB2_NATIVE", "1")
	d = f.Capabilities(domain.DataSourceDB2)
	for _, cap := range []connector.Capability{
		connector.CapabilityMetadata, connector.CapabilityFullRead, connector.CapabilityFullWrite,
		connector.CapabilitySchemaCreate, connector.CapabilityCDCApply, connector.CapabilityCDCTransactional,
		connector.CapabilityDDLApply, connector.CapabilityPointLookup, connector.CapabilityMigrationPrecheck,
	} {
		if !d.Has(cap) {
			t.Fatalf("DB2 experimental descriptor missing %s: %+v", cap, d)
		}
	}
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) {
		t.Fatalf("DB2 source CDC must remain blocked in RC5: %+v", d)
	}
	if d.Maturity != connector.MaturityExperimental || !d.QualificationRequired {
		t.Fatalf("unexpected DB2 maturity: %+v", d)
	}
}

func TestDB2NoLongerUsesExternalJDBCClassification(t *testing.T) {
	if domain.DataSourceDB2.IsExternalJDBC() {
		t.Fatal("DB2 must use QMigration native DRDA, not external JDBC")
	}
	if domain.DataSourceDameng.IsExternalJDBC() {
		t.Fatal("Dameng must use the RC13 native connector path")
	}
	if domain.DataSourceGaussDB.IsExternalJDBC() {
		t.Fatal("GaussDB must use the RC14 PostgreSQL-wire connector path")
	}
	if domain.DataSourceGBase.IsExternalJDBC() {
		t.Fatal("GBase must use the RC18 native GBase 8a connector path")
	}
}
