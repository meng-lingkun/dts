package gbaseconnector

import (
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestCapabilitiesAreQualificationGated(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE", "")
	d := f.Capabilities(domain.DataSourceGBase)
	if !d.Has(connector.CapabilityProtocolProbe) || d.Has(connector.CapabilityFullRead) {
		t.Fatalf("default GBase descriptor must be probe-only: %+v", d)
	}
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE", "1")
	d = f.Capabilities(domain.DataSourceGBase)
	for _, cap := range []connector.Capability{
		connector.CapabilityMetadata,
		connector.CapabilityFullRead,
		connector.CapabilityFullWrite,
		connector.CapabilityKeysetBoundary,
		connector.CapabilitySchemaCreate,
		connector.CapabilityMigrationPrecheck,
	} {
		if !d.Has(cap) {
			t.Fatalf("GBase experimental descriptor missing %s: %+v", cap, d)
		}
	}
	for _, cap := range []connector.Capability{
		connector.CapabilityCDCPosition,
		connector.CapabilityCDCRead,
		connector.CapabilityCDCApply,
		connector.CapabilityCDCTransactional,
		connector.CapabilityPostLoadSchema,
	} {
		if d.Has(cap) {
			t.Fatalf("GBase source/default path must not advertise capability %s: %+v", cap, d)
		}
	}
	if d.Maturity != connector.MaturityExperimental || !d.QualificationRequired {
		t.Fatalf("unexpected GBase maturity: %+v", d)
	}
}

func TestGBaseIsNoLongerExternalJDBC(t *testing.T) {
	if domain.DataSourceGBase.IsExternalJDBC() {
		t.Fatal("GBase 8a must use the RC18 native connector path")
	}
}

func TestGBaseTargetCDCApplyGateIsNonTransactional(t *testing.T) {
	f := NewFactory()
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE", "1")
	t.Setenv("QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC", "1")
	d := f.Capabilities(domain.DataSourceGBase)
	if !d.Has(connector.CapabilityCDCApply) || !d.Has(connector.CapabilityPointLookup) {
		t.Fatalf("GBase target CDC capabilities missing: %+v", d)
	}
	if d.Has(connector.CapabilityCDCTransactional) || d.Has(connector.CapabilityCDCRead) {
		t.Fatalf("GBase RC27 must not overclaim transactional/source CDC: %+v", d)
	}
}
