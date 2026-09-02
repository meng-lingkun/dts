package mysqlconnector

import (
	"context"
	"testing"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func TestSourceCDCCapabilitiesMatchActualProtocol(t *testing.T) {
	factory := NewFactory()
	direct := []domain.DataSourceType{
		domain.DataSourceMySQL,
		domain.DataSourceMariaDB,
		domain.DataSourcePolarDBX,
		domain.DataSourcePolarDBMySQL,
	}
	for _, typ := range direct {
		d := factory.Capabilities(typ)
		if !d.Has(connector.CapabilityCDCPosition) || !d.Has(connector.CapabilityCDCRead) {
			t.Fatalf("%s must advertise native MySQL binlog source capabilities: %#v", typ, d.Capabilities)
		}
	}

	tidb := factory.Capabilities(domain.DataSourceTiDB)
	if !tidb.Has(connector.CapabilityCDCPosition) || !tidb.Has(connector.CapabilityCDCRead) {
		t.Fatalf("TiDB must advertise dedicated TiCDC source capabilities: %#v", tidb.Capabilities)
	}
	if tidb.Maturity != connector.MaturityExperimental || !tidb.QualificationRequired {
		t.Fatalf("TiDB TiCDC must remain qualification-gated before real-instance validation: %#v", tidb)
	}
	if !tidb.Has(connector.CapabilityValidationSnapshot) {
		t.Fatalf("TiDB must advertise exact TSO validation snapshots: %#v", tidb.Capabilities)
	}

	oceanbase := factory.Capabilities(domain.DataSourceOceanBase)
	if !oceanbase.Has(connector.CapabilityCDCPosition) || !oceanbase.Has(connector.CapabilityCDCRead) {
		t.Fatalf("OceanBase must advertise the dedicated ODP/Binlog Service CDC software path: %#v", oceanbase.Capabilities)
	}
	if oceanbase.Maturity != connector.MaturityExperimental || !oceanbase.QualificationRequired {
		t.Fatalf("OceanBase Binlog Service source must remain qualification-gated before real-instance validation: %#v", oceanbase)
	}
	if !oceanbase.Has(connector.CapabilityCDCApply) || !oceanbase.Has(connector.CapabilityCDCTransactional) {
		t.Fatalf("OceanBase remains a valid MySQL-protocol CDC target: %#v", oceanbase.Capabilities)
	}

}

var _ connector.ValidationSnapshotConnector = (*Connector)(nil)

func TestTiDBValidationSnapshotRejectsNonTSOWithoutDial(t *testing.T) {
	c := &Connector{ds: domain.DataSource{Type: domain.DataSourceTiDB, Host: "127.0.0.1", Port: 4000}}
	_, err := c.OpenValidationSnapshot(context.Background(), domain.CDCPosition{PositionType: "GTID", PositionValue: "uuid:1-2"})
	if err == nil {
		t.Fatal("TiDB validation snapshot must reject a non-TIDB_TSO position before dialing")
	}
}
