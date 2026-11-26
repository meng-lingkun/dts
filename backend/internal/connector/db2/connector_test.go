package db2connector

import (
	"testing"

	"dts/backend/internal/connector"
	"dts/backend/internal/domain"
)

func TestDB2RangeIncludesBothEndpointsAndZero(t *testing.T) {
	for _, tc := range []struct {
		start, end int64
		want       string
	}{{0, 0, `"ID" BETWEEN 0 AND 0`}, {-9, 0, `"ID" BETWEEN -9 AND 0`}, {0, 9, `"ID" BETWEEN 0 AND 9`}} {
		req := connector.ReadBatchRequest{PrimaryKey: "ID", StartPK: tc.start, EndPK: tc.end}
		if got := db2RangePredicate(req); got != tc.want {
			t.Fatalf("range %d..%d: %q", tc.start, tc.end, got)
		}
		req.UseKeyset = true
		if db2RangePredicate(req) != "" {
			t.Fatal("numeric bounds leaked into keyset read")
		}
	}
}

func TestDB2CompositeKeysetBound(t *testing.T) {
	keys := []string{"A", "B"}
	columns := []domain.ColumnInfo{{Name: "A", DataType: "INTEGER"}, {Name: "B", DataType: "INTEGER"}}
	bound := []connector.Value{{Raw: []byte("5")}, {Raw: []byte("9")}}
	for op, want := range map[string]string{
		">=": `(("A">5) OR ("A"=5 AND "B">=9))`,
		"<=": `(("A"<5) OR ("A"=5 AND "B"<=9))`,
		">":  `(("A">5) OR ("A"=5 AND "B">9))`,
		"<":  `(("A"<5) OR ("A"=5 AND "B"<9))`,
	} {
		got, err := lexicographicPredicate(keys, columns, bound, op)
		if err != nil || got != want {
			t.Fatalf("bound %s: %s, %v; want %s", op, got, err, want)
		}
	}
}

func TestDB2CapabilitiesAreGatedAndNeverAdvertiseSourceCDC(t *testing.T) {
	f := NewFactory()
	t.Setenv("DTS_EXPERIMENTAL_DB2_NATIVE", "")
	d := f.Capabilities(domain.DataSourceDB2)
	if !d.Has(connector.CapabilityProtocolProbe) || d.Has(connector.CapabilityFullRead) {
		t.Fatalf("DB2 default descriptor must be probe only: %+v", d)
	}
	if d.Has(connector.CapabilityCDCRead) || d.Has(connector.CapabilityCDCPosition) {
		t.Fatalf("DB2 source CDC must not be advertised: %+v", d)
	}

	t.Setenv("DTS_EXPERIMENTAL_DB2_NATIVE", "1")
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
		t.Fatal("DB2 must use DTS native DRDA, not external JDBC")
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
