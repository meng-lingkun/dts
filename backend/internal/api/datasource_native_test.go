package api

import (
	"qmigration/backend/internal/domain"
	"testing"
)

func TestPostgreSQLWireCompatibleTLSDefaults(t *testing.T) {
	for _, typ := range []domain.DataSourceType{domain.DataSourceOpenGauss, domain.DataSourceKingbase} {
		mode, err := normalizeDataSourceTLSMode(typ, "")
		if err != nil {
			t.Fatal(err)
		}
		if mode != domain.TLSModePreferred {
			t.Fatalf("%s default tls=%s", typ, mode)
		}
		if typ.IsExternalJDBC() {
			t.Fatalf("%s should be native PG-wire, not external JDBC", typ)
		}
	}
}
