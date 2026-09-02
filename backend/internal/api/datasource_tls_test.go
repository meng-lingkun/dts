package api

import (
	"encoding/json"
	"net/http"
	"qmigration/backend/internal/domain"
	"testing"
)

func TestDataSourceTLSModeNormalizationAndDefaults(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/datasources", map[string]any{
		"name": "mysql-tls", "type": "mysql", "host": "db.example", "port": 3306, "username": "migration", "tls_mode": "required", "tls_server_name": "mysql.internal",
	}, "")
	if rr.Code != http.StatusCreated {
		t.Fatalf("create mysql status=%d body=%s", rr.Code, rr.Body.String())
	}
	var mysqlDS domain.DataSource
	if err := json.Unmarshal(rr.Body.Bytes(), &mysqlDS); err != nil {
		t.Fatal(err)
	}
	if mysqlDS.TLSMode != domain.TLSModeRequired || mysqlDS.TLSServerName != "mysql.internal" {
		t.Fatalf("unexpected MySQL TLS config: %+v", mysqlDS)
	}

	rr = requestJSON(t, h, http.MethodPost, "/api/v1/datasources", map[string]any{
		"name": "pg-default", "type": "postgresql", "host": "pg.example", "port": 5432, "username": "postgres",
	}, "")
	if rr.Code != http.StatusCreated {
		t.Fatalf("create postgres status=%d body=%s", rr.Code, rr.Body.String())
	}
	var pgDS domain.DataSource
	if err := json.Unmarshal(rr.Body.Bytes(), &pgDS); err != nil {
		t.Fatal(err)
	}
	if pgDS.TLSMode != domain.TLSModePreferred {
		t.Fatalf("postgres default TLS=%s want=%s", pgDS.TLSMode, domain.TLSModePreferred)
	}
}

func TestDataSourceRejectsInvalidTLSMode(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/datasources", map[string]any{
		"name": "bad-tls", "type": "mysql", "host": "db.example", "port": 3306, "username": "migration", "tls_mode": "verify_everything",
	}, "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestOracleNativeTCPSModeIsAccepted(t *testing.T) {
	h := newAuthTestServer(t)
	rr := requestJSON(t, h, http.MethodPost, "/api/v1/datasources", map[string]any{
		"name": "oracle", "type": "oracle", "host": "oracle.example", "port": 2484, "username": "migration", "database": "app", "tls_mode": "REQUIRED", "tls_server_name": "oracle.internal",
	}, "")
	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestSQLServerTLSModeDefaultsToPreferred(t *testing.T) {
	mode, err := normalizeDataSourceTLSMode(domain.DataSourceSQLServer, "")
	if err != nil {
		t.Fatal(err)
	}
	if mode != domain.TLSModePreferred {
		t.Fatalf("mode=%s want=%s", mode, domain.TLSModePreferred)
	}
}
