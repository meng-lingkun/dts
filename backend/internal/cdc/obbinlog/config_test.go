package obbinlog

import (
	"testing"

	"qmigration/backend/internal/domain"
)

func TestParseEndpoint(t *testing.T) {
	ep, err := ParseEndpoint("obbinlog://odp.internal:2883?fallback=odp2.internal:2883&fallback=odp3.internal:2883")
	if err != nil {
		t.Fatal(err)
	}
	if ep.Host != "odp.internal" || ep.Port != 2883 || ep.TLS || len(ep.Fallbacks) != 2 {
		t.Fatalf("unexpected endpoint: %#v", ep)
	}
	if ep.FailoverString() != "odp2.internal:2883,odp3.internal:2883" {
		t.Fatalf("unexpected failover list: %s", ep.FailoverString())
	}
	secure, err := ParseEndpoint("obbinlogs://10.0.0.8:2883?server_name=odp.example.internal&fallback=10.0.0.9:2883")
	if err != nil {
		t.Fatal(err)
	}
	if !secure.TLS || secure.ServerName != "odp.example.internal" || len(secure.Addresses()) != 2 {
		t.Fatalf("unexpected secure endpoint: %#v", secure)
	}
	for _, bad := range []string{
		"", "mysql://x:2883", "obbinlog://x", "obbinlog://u:p@x:2883",
		"obbinlog://x:70000", "obbinlog://x:2883?password=x", "obbinlog://x:2883?fallback=bad",
	} {
		if _, err := ParseEndpoint(bad); err == nil {
			t.Fatalf("expected %q to fail", bad)
		}
	}
}

func TestDataSourceForSubscription(t *testing.T) {
	ds := domain.DataSource{Type: domain.DataSourceOceanBase, Host: "sql", Port: 2881, CDCURL: "obbinlogs://odp:2883?server_name=odp.example", TLSMode: domain.TLSModeDisable}
	out, _, err := DataSourceForSubscription(ds)
	if err != nil {
		t.Fatal(err)
	}
	if out.Host != "odp" || out.Port != 2883 || out.TLSMode != domain.TLSModeRequired || out.TLSServerName != "odp.example" {
		t.Fatalf("unexpected subscription datasource: %#v", out)
	}
	plain := ds
	plain.CDCURL = "obbinlog://odp:2883"
	plain.TLSMode = domain.TLSModeRequired
	out, _, err = DataSourceForSubscription(plain)
	if err != nil {
		t.Fatal(err)
	}
	if out.TLSMode != domain.TLSModeDisable || out.TLSServerName != "" {
		t.Fatalf("plain CDC URL must not inherit SQL endpoint TLS: %#v", out)
	}
}
