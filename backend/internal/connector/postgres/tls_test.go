package postgresconnector

import (
	"context"
	"io"
	"net"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestPostgreSQLTLSModeDefaultsToPreferred(t *testing.T) {
	mode, err := pgTLSMode(domain.DataSource{Type: domain.DataSourcePostgreSQL})
	if err != nil {
		t.Fatal(err)
	}
	if mode != domain.TLSModePreferred {
		t.Fatalf("mode=%s want=%s", mode, domain.TLSModePreferred)
	}
}

func TestPostgreSQLTLSRequiredNeverDowngrades(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := ln.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		req := make([]byte, 8)
		if _, e = io.ReadFull(c, req); e != nil {
			return
		}
		_, _ = c.Write([]byte{'N'})
	}()
	addr := ln.Addr().(*net.TCPAddr)
	_, err = dialPG(context.Background(), domain.DataSource{Type: domain.DataSourcePostgreSQL, Host: "127.0.0.1", Port: addr.Port, Username: "postgres", Database: "postgres", TLSMode: domain.TLSModeRequired})
	_ = ln.Close()
	<-done
	if err == nil || !strings.Contains(err.Error(), "TLS REQUIRED") {
		t.Fatalf("expected TLS REQUIRED failure, got %v", err)
	}
}

func TestPostgreSQLTLSConfigRejectsInvalidCA(t *testing.T) {
	_, err := pgTLSConfig(domain.DataSource{Host: "db.example", TLSCACert: "not a PEM"})
	if err == nil {
		t.Fatal("expected invalid CA error")
	}
}
