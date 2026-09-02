package obbinlog

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"qmigration/backend/internal/domain"
)

type Address struct {
	Host string
	Port int
}

func (a Address) String() string { return net.JoinHostPort(a.Host, strconv.Itoa(a.Port)) }

// Endpoint describes OceanBase Binlog subscription endpoints exposed through
// ODP. Binlog Server itself has a management/service port (commonly 2983), but
// downstream MySQL binlog subscribers are expected to connect through a
// tenant-aware ODP endpoint. QMigration therefore stores the subscription
// endpoint explicitly instead of guessing it from the SQL datasource address.
//
// Supported datasource cdc_url forms:
//
//	obbinlog://odp1:2883
//	obbinlog://odp1:2883?fallback=odp2:2883&fallback=odp3:2883
//	obbinlogs://odp1:2883?server_name=binlog.example.internal&fallback=odp2:2883
//
// Credentials are deliberately not accepted in the URL. The OceanBase tenant
// datasource username/password remain encrypted in QMigration and are reused for
// ODP binlog subscription connections.
type Endpoint struct {
	Host       string
	Port       int
	TLS        bool
	ServerName string
	Fallbacks  []Address
	URL        string
}

func parseAddress(raw string) (Address, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Address{}, errors.New("empty ODP fallback endpoint")
	}
	host, portRaw, err := net.SplitHostPort(raw)
	if err != nil {
		return Address{}, fmt.Errorf("invalid ODP endpoint %q; expected host:port: %w", raw, err)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return Address{}, fmt.Errorf("invalid ODP endpoint %q: empty host", raw)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port < 1 || port > 65535 {
		return Address{}, fmt.Errorf("invalid ODP endpoint port %q", portRaw)
	}
	return Address{Host: host, Port: port}, nil
}

func ParseEndpoint(raw string) (Endpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Endpoint{}, errors.New("OceanBase source CDC requires cdc_url (obbinlog://ODP_HOST:PORT)")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, fmt.Errorf("parse OceanBase Binlog cdc_url: %w", err)
	}
	if u.User != nil {
		return Endpoint{}, errors.New("OceanBase Binlog cdc_url must not embed credentials")
	}
	if u.Fragment != "" {
		return Endpoint{}, errors.New("OceanBase Binlog cdc_url must not contain a fragment")
	}
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	if scheme != "obbinlog" && scheme != "obbinlogs" {
		return Endpoint{}, fmt.Errorf("unsupported OceanBase Binlog cdc_url scheme %q; expected obbinlog or obbinlogs", u.Scheme)
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		return Endpoint{}, errors.New("OceanBase Binlog cdc_url requires an ODP host")
	}
	portRaw := strings.TrimSpace(u.Port())
	if portRaw == "" {
		return Endpoint{}, errors.New("OceanBase Binlog cdc_url requires an explicit ODP port")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port < 1 || port > 65535 {
		return Endpoint{}, fmt.Errorf("invalid OceanBase Binlog ODP port %q", portRaw)
	}
	q := u.Query()
	for key := range q {
		if key != "server_name" && key != "fallback" {
			return Endpoint{}, fmt.Errorf("unsupported OceanBase Binlog cdc_url query parameter %q", key)
		}
	}
	serverName := strings.TrimSpace(q.Get("server_name"))
	if strings.ContainsAny(serverName, "\r\n\x00") {
		return Endpoint{}, errors.New("invalid OceanBase Binlog TLS server_name")
	}
	fallbacks := make([]Address, 0, len(q["fallback"]))
	seen := map[string]bool{net.JoinHostPort(host, strconv.Itoa(port)): true}
	for _, item := range q["fallback"] {
		// Repeated fallback= is preferred, but a comma-separated value is accepted
		// for shell/UI convenience.
		for _, rawAddr := range strings.Split(item, ",") {
			addr, err := parseAddress(rawAddr)
			if err != nil {
				return Endpoint{}, err
			}
			key := addr.String()
			if seen[key] {
				continue
			}
			seen[key] = true
			fallbacks = append(fallbacks, addr)
			if len(fallbacks) > 7 {
				return Endpoint{}, errors.New("OceanBase Binlog cdc_url supports at most 7 fallback ODP endpoints")
			}
		}
	}
	normScheme := "obbinlog"
	if scheme == "obbinlogs" {
		normScheme = "obbinlogs"
	}
	normURL := &url.URL{Scheme: normScheme, Host: net.JoinHostPort(host, strconv.Itoa(port))}
	nq := url.Values{}
	if serverName != "" {
		nq.Set("server_name", serverName)
	}
	for _, addr := range fallbacks {
		nq.Add("fallback", addr.String())
	}
	normURL.RawQuery = nq.Encode()
	return Endpoint{Host: host, Port: port, TLS: scheme == "obbinlogs", ServerName: serverName, Fallbacks: fallbacks, URL: normURL.String()}, nil
}

func (e Endpoint) Addresses() []Address {
	out := make([]Address, 0, 1+len(e.Fallbacks))
	out = append(out, Address{Host: e.Host, Port: e.Port})
	out = append(out, e.Fallbacks...)
	return out
}

func (e Endpoint) FailoverString() string {
	parts := make([]string, 0, len(e.Fallbacks))
	for _, addr := range e.Fallbacks {
		parts = append(parts, addr.String())
	}
	return strings.Join(parts, ",")
}

// DataSourceForSubscription clones the SQL datasource into the primary ODP
// endpoint used for SHOW MASTER STATUS and COM_BINLOG_DUMP(_GTID).
func DataSourceForSubscription(ds domain.DataSource) (domain.DataSource, Endpoint, error) {
	ep, err := ParseEndpoint(ds.CDCURL)
	if err != nil {
		return domain.DataSource{}, Endpoint{}, err
	}
	out := ds
	out.Host = ep.Host
	out.Port = ep.Port
	if ep.TLS {
		out.TLSMode = domain.TLSModeRequired
	} else {
		out.TLSMode = domain.TLSModeDisable
	}
	// Leave the name empty when no shared server_name is explicitly configured;
	// the MySQL TLS stack then validates each failover host against its own name.
	out.TLSServerName = ep.ServerName
	return out, ep, nil
}
