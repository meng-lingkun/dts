package oracleconnector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	tnsConnect  = 1
	tnsAccept   = 2
	tnsRefuse   = 4
	tnsRedirect = 5
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }
func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{connector.CapabilityProtocolProbe}
	note := "QMigration native Oracle Net/TNS/TCPS + TTC transport is available; production data-plane capabilities remain gated until explicitly enabled"
	if experimentalOracleNativeEnabled() {
		caps = append(caps,
			connector.CapabilityMetadata,
			connector.CapabilityFullRead,
			connector.CapabilityKeysetBoundary,
			connector.CapabilityPartition,
			connector.CapabilityRuntimeLoad,
			connector.CapabilitySchemaObjects,
			connector.CapabilityPointLookup,
			connector.CapabilityMigrationPrecheck,
		)
		note = "EXPERIMENTAL QMigration native Oracle TTC source data plane enabled by QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE"
	}
	if experimentalOracleTargetEnabled() {
		caps = append(caps,
			connector.CapabilityFullWrite,
			connector.CapabilitySchemaCreate,
			connector.CapabilityPostLoadSchema,
			connector.CapabilityCDCApply,
			connector.CapabilityCDCTransactional,
			connector.CapabilityDDLApply,
		)
		note += "; EXPERIMENTAL Oracle target bind/array-bind/prepared DML + LOB/schema/CDC apply enabled by QMIGRATION_EXPERIMENTAL_ORACLE_TARGET"
	}
	if experimentalOracleLogMinerCDCEnabled() {
		caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead, connector.CapabilityValidationSnapshot)
		note += "; EXPERIMENTAL LogMiner/SCN CDC reader + exact AS OF SCN validation snapshot enabled by QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC"
	}
	return connector.Descriptor{Type: t, Protocol: "oracle-tns", Native: true, Capabilities: caps, Maturity: connector.MaturityExperimental, QualificationRequired: true, Note: note}
}
func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	if ds.Host == "" || ds.Port <= 0 {
		return nil, errors.New("invalid Oracle endpoint")
	}
	return &Connector{ds: ds}, nil
}

type Connector struct {
	ds                domain.DataSource
	version           string
	sessionProperties map[string]string
	mu                sync.Mutex
	accepted          *acceptedSession
	proto             ttcProtocolInfo
	data              ttcDataTypeInfo
	authenticated     bool
	inTransaction     bool
	prepared          map[string]oracleTTCPrepared
	// validationSCN pins source table reads to one Oracle Flashback Query SCN.
	// It is set only on independent validation connectors returned by
	// OpenValidationSnapshot and must never leak into the normal migration
	// source/target connector.
	validationSCN string
}

func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.accepted == nil || c.accepted.Session == nil {
		return nil
	}
	err := c.accepted.Session.Close()
	c.accepted = nil
	c.authenticated = false
	c.inTransaction = false
	c.prepared = nil
	return err
}
func (c *Connector) GetVersion(ctx context.Context) (string, error) {
	if c.version == "" {
		if err := c.TestConnection(ctx); err != nil {
			return "", err
		}
	}
	return c.version, nil
}

type acceptedSession struct {
	Session  *tnsDataSession
	Body     []byte
	Host     string
	Port     int
	Protocol string
}

func (c *Connector) TestConnection(ctx context.Context) error {
	accepted, err := c.openAcceptedSession(ctx)
	if err != nil {
		return err
	}
	defer accepted.Session.Close()
	if len(accepted.Body) >= 2 {
		c.version = fmt.Sprintf("oracle-tns-%d", binary.BigEndian.Uint16(accepted.Body[:2]))
	} else {
		c.version = "oracle-tns"
	}
	if strings.EqualFold(accepted.Protocol, "TCPS") || c.ds.TLSMode == domain.TLSModeRequired {
		c.version += "-tcps"
	}
	queryProbe := experimentalOracleTTCQueryEnabled() || experimentalOracleNativeEnabled()
	authProbe := experimentalOracleTTCAuthEnabled() || queryProbe
	deepProbe := experimentalOracleTTCNegotiationEnabled() || authProbe
	if deepProbe {
		info, err := c.negotiateTTCProtocol(ctx, accepted)
		if err != nil {
			return err
		}
		dataInfo, err := c.negotiateTTCDataTypes(ctx, accepted, info)
		if err != nil {
			return err
		}
		c.version = fmt.Sprintf("oracle-ttc-v%d-charset-%d-ttc%d", info.ServerVersion, info.ServerCharset, dataInfo.TTCVersion)
		if authProbe {
			result, err := c.authenticateTTC(ctx, accepted, info, dataInfo)
			if err != nil {
				return err
			}
			c.sessionProperties = result.SessionProperties
			c.version += "-auth"
			if queryProbe {
				query, err := c.executeTTCSelect(ctx, accepted, info, dataInfo, "SELECT 1 AS QMIGRATION_PROBE FROM DUAL")
				if err != nil {
					return err
				}
				if len(query.Columns) != 1 || len(query.Rows) != 1 || len(query.Rows[0]) != 1 || fmt.Sprint(query.Rows[0][0]) != "1" {
					return fmt.Errorf("Oracle TTC SELECT probe returned unexpected shape/value: columns=%d rows=%d", len(query.Columns), len(query.Rows))
				}
				c.version += "-query"
			}
		}
		if strings.EqualFold(accepted.Protocol, "TCPS") || c.ds.TLSMode == domain.TLSModeRequired {
			c.version += "-tcps"
		}
	}
	return nil
}

func envEnabled(name string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}
func experimentalOracleTTCNegotiationEnabled() bool {
	return envEnabled("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_NEGOTIATION")
}
func experimentalOracleTTCAuthEnabled() bool {
	return envEnabled("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_AUTH")
}
func experimentalOracleTTCQueryEnabled() bool {
	return envEnabled("QMIGRATION_EXPERIMENTAL_ORACLE_TTC_QUERY")
}
func experimentalOracleNativeEnabled() bool {
	return envEnabled("QMIGRATION_EXPERIMENTAL_ORACLE_NATIVE")
}
func experimentalOracleTargetEnabled() bool {
	return experimentalOracleNativeEnabled() && envEnabled("QMIGRATION_EXPERIMENTAL_ORACLE_TARGET")
}
func experimentalOracleLogMinerCDCEnabled() bool {
	return experimentalOracleNativeEnabled() && envEnabled("QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC")
}

// openAcceptedSession follows Oracle listener redirects and returns the same
// live TCP/TCPS connection after TNS ACCEPT. TTC authentication, SQL, Full/CDC
// paths reuse this same accepted transport instead of probe-and-close behavior.
func (c *Connector) openAcceptedSession(ctx context.Context) (*acceptedSession, error) {
	service := strings.TrimSpace(c.ds.Database)
	if service == "" {
		return nil, errors.New("Oracle native TNS session requires database/service name")
	}
	host, port, protocol := c.ds.Host, c.ds.Port, ""
	for redirects := 0; redirects < 4; redirects++ {
		nc, err := c.dialEndpoint(ctx, host, port, protocol)
		if err != nil {
			return nil, err
		}
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		descriptor := fmt.Sprintf("(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=%s)(CID=(PROGRAM=QMigration)(HOST=qmigration)(USER=qmigration))))", sanitizeTNSValue(service))
		if _, err = nc.Write(buildConnectPacket(descriptor)); err != nil {
			_ = nc.Close()
			return nil, fmt.Errorf("TNS CONNECT write: %w", err)
		}
		typ, body, err := readTNSPacket(nc)
		if err != nil {
			_ = nc.Close()
			return nil, fmt.Errorf("TNS response: %w", err)
		}
		switch typ {
		case tnsAccept:
			// Clear the probe deadline; upper TTC operations apply their own context deadlines.
			_ = nc.SetDeadline(time.Time{})
			effectiveProtocol := strings.ToUpper(strings.TrimSpace(protocol))
			if effectiveProtocol == "" {
				if _, ok := nc.(*tls.Conn); ok {
					effectiveProtocol = "TCPS"
				} else {
					effectiveProtocol = "TCP"
				}
			}
			return &acceptedSession{Session: &tnsDataSession{conn: nc}, Body: append([]byte(nil), body...), Host: host, Port: port, Protocol: effectiveProtocol}, nil
		case tnsRedirect:
			_ = nc.Close()
			target, err := parseRedirectTarget(string(body))
			if err != nil {
				return nil, fmt.Errorf("Oracle listener redirect: %w", err)
			}
			host, port, protocol = target.Host, target.Port, target.Protocol
		case tnsRefuse:
			_ = nc.Close()
			return nil, fmt.Errorf("Oracle listener refused service %q: %s", service, printable(body))
		default:
			_ = nc.Close()
			return nil, fmt.Errorf("unexpected Oracle TNS packet type %d", typ)
		}
	}
	return nil, errors.New("Oracle listener redirect limit exceeded")
}

type redirectTarget struct {
	Host     string
	Port     int
	Protocol string
}

func oracleTLSMode(ds domain.DataSource) (domain.TLSMode, error) {
	mode := domain.TLSMode(strings.ToUpper(strings.TrimSpace(string(ds.TLSMode))))
	if mode == "" {
		mode = domain.TLSModeDisable
	}
	switch mode {
	case domain.TLSModeDisable, domain.TLSModePreferred, domain.TLSModeRequired:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid Oracle TLS mode %q", ds.TLSMode)
	}
}

func oracleTLSConfig(ds domain.DataSource, host string) (*tls.Config, error) {
	serverName := strings.TrimSpace(ds.TLSServerName)
	if serverName == "" {
		serverName = host
	}
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(ds.TLSCACert) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ds.TLSCACert)) {
			return nil, errors.New("invalid Oracle TCPS CA PEM")
		}
		cfg.RootCAs = pool
	}
	certPEM := strings.TrimSpace(ds.TLSClientCert)
	keyPEM := strings.TrimSpace(ds.TLSClientKey)
	if certPEM != "" || keyPEM != "" {
		if certPEM == "" || keyPEM == "" {
			return nil, errors.New("Oracle TCPS mTLS requires both client certificate and private key")
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("load Oracle TCPS client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func (c *Connector) dialEndpoint(ctx context.Context, host string, port int, protocol string) (net.Conn, error) {
	mode, err := oracleTLSMode(c.ds)
	if err != nil {
		return nil, err
	}
	protocol = strings.ToUpper(strings.TrimSpace(protocol))
	if protocol != "" && protocol != "TCP" && protocol != "TCPS" {
		return nil, fmt.Errorf("unsupported Oracle redirect protocol %q", protocol)
	}
	if protocol == "TCPS" && mode == domain.TLSModeDisable {
		return nil, errors.New("Oracle listener redirected to TCPS but datasource tls_mode=DISABLE")
	}
	if protocol == "TCP" && mode == domain.TLSModeRequired {
		return nil, errors.New("Oracle TLS REQUIRED but listener redirected to plaintext TCP")
	}
	d := &net.Dialer{Timeout: 5 * time.Second}
	addr := net.JoinHostPort(host, fmt.Sprint(port))
	useTLS := protocol == "TCPS" || mode == domain.TLSModeRequired
	tryPreferredTLS := protocol == "" && mode == domain.TLSModePreferred
	if useTLS || tryPreferredTLS {
		cfg, cfgErr := oracleTLSConfig(c.ds, host)
		if cfgErr != nil {
			return nil, cfgErr
		}
		tlsDialer := tls.Dialer{NetDialer: d, Config: cfg}
		nc, tlsErr := tlsDialer.DialContext(ctx, "tcp", addr)
		if tlsErr == nil {
			return nc, nil
		}
		if useTLS {
			return nil, fmt.Errorf("Oracle TCPS handshake: %w", tlsErr)
		}
	}
	return d.DialContext(ctx, "tcp", addr)
}

func (c *Connector) probeEndpoint(ctx context.Context, host string, port int, service, protocol string) (byte, []byte, error) {
	nc, err := c.dialEndpoint(ctx, host, port, protocol)
	if err != nil {
		return 0, nil, err
	}
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	descriptor := fmt.Sprintf("(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=%s)(CID=(PROGRAM=QMigration)(HOST=qmigration)(USER=qmigration))))", sanitizeTNSValue(service))
	if _, err = nc.Write(buildConnectPacket(descriptor)); err != nil {
		return 0, nil, fmt.Errorf("TNS CONNECT write: %w", err)
	}
	typ, body, err := readTNSPacket(nc)
	if err != nil {
		return 0, nil, fmt.Errorf("TNS response: %w", err)
	}
	return typ, body, nil
}

func parseRedirectTarget(v string) (redirectTarget, error) {
	upper := strings.ToUpper(v)
	find := func(key string) string {
		i := strings.Index(upper, "("+key+"=")
		if i < 0 {
			return ""
		}
		start := i + len(key) + 2
		end := strings.Index(v[start:], ")")
		if end < 0 {
			return ""
		}
		return strings.TrimSpace(v[start : start+end])
	}
	host := find("HOST")
	portRaw := find("PORT")
	protocol := strings.ToUpper(find("PROTOCOL"))
	if host == "" || portRaw == "" {
		return redirectTarget{}, fmt.Errorf("redirect did not contain HOST/PORT: %s", printable([]byte(v)))
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return redirectTarget{}, fmt.Errorf("invalid redirect port %q", portRaw)
	}
	if protocol == "" {
		protocol = "TCP"
	}
	return redirectTarget{Host: host, Port: port, Protocol: protocol}, nil
}

func parseRedirectAddress(v string) (string, int, error) {
	t, err := parseRedirectTarget(v)
	return t.Host, t.Port, err
}

func sanitizeTNSValue(v string) string {
	v = strings.TrimSpace(v)
	r := strings.NewReplacer("(", "", " )", "", ")", "", "=", "")
	return r.Replace(v)
}
func printable(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 240 {
		s = s[:240]
	}
	return s
}

func buildConnectPacket(connectData string) []byte {
	data := []byte(connectData)
	// Oracle Net CONNECT uses an 8-byte TNS header followed by a 50-byte
	// CONNECT fixed area. Keeping the descriptor offset explicit makes packet
	// construction deterministic and testable without vendor client libraries.
	fixed := make([]byte, 50)
	binary.BigEndian.PutUint16(fixed[0:2], 0x0139) // protocol version
	binary.BigEndian.PutUint16(fixed[2:4], 0x012c) // compatible version
	binary.BigEndian.PutUint16(fixed[6:8], 8192)   // SDU
	binary.BigEndian.PutUint16(fixed[8:10], 32767) // TDU
	binary.BigEndian.PutUint16(fixed[10:12], 0x7f08)
	binary.BigEndian.PutUint16(fixed[14:16], 0x0100)
	binary.BigEndian.PutUint16(fixed[16:18], uint16(len(data)))
	binary.BigEndian.PutUint16(fixed[18:20], 58) // 8-byte header + 50-byte fixed area
	binary.BigEndian.PutUint32(fixed[20:24], 0x00000800)
	packet := make([]byte, 8+len(fixed)+len(data))
	binary.BigEndian.PutUint16(packet[0:2], uint16(len(packet)))
	packet[4] = tnsConnect
	copy(packet[8:], fixed)
	copy(packet[58:], data)
	return packet
}

func readTNSPacket(r io.Reader) (byte, []byte, error) {
	h := make([]byte, 8)
	if _, err := io.ReadFull(r, h); err != nil {
		return 0, nil, err
	}
	ln := int(binary.BigEndian.Uint16(h[0:2]))
	if ln < 8 || ln > 1<<20 {
		return 0, nil, fmt.Errorf("invalid TNS packet length %d", ln)
	}
	body := make([]byte, ln-8)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return h[4], body, nil
}
