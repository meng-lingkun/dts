package postgresconnector

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"time"
)

type pgResult struct {
	columns []string
	rows    [][][]byte
	nulls   [][]bool
}
type pgClient struct {
	conn          net.Conn
	r             *bufio.Reader
	user          string
	serverVersion string
}

func dialPG(ctx context.Context, ds domain.DataSource) (*pgClient, error) {
	return dialPGWithParams(ctx, ds, nil)
}

func pgTLSMode(ds domain.DataSource) (domain.TLSMode, error) {
	mode := domain.TLSMode(strings.ToUpper(strings.TrimSpace(string(ds.TLSMode))))
	if mode == "" {
		// Keep the pre-V0.14 PostgreSQL behavior for existing datasource rows.
		mode = domain.TLSModePreferred
	}
	switch mode {
	case domain.TLSModeDisable, domain.TLSModePreferred, domain.TLSModeRequired:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid PostgreSQL TLS mode %q", ds.TLSMode)
	}
}

func pgTLSConfig(ds domain.DataSource) (*tls.Config, error) {
	serverName := strings.TrimSpace(ds.TLSServerName)
	if serverName == "" {
		serverName = ds.Host
	}
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(ds.TLSCACert) != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(ds.TLSCACert)) {
			return nil, errors.New("invalid PostgreSQL TLS CA certificate PEM")
		}
		cfg.RootCAs = pool
	}
	certPEM, keyPEM := strings.TrimSpace(ds.TLSClientCert), strings.TrimSpace(ds.TLSClientKey)
	if certPEM != "" || keyPEM != "" {
		if certPEM == "" || keyPEM == "" {
			return nil, errors.New("PostgreSQL TLS client certificate and key must be configured together")
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("invalid PostgreSQL TLS client certificate/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func dialPGWithParams(ctx context.Context, ds domain.DataSource, extra map[string]string) (*pgClient, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ds.Host, strconv.Itoa(ds.Port)))
	if err != nil {
		return nil, err
	}
	mode, err := pgTLSMode(ds)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if mode != domain.TLSModeDisable {
		// PostgreSQL SSLRequest is sent before StartupMessage.
		req := make([]byte, 8)
		binary.BigEndian.PutUint32(req[0:4], 8)
		binary.BigEndian.PutUint32(req[4:8], 80877103)
		if _, err = conn.Write(req); err != nil {
			conn.Close()
			return nil, err
		}
		one := make([]byte, 1)
		if _, err = io.ReadFull(conn, one); err != nil {
			conn.Close()
			return nil, err
		}
		if one[0] == 'S' {
			tlsCfg, e := pgTLSConfig(ds)
			if e != nil {
				conn.Close()
				return nil, e
			}
			tc := tls.Client(conn, tlsCfg)
			if err = tc.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, fmt.Errorf("postgres TLS handshake: %w", err)
			}
			conn = tc
		} else if one[0] == 'N' {
			if mode == domain.TLSModeRequired {
				conn.Close()
				return nil, errors.New("PostgreSQL TLS REQUIRED but server rejected SSLRequest")
			}
		} else {
			conn.Close()
			return nil, fmt.Errorf("unexpected PostgreSQL SSL response %q", one[0])
		}
	}
	c := &pgClient{conn: conn, r: bufio.NewReader(conn), user: ds.Username}
	database := ds.Database
	if database == "" {
		database = ds.Username
	}
	if err = c.startupWithParams(ctx, ds.Username, ds.Password, database, extra); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *pgClient) close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
func cstr(s string) []byte { return append([]byte(s), 0) }
func (c *pgClient) startup(ctx context.Context, user, password, database string) error {
	return c.startupWithParams(ctx, user, password, database, nil)
}

func (c *pgClient) startupWithParams(ctx context.Context, user, password, database string, extra map[string]string) error {
	var body bytes.Buffer
	_ = binary.Write(&body, binary.BigEndian, uint32(196608))
	body.Write(cstr("user"))
	body.Write(cstr(user))
	body.Write(cstr("database"))
	body.Write(cstr(database))
	body.Write(cstr("client_encoding"))
	body.Write(cstr("UTF8"))
	body.Write(cstr("application_name"))
	body.Write(cstr("qmigration"))
	for key, value := range extra {
		body.Write(cstr(key))
		body.Write(cstr(value))
	}
	body.WriteByte(0)
	pkt := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(pkt[:4], uint32(len(pkt)))
	copy(pkt[4:], body.Bytes())
	if _, err := c.conn.Write(pkt); err != nil {
		return err
	}
	var scram *scramState
	for {
		typ, payload, err := c.readMessage(ctx)
		if err != nil {
			return err
		}
		switch typ {
		case 'R':
			if len(payload) < 4 {
				return errors.New("short auth response")
			}
			code := binary.BigEndian.Uint32(payload[:4])
			data := payload[4:]
			switch code {
			case 0:
			case 3:
				if err = c.sendPassword(password); err != nil {
					return err
				}
			case 5:
				if len(data) < 4 {
					return errors.New("short md5 challenge")
				}
				sum1 := md5.Sum([]byte(password + user))
				inner := hex.EncodeToString(sum1[:])
				sum2 := md5.Sum(append([]byte(inner), data[:4]...))
				if err = c.sendPassword("md5" + hex.EncodeToString(sum2[:])); err != nil {
					return err
				}
			case 10:
				mechs := string(data)
				if !strings.Contains(mechs, "SCRAM-SHA-256") {
					return fmt.Errorf("unsupported SASL mechanisms %q", mechs)
				}
				scram, err = newSCRAM(user, password)
				if err != nil {
					return err
				}
				if err = c.sendSASLInitial(scram.clientFirst); err != nil {
					return err
				}
			case 11:
				if scram == nil {
					return errors.New("unexpected SASL continue")
				}
				final, err := scram.continueWith(string(data))
				if err != nil {
					return err
				}
				if err = c.writeMessage('p', []byte(final)); err != nil {
					return err
				}
			case 12:
				if scram != nil {
					if err := scram.verifyServerFinal(string(data)); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("unsupported PostgreSQL authentication method %d", code)
			}
		case 'S':
			parts := bytes.SplitN(payload, []byte{0}, 3)
			if len(parts) >= 2 && string(parts[0]) == "server_version" {
				c.serverVersion = string(parts[1])
			}
		case 'K':
		case 'Z':
			return nil
		case 'E':
			return parsePGError(payload)
		case 'N':
		default:
		}
	}
}
func (c *pgClient) sendPassword(p string) error { return c.writeMessage('p', cstr(p)) }
func (c *pgClient) sendSASLInitial(first string) error {
	var b bytes.Buffer
	b.Write(cstr("SCRAM-SHA-256"))
	_ = binary.Write(&b, binary.BigEndian, int32(len(first)))
	b.WriteString(first)
	return c.writeMessage('p', b.Bytes())
}
func (c *pgClient) writeMessage(typ byte, payload []byte) error {
	buf := make([]byte, 1+4+len(payload))
	buf[0] = typ
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(payload)))
	copy(buf[5:], payload)
	_, err := c.conn.Write(buf)
	return err
}
func (c *pgClient) readMessage(ctx context.Context) (byte, []byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetReadDeadline(deadline)
	} else {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	}
	typ, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	hdr := make([]byte, 4)
	if _, err = io.ReadFull(c.r, hdr); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr)) - 4
	if n < 0 || n > 128<<20 {
		return 0, nil, fmt.Errorf("invalid PostgreSQL message length %d", n)
	}
	p := make([]byte, n)
	_, err = io.ReadFull(c.r, p)
	return typ, p, err
}
func parsePGError(p []byte) error {
	msg := "PostgreSQL error"
	i := 0
	for i < len(p) && p[i] != 0 {
		code := p[i]
		i++
		j := bytes.IndexByte(p[i:], 0)
		if j < 0 {
			break
		}
		v := string(p[i : i+j])
		if code == 'M' {
			msg = v
		}
		i += j + 1
	}
	return errors.New(msg)
}
func (c *pgClient) query(ctx context.Context, sql string) (*pgResult, error) {
	if err := c.writeMessage('Q', cstr(sql)); err != nil {
		return nil, err
	}
	res := &pgResult{}
	for {
		typ, p, err := c.readMessage(ctx)
		if err != nil {
			return nil, err
		}
		switch typ {
		case 'T':
			cols, err := parseRowDescription(p)
			if err != nil {
				return nil, err
			}
			res.columns = cols
		case 'D':
			row, nulls, err := parseDataRow(p)
			if err != nil {
				return nil, err
			}
			res.rows = append(res.rows, row)
			res.nulls = append(res.nulls, nulls)
		case 'C', 'I', 'N', 'S':
		case 'E':
			return nil, parsePGError(p)
		case 'Z':
			return res, nil
		}
	}
}
func (c *pgClient) exec(ctx context.Context, sql string) error {
	_, err := c.query(ctx, sql)
	return err
}
func parseRowDescription(p []byte) ([]string, error) {
	if len(p) < 2 {
		return nil, errors.New("short row description")
	}
	n := int(binary.BigEndian.Uint16(p[:2]))
	i := 2
	out := make([]string, 0, n)
	for k := 0; k < n; k++ {
		j := bytes.IndexByte(p[i:], 0)
		if j < 0 {
			return nil, errors.New("bad row description")
		}
		out = append(out, string(p[i:i+j]))
		i += j + 1
		if i+18 > len(p) {
			return nil, errors.New("short row field")
		}
		i += 18
	}
	return out, nil
}
func parseDataRow(p []byte) ([][]byte, []bool, error) {
	if len(p) < 2 {
		return nil, nil, errors.New("short data row")
	}
	n := int(binary.BigEndian.Uint16(p[:2]))
	i := 2
	row := make([][]byte, n)
	nulls := make([]bool, n)
	for k := 0; k < n; k++ {
		if i+4 > len(p) {
			return nil, nil, errors.New("short data length")
		}
		l := int(int32(binary.BigEndian.Uint32(p[i : i+4])))
		i += 4
		if l == -1 {
			nulls[k] = true
			continue
		}
		if l < 0 || i+l > len(p) {
			return nil, nil, errors.New("invalid data length")
		}
		row[k] = append([]byte(nil), p[i:i+l]...)
		i += l
	}
	return row, nulls, nil
}

type scramState struct{ user, password, nonce, clientFirstBare, clientFirst, serverFirst, clientFinalNoProof, expectedServerSig string }

func saslEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "=", "=3D"), ",", "=2C")
}
func newSCRAM(user, password string) (*scramState, error) {
	nonceBytes := make([]byte, 18)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, err
	}
	nonce := base64.RawStdEncoding.EncodeToString(nonceBytes)
	bare := "n=" + saslEscape(user) + ",r=" + nonce
	return &scramState{user: user, password: password, nonce: nonce, clientFirstBare: bare, clientFirst: "n,," + bare}, nil
}
func parseAttrs(s string) map[string]string {
	m := map[string]string{}
	for _, p := range strings.Split(s, ",") {
		if len(p) >= 3 && p[1] == '=' {
			m[p[:1]] = p[2:]
		}
	}
	return m
}
func (s *scramState) continueWith(serverFirst string) (string, error) {
	a := parseAttrs(serverFirst)
	nonce := a["r"]
	if !strings.HasPrefix(nonce, s.nonce) {
		return "", errors.New("SCRAM server nonce does not extend client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(a["s"])
	if err != nil {
		return "", err
	}
	iter, err := strconv.Atoi(a["i"])
	if err != nil || iter < 1 {
		return "", errors.New("invalid SCRAM iteration count")
	}
	salted := pbkdf2SHA256([]byte(s.password), salt, iter, 32)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	s.clientFinalNoProof = "c=biws,r=" + nonce
	s.serverFirst = serverFirst
	auth := s.clientFirstBare + "," + serverFirst + "," + s.clientFinalNoProof
	sig := hmacSHA256(stored[:], []byte(auth))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	serverKey := hmacSHA256(salted, []byte("Server Key"))
	serverSig := hmacSHA256(serverKey, []byte(auth))
	s.expectedServerSig = base64.StdEncoding.EncodeToString(serverSig)
	return s.clientFinalNoProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}
func (s *scramState) verifyServerFinal(v string) error {
	a := parseAttrs(v)
	if e := a["e"]; e != "" {
		return fmt.Errorf("SCRAM server error: %s", e)
	}
	if a["v"] != "" && !hmac.Equal([]byte(a["v"]), []byte(s.expectedServerSig)) {
		return errors.New("SCRAM server signature mismatch")
	}
	return nil
}
func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(msg)
	return h.Sum(nil)
}
func pbkdf2SHA256(password, salt []byte, iter, keyLen int) []byte {
	hLen := 32
	blocks := (keyLen + hLen - 1) / hLen
	out := make([]byte, 0, blocks*hLen)
	for block := 1; block <= blocks; block++ {
		b := make([]byte, len(salt)+4)
		copy(b, salt)
		binary.BigEndian.PutUint32(b[len(salt):], uint32(block))
		u := hmacSHA256(password, b)
		t := append([]byte(nil), u...)
		for i := 1; i < iter; i++ {
			u = hmacSHA256(password, u)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}
