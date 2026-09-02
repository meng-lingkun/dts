package mysqlconnector

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/domain"
)

const (
	clientLongPassword     uint32 = 1 << 0
	clientLongFlag         uint32 = 1 << 2
	clientConnectWithDB    uint32 = 1 << 3
	clientProtocol41       uint32 = 1 << 9
	clientSSL              uint32 = 1 << 11
	clientTransactions     uint32 = 1 << 13
	clientSecureConnection uint32 = 1 << 15
	clientMultiResults     uint32 = 1 << 17
	clientPluginAuth       uint32 = 1 << 19
)

const maxPacketPayload = 0xFFFFFF

type protocolClient struct {
	conn          net.Conn
	r             *bufio.Reader
	seq           uint8
	serverVersion string
	scramble      []byte
	authPlugin    string
	secure        bool
}

type queryResult struct {
	columns []string
	rows    [][][]byte
	nulls   [][]bool
}

type handshake struct {
	serverVersion string
	capabilities  uint32
	scramble      []byte
	plugin        string
}

func dialProtocol(ctx context.Context, d domain.DataSource) (*protocolClient, error) {
	dialer := net.Dialer{Timeout: 8 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(d.Host, strconv.Itoa(d.Port)))
	if err != nil {
		return nil, err
	}
	p := &protocolClient{conn: conn, r: bufio.NewReaderSize(conn, 64*1024)}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	}
	payload, err := p.readPacket()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read mysql handshake: %w", err)
	}
	hs, err := parseHandshake(payload)
	if err != nil {
		conn.Close()
		return nil, err
	}
	p.serverVersion, p.scramble, p.authPlugin = hs.serverVersion, hs.scramble, hs.plugin
	p.seq = 1

	mode := strings.ToUpper(strings.TrimSpace(string(d.TLSMode)))
	if mode == "" {
		mode = string(domain.TLSModeDisable)
	}
	if mode != string(domain.TLSModeDisable) && mode != string(domain.TLSModePreferred) && mode != string(domain.TLSModeRequired) {
		conn.Close()
		return nil, fmt.Errorf("invalid MySQL TLS mode %q", d.TLSMode)
	}
	wantTLS := mode == string(domain.TLSModeRequired) || (mode == string(domain.TLSModePreferred) && hs.capabilities&clientSSL != 0)
	if mode == string(domain.TLSModeRequired) && hs.capabilities&clientSSL == 0 {
		conn.Close()
		return nil, errors.New("MySQL TLS REQUIRED but server does not advertise CLIENT_SSL")
	}
	if wantTLS {
		if err := p.sendSSLRequest(hs, d.Database); err != nil {
			conn.Close()
			return nil, err
		}
		tlsCfg, err := mysqlTLSConfig(d)
		if err != nil {
			conn.Close()
			return nil, err
		}
		tlsConn := tls.Client(conn, tlsCfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("mysql TLS handshake: %w", err)
		}
		p.conn = tlsConn
		p.r = bufio.NewReaderSize(tlsConn, 64*1024)
		p.secure = true
	}
	if err := p.sendHandshakeResponse(hs, d.Username, d.Password, d.Database); err != nil {
		p.conn.Close()
		return nil, err
	}
	if err := p.finishAuth(d.Password); err != nil {
		p.conn.Close()
		return nil, err
	}
	_ = p.conn.SetDeadline(time.Time{})
	if _, err := p.exec(context.Background(), "SET NAMES utf8mb4"); err != nil {
		p.conn.Close()
		return nil, fmt.Errorf("set names utf8mb4: %w", err)
	}
	return p, nil
}

func mysqlTLSConfig(d domain.DataSource) (*tls.Config, error) {
	serverName := strings.TrimSpace(d.TLSServerName)
	if serverName == "" {
		serverName = d.Host
	}
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(d.TLSCACert) != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(d.TLSCACert)) {
			return nil, errors.New("invalid MySQL TLS CA certificate PEM")
		}
		cfg.RootCAs = pool
	}
	certPEM, keyPEM := strings.TrimSpace(d.TLSClientCert), strings.TrimSpace(d.TLSClientKey)
	if certPEM != "" || keyPEM != "" {
		if certPEM == "" || keyPEM == "" {
			return nil, errors.New("MySQL TLS client certificate and key must be configured together")
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("invalid MySQL TLS client certificate/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func negotiatedCapabilities(hs *handshake, database string, secure bool) uint32 {
	caps := clientLongPassword | clientLongFlag | clientProtocol41 | clientTransactions | clientSecureConnection | clientMultiResults
	if hs.capabilities&clientPluginAuth != 0 {
		caps |= clientPluginAuth
	}
	if database != "" && hs.capabilities&clientConnectWithDB != 0 {
		caps |= clientConnectWithDB
	}
	if secure {
		caps |= clientSSL
	}
	return caps & hs.capabilities
}

func (p *protocolClient) sendSSLRequest(hs *handshake, database string) error {
	caps := negotiatedCapabilities(hs, database, true)
	if caps&clientSSL == 0 {
		return errors.New("server does not support CLIENT_SSL")
	}
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, caps)
	_ = binary.Write(&b, binary.LittleEndian, uint32(64*1024*1024))
	b.WriteByte(45)
	b.Write(make([]byte, 23))
	return p.writePacket(b.Bytes())
}

func (p *protocolClient) close() error {
	if p == nil || p.conn == nil {
		return nil
	}
	return p.conn.Close()
}

func parseHandshake(payload []byte) (*handshake, error) {
	if len(payload) < 1 {
		return nil, errors.New("empty mysql handshake")
	}
	if payload[0] == 0xff {
		return nil, parseErrorPacket(payload)
	}
	if payload[0] != 0x0a {
		return nil, fmt.Errorf("unsupported mysql protocol version %d", payload[0])
	}
	pos := 1
	end := bytes.IndexByte(payload[pos:], 0)
	if end < 0 {
		return nil, errors.New("malformed mysql handshake server version")
	}
	serverVersion := string(payload[pos : pos+end])
	pos += end + 1
	if len(payload) < pos+4+8+1+2 {
		return nil, errors.New("short mysql handshake")
	}
	pos += 4
	scramble := append([]byte(nil), payload[pos:pos+8]...)
	pos += 8
	pos++
	capLow := uint32(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	if pos >= len(payload) {
		return &handshake{serverVersion: serverVersion, capabilities: capLow, scramble: scramble, plugin: "mysql_native_password"}, nil
	}
	if len(payload) < pos+1+2+2+1+10 {
		return nil, errors.New("short mysql 4.1 handshake")
	}
	pos++    // charset
	pos += 2 // status
	capHigh := uint32(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	caps := capLow | capHigh<<16
	authLen := int(payload[pos])
	pos++
	pos += 10
	if caps&clientSecureConnection != 0 && pos < len(payload) {
		part2Len := 13
		if authLen > 0 && authLen-8 < part2Len {
			part2Len = authLen - 8
		}
		if part2Len < 0 {
			part2Len = 0
		}
		if pos+part2Len > len(payload) {
			part2Len = len(payload) - pos
		}
		if part2Len > 0 {
			part := payload[pos : pos+part2Len]
			part = bytes.TrimRight(part, "\x00")
			scramble = append(scramble, part...)
			pos += part2Len
		}
	}
	plugin := "mysql_native_password"
	if caps&clientPluginAuth != 0 && pos < len(payload) {
		rest := payload[pos:]
		if i := bytes.IndexByte(rest, 0); i >= 0 {
			rest = rest[:i]
		}
		if len(rest) > 0 {
			plugin = string(rest)
		}
	}
	if len(scramble) > 20 {
		scramble = scramble[:20]
	}
	return &handshake{serverVersion: serverVersion, capabilities: caps, scramble: scramble, plugin: plugin}, nil
}

func (p *protocolClient) sendHandshakeResponse(hs *handshake, username, password, database string) error {
	caps := negotiatedCapabilities(hs, database, p.secure)
	plugin := hs.plugin
	if plugin == "" {
		plugin = "mysql_native_password"
	}
	auth, err := authResponse(plugin, password, hs.scramble, p.secure)
	if err != nil {
		return err
	}
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, caps)
	_ = binary.Write(&b, binary.LittleEndian, uint32(64*1024*1024))
	b.WriteByte(45) // utf8mb4_general_ci
	b.Write(make([]byte, 23))
	b.WriteString(username)
	b.WriteByte(0)
	if caps&clientSecureConnection != 0 {
		b.WriteByte(byte(len(auth)))
		b.Write(auth)
	} else {
		b.Write(auth)
		b.WriteByte(0)
	}
	if caps&clientConnectWithDB != 0 {
		b.WriteString(database)
		b.WriteByte(0)
	}
	if caps&clientPluginAuth != 0 {
		b.WriteString(plugin)
		b.WriteByte(0)
	}
	return p.writePacket(b.Bytes())
}

func authResponse(plugin, password string, scramble []byte, secure bool) ([]byte, error) {
	if password == "" {
		return nil, nil
	}
	switch plugin {
	case "mysql_native_password", "":
		return scrambleNativePassword(password, scramble), nil
	case "caching_sha2_password":
		return scrambleCachingSHA2(password, scramble), nil
	case "sha256_password":
		if secure {
			return append([]byte(password), 0), nil
		}
		// Ask the server for the RSA key during the auth-more-data phase.
		return []byte{1}, nil
	default:
		return nil, fmt.Errorf("unsupported mysql auth plugin %q", plugin)
	}
}

func scrambleNativePassword(password string, scramble []byte) []byte {
	h := sha1.New()
	h.Write([]byte(password))
	s1 := h.Sum(nil)
	h.Reset()
	h.Write(s1)
	s2 := h.Sum(nil)
	h.Reset()
	h.Write(scramble)
	h.Write(s2)
	s3 := h.Sum(nil)
	out := make([]byte, len(s1))
	for i := range s1 {
		out[i] = s1[i] ^ s3[i]
	}
	return out
}
func scrambleCachingSHA2(password string, scramble []byte) []byte {
	h1 := sha256.Sum256([]byte(password))
	h2 := sha256.Sum256(h1[:])
	h := sha256.New()
	h.Write(h2[:])
	h.Write(scramble)
	h3 := h.Sum(nil)
	out := make([]byte, len(h1))
	for i := range h1 {
		out[i] = h1[i] ^ h3[i]
	}
	return out
}

func (p *protocolClient) finishAuth(password string) error {
	plugin := p.authPlugin
	for i := 0; i < 8; i++ {
		pkt, err := p.readPacket()
		if err != nil {
			return fmt.Errorf("read mysql auth response: %w", err)
		}
		if len(pkt) == 0 {
			return errors.New("empty mysql auth response")
		}
		switch pkt[0] {
		case 0x00:
			return nil
		case 0xff:
			return parseErrorPacket(pkt)
		case 0xfe:
			if len(pkt) == 1 {
				return nil
			}
			pos := 1
			z := bytes.IndexByte(pkt[pos:], 0)
			if z < 0 {
				return errors.New("malformed auth switch packet")
			}
			plugin = string(pkt[pos : pos+z])
			pos += z + 1
			scramble := bytes.TrimRight(pkt[pos:], "\x00")
			p.authPlugin = plugin
			p.scramble = append([]byte(nil), scramble...)
			resp, err := authResponse(plugin, password, scramble, p.secure)
			if err != nil {
				return err
			}
			if err = p.writePacket(resp); err != nil {
				return err
			}
		case 0x01:
			if plugin == "caching_sha2_password" {
				if len(pkt) < 2 {
					return errors.New("malformed caching_sha2 auth packet")
				}
				switch pkt[1] {
				case 0x03: // fast auth succeeded; final OK follows.
					continue
				case 0x04:
					if password == "" {
						if err := p.writePacket([]byte{0}); err != nil {
							return err
						}
						continue
					}
					if p.secure {
						if err := p.writePacket(append([]byte(password), 0)); err != nil {
							return err
						}
						continue
					}
					if err := p.writePacket([]byte{0x02}); err != nil {
						return err
					}
					keyPkt, err := p.readPacket()
					if err != nil {
						return err
					}
					if len(keyPkt) < 2 || keyPkt[0] != 0x01 {
						return errors.New("mysql did not return caching_sha2 RSA public key")
					}
					enc, err := encryptPasswordRSA(password, p.scramble, keyPkt[1:])
					if err != nil {
						return err
					}
					if err = p.writePacket(enc); err != nil {
						return err
					}
				default:
					return fmt.Errorf("unsupported caching_sha2 auth state 0x%x", pkt[1])
				}
			} else if plugin == "sha256_password" {
				if p.secure {
					if err := p.writePacket(append([]byte(password), 0)); err != nil {
						return err
					}
					continue
				}
				// sha256_password returns the public key in auth-more-data.
				enc, err := encryptPasswordRSA(password, p.scramble, pkt[1:])
				if err != nil {
					return err
				}
				if err = p.writePacket(enc); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("unexpected auth-more-data for plugin %s", plugin)
			}
		default:
			return fmt.Errorf("unexpected mysql auth packet header 0x%x", pkt[0])
		}
	}
	return errors.New("mysql authentication exceeded protocol steps")
}

func encryptPasswordRSA(password string, scramble, pemBytes []byte) ([]byte, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("invalid mysql RSA public key")
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		if pk, err2 := x509.ParsePKCS1PublicKey(block.Bytes); err2 == nil {
			return encryptWithRSA(password, scramble, pk)
		}
		return nil, err
	}
	pub, ok := pubAny.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("mysql public key is not RSA")
	}
	return encryptWithRSA(password, scramble, pub)
}
func encryptWithRSA(password string, scramble []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := append([]byte(password), 0)
	for i := range plain {
		plain[i] ^= scramble[i%len(scramble)]
	}
	return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, plain, nil)
}

func (p *protocolClient) query(ctx context.Context, sql string) (*queryResult, error) {
	if err := p.setDeadline(ctx); err != nil {
		return nil, err
	}
	defer p.clearDeadline()
	p.seq = 0
	if err := p.writePacket(append([]byte{0x03}, []byte(sql)...)); err != nil {
		return nil, err
	}
	first, err := p.readPacket()
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, errors.New("empty mysql query response")
	}
	if first[0] == 0xff {
		return nil, parseErrorPacket(first)
	}
	if first[0] == 0x00 {
		return &queryResult{}, nil
	}
	count, _, ok := readLenEncInt(first, 0)
	if !ok {
		return nil, errors.New("invalid mysql column count")
	}
	res := &queryResult{columns: make([]string, 0, int(count))}
	for i := 0; i < int(count); i++ {
		pkt, err := p.readPacket()
		if err != nil {
			return nil, err
		}
		name, err := parseColumnName(pkt)
		if err != nil {
			return nil, err
		}
		res.columns = append(res.columns, name)
	}
	term, err := p.readPacket()
	if err != nil {
		return nil, err
	}
	if !isEOFPacket(term) && !(len(term) > 0 && term[0] == 0x00) {
		return nil, errors.New("missing mysql column terminator")
	}
	for {
		pkt, err := p.readPacket()
		if err != nil {
			return nil, err
		}
		if isEOFPacket(pkt) {
			break
		}
		if len(pkt) > 0 && pkt[0] == 0xff {
			return nil, parseErrorPacket(pkt)
		}
		vals, nulls, err := parseTextRow(pkt, int(count))
		if err != nil {
			return nil, err
		}
		res.rows = append(res.rows, vals)
		res.nulls = append(res.nulls, nulls)
	}
	return res, nil
}

func (p *protocolClient) exec(ctx context.Context, sql string) (uint64, error) {
	if err := p.setDeadline(ctx); err != nil {
		return 0, err
	}
	defer p.clearDeadline()
	p.seq = 0
	if err := p.writePacket(append([]byte{0x03}, []byte(sql)...)); err != nil {
		return 0, err
	}
	pkt, err := p.readPacket()
	if err != nil {
		return 0, err
	}
	if len(pkt) == 0 {
		return 0, errors.New("empty mysql exec response")
	}
	if pkt[0] == 0xff {
		return 0, parseErrorPacket(pkt)
	}
	if pkt[0] != 0x00 {
		return 0, fmt.Errorf("unexpected mysql exec response 0x%x", pkt[0])
	}
	affected, _, ok := readLenEncInt(pkt, 1)
	if !ok {
		return 0, nil
	}
	return affected, nil
}

func (p *protocolClient) setDeadline(ctx context.Context) error {
	if d, ok := ctx.Deadline(); ok {
		return p.conn.SetDeadline(d)
	}
	return p.conn.SetDeadline(time.Now().Add(60 * time.Second))
}
func (p *protocolClient) clearDeadline() { _ = p.conn.SetDeadline(time.Time{}) }

func (p *protocolClient) readPacket() ([]byte, error) {
	var out []byte
	for {
		h := make([]byte, 4)
		if _, err := io.ReadFull(p.r, h); err != nil {
			return nil, err
		}
		length := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
		p.seq = h[3] + 1
		buf := make([]byte, length)
		if _, err := io.ReadFull(p.r, buf); err != nil {
			return nil, err
		}
		out = append(out, buf...)
		if length < maxPacketPayload {
			return out, nil
		}
	}
}
func (p *protocolClient) writePacket(payload []byte) error {
	for {
		n := len(payload)
		if n > maxPacketPayload {
			n = maxPacketPayload
		}
		h := []byte{byte(n), byte(n >> 8), byte(n >> 16), p.seq}
		p.seq++
		if _, err := p.conn.Write(h); err != nil {
			return err
		}
		if n > 0 {
			if _, err := p.conn.Write(payload[:n]); err != nil {
				return err
			}
		}
		payload = payload[n:]
		if n < maxPacketPayload {
			return nil
		}
		if len(payload) == 0 {
			h = []byte{0, 0, 0, p.seq}
			p.seq++
			_, err := p.conn.Write(h)
			return err
		}
	}
}

func parseErrorPacket(pkt []byte) error {
	if len(pkt) < 3 {
		return errors.New("mysql error")
	}
	code := binary.LittleEndian.Uint16(pkt[1:3])
	pos := 3
	state := ""
	if len(pkt) >= 9 && pkt[3] == '#' {
		state = string(pkt[4:9])
		pos = 9
	}
	msg := "mysql error"
	if pos < len(pkt) {
		msg = string(pkt[pos:])
	}
	if state != "" {
		return fmt.Errorf("mysql error %d (%s): %s", code, state, msg)
	}
	return fmt.Errorf("mysql error %d: %s", code, msg)
}
func isEOFPacket(pkt []byte) bool { return len(pkt) > 0 && pkt[0] == 0xfe && len(pkt) < 9 }
func parseColumnName(pkt []byte) (string, error) {
	pos := 0
	for i := 0; i < 6; i++ {
		v, n, ok := readLenEncBytes(pkt, pos)
		if !ok {
			return "", errors.New("malformed mysql column definition")
		}
		if i == 4 {
			return string(v), nil
		}
		pos = n
	}
	return "", errors.New("missing mysql column name")
}
func parseTextRow(pkt []byte, count int) ([][]byte, []bool, error) {
	vals := make([][]byte, count)
	nulls := make([]bool, count)
	pos := 0
	for i := 0; i < count; i++ {
		if pos >= len(pkt) {
			return nil, nil, errors.New("short mysql text row")
		}
		if pkt[pos] == 0xfb {
			nulls[i] = true
			pos++
			continue
		}
		v, n, ok := readLenEncBytes(pkt, pos)
		if !ok {
			return nil, nil, errors.New("invalid mysql row value")
		}
		vals[i] = append([]byte(nil), v...)
		pos = n
	}
	return vals, nulls, nil
}
func readLenEncBytes(b []byte, pos int) ([]byte, int, bool) {
	n, next, ok := readLenEncInt(b, pos)
	if !ok {
		return nil, pos, false
	}
	if n == ^uint64(0) {
		return nil, next, true
	}
	if n > uint64(len(b)-next) {
		return nil, pos, false
	}
	return b[next : next+int(n)], next + int(n), true
}
func readLenEncInt(b []byte, pos int) (uint64, int, bool) {
	if pos >= len(b) {
		return 0, pos, false
	}
	switch b[pos] {
	case 0xfb:
		return ^uint64(0), pos + 1, true
	case 0xfc:
		if pos+3 > len(b) {
			return 0, pos, false
		}
		return uint64(binary.LittleEndian.Uint16(b[pos+1 : pos+3])), pos + 3, true
	case 0xfd:
		if pos+4 > len(b) {
			return 0, pos, false
		}
		return uint64(b[pos+1]) | uint64(b[pos+2])<<8 | uint64(b[pos+3])<<16, pos + 4, true
	case 0xfe:
		if pos+9 > len(b) {
			return 0, pos, false
		}
		return binary.LittleEndian.Uint64(b[pos+1 : pos+9]), pos + 9, true
	default:
		return uint64(b[pos]), pos + 1, true
	}
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func quoteSQLString(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\x00", "\\0", "\n", "\\n", "\r", "\\r", "\x1a", "\\Z", "'", "\\'")
	return "'" + r.Replace(s) + "'"
}
