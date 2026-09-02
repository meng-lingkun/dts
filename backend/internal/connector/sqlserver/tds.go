package sqlserverconnector

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

const (
	tdsSQLBatch = 0x01
	tdsReply    = 0x04
	tdsLogin7   = 0x10

	tokError        = 0xaa
	tokInfo         = 0xab
	tokLoginAck     = 0xad
	tokFeatureAck   = 0xae
	tokEnvChange    = 0xe3
	tokSessionState = 0xe4
	tokSSPI         = 0xed
	tokColMetadata  = 0x81
	tokRow          = 0xd1
	tokNBCRow       = 0xd2
	tokDone         = 0xfd
	tokDoneProc     = 0xfe
	tokDoneInProc   = 0xff

	typeNVarChar  = 0xe7
	typeNChar     = 0xef
	typeVarBinary = 0xa5
	typeBinary    = 0xad
)

type preloginInfo struct {
	Version    string
	Encryption byte
}

type tdsClient struct {
	conn       net.Conn
	packetSize int
	packetID   byte
	version    string
}

func experimentalFullEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_SQLSERVER_NATIVE")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func utf16Bytes(s string) []byte {
	u := utf16.Encode([]rune(s))
	b := make([]byte, len(u)*2)
	for i, v := range u {
		binary.LittleEndian.PutUint16(b[i*2:], v)
	}
	return b
}
func decodeUTF16(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	u := make([]uint16, len(b)/2)
	for i := range u {
		u[i] = binary.LittleEndian.Uint16(b[i*2:])
	}
	return string(utf16.Decode(u))
}
func obfuscatePassword(s string) []byte {
	b := utf16Bytes(s)
	for i, v := range b {
		b[i] = ((v << 4) | (v >> 4)) ^ 0xa5
	}
	return b
}

func buildLogin7(dsHost, user, password, database string, packetSize int) []byte {
	if packetSize < 512 {
		packetSize = 4096
	}
	const fixedLen = 94
	host := "qmigration"
	app := "QMigration"
	server := dsHost
	clientInt := "QMigration Native TDS"
	fields := [][]byte{utf16Bytes(host), utf16Bytes(user), obfuscatePassword(password), utf16Bytes(app), utf16Bytes(server), nil, utf16Bytes(clientInt), nil, utf16Bytes(database)}
	payload := make([]byte, fixedLen)
	pos := fixedLen
	setField := func(pairOff int, data []byte, chars int) {
		binary.LittleEndian.PutUint16(payload[pairOff:pairOff+2], uint16(pos))
		binary.LittleEndian.PutUint16(payload[pairOff+2:pairOff+4], uint16(chars))
		pos += len(data)
	}
	// LOGIN7 base header.
	binary.LittleEndian.PutUint32(payload[4:8], 0x74000004) // TDS 7.4
	binary.LittleEndian.PutUint32(payload[8:12], uint32(packetSize))
	binary.LittleEndian.PutUint32(payload[12:16], 0x00010000)
	binary.LittleEndian.PutUint32(payload[16:20], uint32(os.Getpid()))
	payload[24] = 0xe0 // use database + standard character/float representation
	payload[25] = 0x03 // init language/database notification; SQL auth (not integrated security)
	payload[26] = 0x00
	payload[27] = 0x00
	binary.LittleEndian.PutUint32(payload[32:36], 0x00000409) // en-US LCID; server still controls collation

	pairOffsets := []int{36, 40, 44, 48, 52, 56, 60, 64, 68}
	for i, data := range fields {
		chars := len(data) / 2
		if i == 2 {
			chars = len(utf16Bytes(password)) / 2
		}
		setField(pairOffsets[i], data, chars)
	}
	// ClientID: deterministic locally-administered value, not a hardware identity.
	copy(payload[72:78], []byte{0x02, 0x51, 0x4d, 0x49, 0x47, 0x01})
	// SSPI/AttachDBFile/ChangePassword remain zero-length at current end offset.
	for _, off := range []int{78, 82, 86} {
		binary.LittleEndian.PutUint16(payload[off:off+2], uint16(pos))
		binary.LittleEndian.PutUint16(payload[off+2:off+4], 0)
	}

	for _, data := range fields {
		payload = append(payload, data...)
	}
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(payload)))
	return payload
}

func (c *tdsClient) writeMessage(ctx context.Context, typ byte, payload []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
	}
	packetSize := c.packetSize
	if packetSize < 512 {
		packetSize = 4096
	}
	maxPayload := packetSize - 8
	if maxPayload <= 0 {
		maxPayload = 4088
	}
	if len(payload) == 0 {
		payload = []byte{}
	}
	for {
		n := len(payload)
		if n > maxPayload {
			n = maxPayload
		}
		status := byte(0)
		if n == len(payload) {
			status = tdsEOM
		}
		h := make([]byte, 8)
		h[0], h[1] = typ, status
		binary.BigEndian.PutUint16(h[2:4], uint16(8+n))
		c.packetID++
		if c.packetID == 0 {
			c.packetID = 1
		}
		h[6] = c.packetID
		if _, err := c.conn.Write(h); err != nil {
			return err
		}
		if n > 0 {
			if _, err := c.conn.Write(payload[:n]); err != nil {
				return err
			}
		}
		payload = payload[n:]
		if status&tdsEOM != 0 {
			return nil
		}
	}
}

func (c *tdsClient) readMessage(ctx context.Context) (byte, []byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetReadDeadline(deadline)
	}
	var typ byte
	out := []byte{}
	for {
		h := make([]byte, 8)
		if _, err := io.ReadFull(c.conn, h); err != nil {
			return 0, nil, err
		}
		if typ == 0 {
			typ = h[0]
		} else if h[0] != typ {
			return 0, nil, fmt.Errorf("TDS message packet type changed %x -> %x", typ, h[0])
		}
		ln := int(binary.BigEndian.Uint16(h[2:4]))
		if ln < 8 || ln > 1<<20 {
			return 0, nil, fmt.Errorf("invalid TDS packet length %d", ln)
		}
		b := make([]byte, ln-8)
		if _, err := io.ReadFull(c.conn, b); err != nil {
			return 0, nil, err
		}
		out = append(out, b...)
		if h[1]&tdsEOM != 0 {
			return typ, out, nil
		}
	}
}

func parsePrelogin(body []byte) (preloginInfo, error) {
	out := preloginInfo{Version: "tds", Encryption: 0xff}
	for off := 0; ; off += 5 {
		if off >= len(body) {
			return out, errors.New("PRELOGIN option terminator missing")
		}
		tok := body[off]
		if tok == 0xff {
			break
		}
		if off+5 > len(body) {
			return out, errors.New("truncated PRELOGIN option")
		}
		pos := int(binary.BigEndian.Uint16(body[off+1 : off+3]))
		ln := int(binary.BigEndian.Uint16(body[off+3 : off+5]))
		if pos+ln > len(body) {
			return out, errors.New("PRELOGIN option outside packet")
		}
		v := body[pos : pos+ln]
		switch tok {
		case 0x00:
			if len(v) >= 4 {
				out.Version = fmt.Sprintf("%d.%d.%d", v[0], v[1], binary.BigEndian.Uint16(v[2:4]))
			}
		case 0x01:
			if len(v) > 0 {
				out.Encryption = v[0]
			}
		}
	}
	return out, nil
}

func sqlServerTLSMode(ds domain.DataSource) (domain.TLSMode, error) {
	mode := domain.TLSMode(strings.ToUpper(strings.TrimSpace(string(ds.TLSMode))))
	if mode == "" {
		mode = domain.TLSModePreferred
	}
	switch mode {
	case domain.TLSModeDisable, domain.TLSModePreferred, domain.TLSModeRequired:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid SQL Server TLS mode %q", ds.TLSMode)
	}
}

func sqlServerTLSConfig(ds domain.DataSource) (*tls.Config, error) {
	serverName := strings.TrimSpace(ds.TLSServerName)
	if serverName == "" {
		serverName = ds.Host
	}
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(ds.TLSCACert) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(ds.TLSCACert)) {
			return nil, errors.New("invalid SQL Server TLS CA PEM")
		}
		cfg.RootCAs = pool
	}
	certPEM := strings.TrimSpace(ds.TLSClientCert)
	keyPEM := strings.TrimSpace(ds.TLSClientKey)
	if certPEM != "" || keyPEM != "" {
		if certPEM == "" || keyPEM == "" {
			return nil, errors.New("SQL Server mTLS requires both client certificate and private key")
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("load SQL Server TLS client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// tdsTLSHandshakeConn transports TLS handshake records inside TDS PRELOGIN
// packets as required by MS-TDS 7.x. After tls.Handshake succeeds, switchToRaw
// makes the same tls.Conn send encrypted application records directly on the
// TCP stream; at that point LOGIN7 and all subsequent TDS packets are protected
// by the negotiated full-connection TLS session.
type tdsTLSHandshakeConn struct {
	raw     net.Conn
	readBuf bytes.Buffer
	rawMode bool
}

func (c *tdsTLSHandshakeConn) switchToRaw() { c.rawMode = true }
func (c *tdsTLSHandshakeConn) Read(p []byte) (int, error) {
	if c.rawMode {
		return c.raw.Read(p)
	}
	if c.readBuf.Len() == 0 {
		h := make([]byte, 8)
		if _, err := io.ReadFull(c.raw, h); err != nil {
			return 0, err
		}
		if h[0] != tdsPrelogin && h[0] != tdsReply {
			return 0, fmt.Errorf("expected TDS TLS negotiation packet, got type 0x%02x", h[0])
		}
		ln := int(binary.BigEndian.Uint16(h[2:4]))
		if ln < 8 || ln > 1<<20 {
			return 0, fmt.Errorf("invalid TDS TLS packet length %d", ln)
		}
		body := make([]byte, ln-8)
		if _, err := io.ReadFull(c.raw, body); err != nil {
			return 0, err
		}
		c.readBuf.Write(body)
	}
	return c.readBuf.Read(p)
}
func (c *tdsTLSHandshakeConn) Write(p []byte) (int, error) {
	if c.rawMode {
		return c.raw.Write(p)
	}
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > 4088 {
			n = 4088
		}
		h := make([]byte, 8)
		h[0] = tdsPrelogin
		h[1] = tdsEOM
		binary.BigEndian.PutUint16(h[2:4], uint16(8+n))
		h[6] = 1
		if _, err := c.raw.Write(h); err != nil {
			return written, err
		}
		if _, err := c.raw.Write(p[:n]); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}
func (c *tdsTLSHandshakeConn) Close() error                       { return c.raw.Close() }
func (c *tdsTLSHandshakeConn) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *tdsTLSHandshakeConn) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *tdsTLSHandshakeConn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *tdsTLSHandshakeConn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *tdsTLSHandshakeConn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }

func upgradeTDSTLS(ctx context.Context, raw net.Conn, ds domain.DataSource) (net.Conn, error) {
	cfg, err := sqlServerTLSConfig(ds)
	if err != nil {
		return nil, err
	}
	framed := &tdsTLSHandshakeConn{raw: raw}
	tlsConn := tls.Client(framed, cfg)
	if deadline, ok := ctx.Deadline(); ok {
		_ = tlsConn.SetDeadline(deadline)
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, fmt.Errorf("SQL Server TDS TLS handshake: %w", err)
	}
	framed.switchToRaw()
	return tlsConn, nil
}

func dialTDS(ctx context.Context, ds domain.DataSource) (*tdsClient, error) {
	d := net.Dialer{}
	nc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ds.Host, strconv.Itoa(ds.Port)))
	if err != nil {
		return nil, err
	}
	cl := &tdsClient{conn: nc, packetSize: 4096}
	fail := func(e error) (*tdsClient, error) { _ = nc.Close(); return nil, e }
	mode, err := sqlServerTLSMode(ds)
	if err != nil {
		return fail(err)
	}
	clientEncrypt := byte(0x00) // ENCRYPT_OFF: supports TLS, does not require it.
	if mode == domain.TLSModeDisable {
		clientEncrypt = 0x02
	} // ENCRYPT_NOT_SUP
	if mode == domain.TLSModeRequired {
		clientEncrypt = 0x01
	} // ENCRYPT_ON
	if err = cl.writeMessage(ctx, tdsPrelogin, buildPreloginPayload(clientEncrypt)); err != nil {
		return fail(fmt.Errorf("PRELOGIN write: %w", err))
	}
	_, body, err := cl.readMessage(ctx)
	if err != nil {
		return fail(fmt.Errorf("PRELOGIN read: %w", err))
	}
	pi, err := parsePrelogin(body)
	if err != nil {
		return fail(err)
	}

	needFullTLS := pi.Encryption == 0x01 || pi.Encryption == 0x03 || mode == domain.TLSModeRequired
	loginOnlyTLS := mode == domain.TLSModePreferred && pi.Encryption == 0x00
	if mode == domain.TLSModeRequired && (pi.Encryption == 0x02 || pi.Encryption == 0x00) {
		return fail(fmt.Errorf("SQL Server TLS REQUIRED but server negotiated encryption mode %d", pi.Encryption))
	}
	if mode == domain.TLSModeDisable && (pi.Encryption == 0x01 || pi.Encryption == 0x03) {
		return fail(fmt.Errorf("SQL Server requires TLS but datasource tls_mode=DISABLE"))
	}
	var secure net.Conn
	if needFullTLS || loginOnlyTLS {
		secure, err = upgradeTDSTLS(ctx, nc, ds)
		if err != nil {
			return fail(err)
		}
		cl.conn = secure
	}

	if err = cl.writeMessage(ctx, tdsLogin7, buildLogin7(ds.Host, ds.Username, ds.Password, ds.Database, cl.packetSize)); err != nil {
		return fail(fmt.Errorf("LOGIN7 write: %w", err))
	}
	if loginOnlyTLS {
		// TDS ENCRYPT_OFF + server ENCRYPT_OFF protects only LOGIN7. The server
		// returns to plaintext immediately afterwards, so stop using tls.Conn
		// after the encrypted login packet has been fully written.
		cl.conn = nc
	}
	_, login, err := cl.readMessage(ctx)
	if err != nil {
		return fail(fmt.Errorf("LOGIN7 read: %w", err))
	}
	if err = parseLoginResponse(login); err != nil {
		return fail(err)
	}
	cl.version = pi.Version
	return cl, nil
}

func parseTdsError(data []byte) string {
	if len(data) < 8 {
		return "SQL Server error"
	}
	p := 6
	ml := int(binary.LittleEndian.Uint16(data[p : p+2]))
	p += 2
	if p+ml*2 > len(data) {
		return "SQL Server error"
	}
	return decodeUTF16(data[p : p+ml*2])
}
func skipFeatureAck(b []byte, p int) (int, error) {
	for {
		if p >= len(b) {
			return p, io.ErrUnexpectedEOF
		}
		id := b[p]
		p++
		if id == 0xff {
			return p, nil
		}
		if p+4 > len(b) {
			return p, io.ErrUnexpectedEOF
		}
		ln := int(binary.LittleEndian.Uint32(b[p : p+4]))
		p += 4
		if p+ln > len(b) {
			return p, io.ErrUnexpectedEOF
		}
		p += ln
	}
}
func parseLoginResponse(b []byte) error {
	loginAck := false
	for p := 0; p < len(b); {
		tok := b[p]
		p++
		switch tok {
		case tokError, tokInfo, tokLoginAck, tokEnvChange, tokSSPI:
			if p+2 > len(b) {
				return io.ErrUnexpectedEOF
			}
			ln := int(binary.LittleEndian.Uint16(b[p : p+2]))
			p += 2
			if p+ln > len(b) {
				return io.ErrUnexpectedEOF
			}
			data := b[p : p+ln]
			p += ln
			if tok == tokError {
				return errors.New(parseTdsError(data))
			}
			if tok == tokLoginAck {
				loginAck = true
			}
		case tokFeatureAck:
			var err error
			p, err = skipFeatureAck(b, p)
			if err != nil {
				return err
			}
		case tokSessionState:
			if p+4 > len(b) {
				return io.ErrUnexpectedEOF
			}
			ln := int(binary.LittleEndian.Uint32(b[p : p+4]))
			p += 4
			if p+ln > len(b) {
				return io.ErrUnexpectedEOF
			}
			p += ln
		case tokDone, tokDoneProc, tokDoneInProc:
			if p+12 > len(b) {
				return io.ErrUnexpectedEOF
			}
			p += 12
		default:
			return fmt.Errorf("unsupported TDS login token 0x%02x", tok)
		}
	}
	if !loginAck {
		return errors.New("SQL Server LOGIN7 response did not contain LOGINACK")
	}
	return nil
}

type tdsColumn struct {
	typ    byte
	maxLen uint16
}

func parseTypeInfo(b []byte, p int, typ byte) (tdsColumn, int, error) {
	col := tdsColumn{typ: typ}
	switch typ {
	case typeNVarChar, typeNChar:
		if p+7 > len(b) {
			return col, p, io.ErrUnexpectedEOF
		}
		col.maxLen = binary.LittleEndian.Uint16(b[p : p+2])
		p += 7
	case typeVarBinary, typeBinary:
		if p+2 > len(b) {
			return col, p, io.ErrUnexpectedEOF
		}
		col.maxLen = binary.LittleEndian.Uint16(b[p : p+2])
		p += 2
	default:
		return col, p, fmt.Errorf("QMigration TDS result decoder supports NVARCHAR/NCHAR/VARBINARY/BINARY only, got type 0x%02x", typ)
	}
	return col, p, nil
}
func readPLP(b []byte, p int) ([]byte, bool, int, error) {
	if p+8 > len(b) {
		return nil, false, p, io.ErrUnexpectedEOF
	}
	total := binary.LittleEndian.Uint64(b[p : p+8])
	p += 8
	if total == 0xffffffffffffffff {
		return nil, true, p, nil
	}
	out := []byte{}
	for {
		if p+4 > len(b) {
			return nil, false, p, io.ErrUnexpectedEOF
		}
		ln := int(binary.LittleEndian.Uint32(b[p : p+4]))
		p += 4
		if ln == 0 {
			break
		}
		if p+ln > len(b) {
			return nil, false, p, io.ErrUnexpectedEOF
		}
		out = append(out, b[p:p+ln]...)
		p += ln
	}
	return out, false, p, nil
}
func readColumnValue(b []byte, p int, c tdsColumn) ([]byte, bool, int, error) {
	if c.maxLen == 0xffff {
		return readPLP(b, p)
	}
	if p+2 > len(b) {
		return nil, false, p, io.ErrUnexpectedEOF
	}
	ln := int(binary.LittleEndian.Uint16(b[p : p+2]))
	p += 2
	if ln == 0xffff {
		return nil, true, p, nil
	}
	if p+ln > len(b) {
		return nil, false, p, io.ErrUnexpectedEOF
	}
	v := append([]byte(nil), b[p:p+ln]...)
	return v, false, p + ln, nil
}

func parseQueryResponse(b []byte) (rows [][][]byte, nulls [][]bool, rowCount int64, err error) {
	var cols []tdsColumn
	for p := 0; p < len(b); {
		tok := b[p]
		p++
		switch tok {
		case tokColMetadata:
			if p+2 > len(b) {
				return nil, nil, 0, io.ErrUnexpectedEOF
			}
			n := int(binary.LittleEndian.Uint16(b[p : p+2]))
			p += 2
			if n == 0xffff {
				cols = nil
				continue
			}
			cols = make([]tdsColumn, 0, n)
			for i := 0; i < n; i++ {
				if p+7 > len(b) {
					return nil, nil, 0, io.ErrUnexpectedEOF
				}
				p += 6
				typ := b[p]
				p++
				col, np, e := parseTypeInfo(b, p, typ)
				if e != nil {
					return nil, nil, 0, e
				}
				p = np
				if p >= len(b) {
					return nil, nil, 0, io.ErrUnexpectedEOF
				}
				nameChars := int(b[p])
				p++
				nameBytes := nameChars * 2
				if p+nameBytes > len(b) {
					return nil, nil, 0, io.ErrUnexpectedEOF
				}
				p += nameBytes
				cols = append(cols, col)
			}
		case tokRow:
			if len(cols) == 0 {
				return nil, nil, 0, errors.New("TDS ROW before COLMETADATA")
			}
			r := make([][]byte, len(cols))
			nullRow := make([]bool, len(cols))
			for i, c := range cols {
				v, isNull, np, e := readColumnValue(b, p, c)
				if e != nil {
					return nil, nil, 0, e
				}
				p = np
				if !isNull && (c.typ == typeNVarChar || c.typ == typeNChar) {
					v = []byte(decodeUTF16(v))
				}
				r[i] = v
				nullRow[i] = isNull
			}
			rows = append(rows, r)
			nulls = append(nulls, nullRow)
		case tokNBCRow:
			return nil, nil, 0, errors.New("TDS NBCROW is not emitted by QMigration CAST queries; decoder intentionally rejects it")
		case tokError, tokInfo, tokEnvChange:
			if p+2 > len(b) {
				return nil, nil, 0, io.ErrUnexpectedEOF
			}
			ln := int(binary.LittleEndian.Uint16(b[p : p+2]))
			p += 2
			if p+ln > len(b) {
				return nil, nil, 0, io.ErrUnexpectedEOF
			}
			data := b[p : p+ln]
			p += ln
			if tok == tokError {
				return nil, nil, 0, errors.New(parseTdsError(data))
			}
		case tokDone, tokDoneProc, tokDoneInProc:
			if p+12 > len(b) {
				return nil, nil, 0, io.ErrUnexpectedEOF
			}
			status := binary.LittleEndian.Uint16(b[p : p+2])
			if status&0x10 != 0 {
				rowCount = int64(binary.LittleEndian.Uint64(b[p+4 : p+12]))
			}
			p += 12
		default:
			return nil, nil, 0, fmt.Errorf("unsupported TDS query token 0x%02x", tok)
		}
	}
	return rows, nulls, rowCount, nil
}

func (c *tdsClient) query(ctx context.Context, sql string) ([][][]byte, [][]bool, int64, error) {
	if err := c.writeMessage(ctx, tdsSQLBatch, utf16Bytes(sql)); err != nil {
		return nil, nil, 0, err
	}
	_, b, err := c.readMessage(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	return parseQueryResponse(b)
}
func (c *tdsClient) exec(ctx context.Context, sql string) (int64, error) {
	_, _, n, err := c.query(ctx, sql)
	return n, err
}
func (c *tdsClient) close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// probeTDS performs only the native TDS PRELOGIN exchange. It is deliberately
// authentication-free and is the default SQL Server capability until the
// experimental native LOGIN7/data-plane gate is enabled.
func probeTDS(ctx context.Context, host string, port int) (string, error) {
	d := net.Dialer{}
	nc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return "", err
	}
	defer nc.Close()
	cl := &tdsClient{conn: nc, packetSize: 4096}
	if err := cl.writeMessage(ctx, tdsPrelogin, buildPreloginPacket()[8:]); err != nil {
		return "", fmt.Errorf("PRELOGIN write: %w", err)
	}
	_, body, err := cl.readMessage(ctx)
	if err != nil {
		return "", fmt.Errorf("PRELOGIN read: %w", err)
	}
	pi, err := parsePrelogin(body)
	if err != nil {
		return "", err
	}
	return pi.Version, nil
}
