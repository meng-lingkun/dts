package db2connector

import (
	"bytes"
	"context"
	"crypto/des"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"qmigration/backend/internal/domain"
)

// QMigration's DRDA implementation is intentionally self-contained.  The
// command/code-point sequence follows the public DRDA/DDM protocol: EXCSAT ->
// ACCSEC -> SECCHK -> ACCRDB, then PRPSQLSTT/OPNQRY for queries and
// EXCSQLIMM for dynamic SQL.  No IBM CLI/JDBC runtime is loaded.
const (
	cpEXCSAT    = 0x1041
	cpACCSEC    = 0x106D
	cpSECCHK    = 0x106E
	cpACCRDB    = 0x2001
	cpCNTQRY    = 0x2006
	cpEXCSQLIMM = 0x200A
	cpEXCSQLSTT = 0x200B
	cpOPNQRY    = 0x200C
	cpPRPSQLSTT = 0x200D
	cpRDBCMM    = 0x200E
	cpRDBRLLBCK = 0x200F
	cpSQLDTA    = 0x2412
	cpSQLSTT    = 0x2414
	cpQRYDSC    = 0x241A
	cpQRYDTA    = 0x241B
	cpACCSECRD  = 0x14AC
	cpEXCSATRD  = 0x1443
	cpEXTDTA    = 0x146C
	cpSQLCARD   = 0x2408
	cpSQLDARD   = 0x2411
	cpOPNQRYRM  = 0x2205
	cpENDQRYRM  = 0x220B
	cpENDUOWRM  = 0x220C
	cpSQLERRRM  = 0x2213
	cpRDBNFNRM  = 0x2211
	cpSECCHKRM  = 0x1219

	cpAGENT      = 0x1403
	cpSQLAM      = 0x2407
	cpCMNTCPIP   = 0x1474
	cpRDB        = 0x240F
	cpSECMGR     = 0x1440
	cpUNICODEMGR = 0x1C08
	cpMGRLVLLS   = 0x1404
	cpEXTNAM     = 0x115E
	cpSRVNAM     = 0x116D
	cpSRVRLSLV   = 0x115A
	cpSRVCLSNM   = 0x1147
	cpSECMEC     = 0x11A2
	cpSECTKN     = 0x11DC
	cpRDBNAM     = 0x2110
	cpUSRID      = 0x11A0
	cpPASSWORD   = 0x11A1
	cpRDBACCCL   = 0x210F
	cpPRDID      = 0x112E
	cpTYPDEFNAM  = 0x002F
	cpCRRTKN     = 0x2135
	cpTYPDEFOVR  = 0x0035
	cpPKGNAMCSN  = 0x2113
	cpRDBCMTOK   = 0x2105
	cpRTNSQLDA   = 0x2116
	cpQRYBLKSZ   = 0x2114
	cpMAXBLKEXT  = 0x2141
	cpQRYCLSIMP  = 0x215D
	cpQRYINSID   = 0x215B
	cpRTNEXTDTA  = 0x2148
	cpSRVDGN     = 0x1153
	cpFDODSC     = 0x0010
	cpFDODTA     = 0x147A

	secmecUSRIDPWD  = 3
	secmecEUSRIDPWD = 9

	drdaMaxDSSBytes = 256 << 20
	drdaQueryBlock  = 65535
	drdaMaxSegment  = 32767
	drdaMaxLOBBytes = 256 << 20
	drdaInlineParam = 32000
)

// DRDA FDO types used by DB2 LUW query data.
const (
	drdaInteger      = 0x02
	drdaNInteger     = 0x03
	drdaSmall        = 0x04
	drdaNSmall       = 0x05
	drdaFloat8       = 0x0A
	drdaNFloat8      = 0x0B
	drdaFloat4       = 0x0C
	drdaNFloat4      = 0x0D
	drdaDecimal      = 0x0E
	drdaNDecimal     = 0x0F
	drdaInteger8     = 0x16
	drdaNInteger8    = 0x17
	drdaLOBLoc       = 0x18
	drdaNLOBLoc      = 0x19
	drdaCLOBLoc      = 0x1A
	drdaNCLOBLoc     = 0x1B
	drdaDBCLOBLoc    = 0x1C
	drdaNDBCLOBLoc   = 0x1D
	drdaRowID        = 0x1E
	drdaNRowID       = 0x1F
	drdaDate         = 0x20
	drdaNDate        = 0x21
	drdaTime         = 0x22
	drdaNTime        = 0x23
	drdaTimestamp    = 0x24
	drdaNTimestamp   = 0x25
	drdaFixByte      = 0x26
	drdaNFixByte     = 0x27
	drdaVarByte      = 0x28
	drdaNVarByte     = 0x29
	drdaLongVarByte  = 0x2A
	drdaNLongVarByte = 0x2B
	drdaChar         = 0x30
	drdaNChar        = 0x31
	drdaVarchar      = 0x32
	drdaNVarchar     = 0x33
	drdaLong         = 0x34
	drdaNLong        = 0x35
	drdaGraphic      = 0x36
	drdaNGraphic     = 0x37
	drdaVarGraph     = 0x38
	drdaNVarGraph    = 0x39
	drdaMix          = 0x3C
	drdaNMix         = 0x3D
	drdaVarMix       = 0x3E
	drdaNVarMix      = 0x3F
	drdaBoolean      = 0xBE
	drdaNBoolean     = 0xBF
	drdaFixBytes     = 0xC0
	drdaNFixBytes    = 0xC1
	drdaVarBinary    = 0xC2
	drdaNVarBinary   = 0xC3
	drdaLOBBytes     = 0xC8
	drdaNLOBBytes    = 0xC9
	drdaLOBCSBCS     = 0xCE
	drdaNLOBCSBCS    = 0xCF
)

var cp500ASCII = [128]byte{
	0x00, 0x01, 0x02, 0x03, 0x37, 0x2d, 0x2e, 0x2f, 0x16, 0x05, 0x25, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x3c, 0x3d, 0x32, 0x26, 0x18, 0x19, 0x3f, 0x27, 0x1c, 0x1d, 0x1e, 0x1f,
	0x40, 0x4f, 0x7f, 0x7b, 0x5b, 0x6c, 0x50, 0x7d, 0x4d, 0x5d, 0x5c, 0x4e, 0x6b, 0x60, 0x4b, 0x61,
	0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0x7a, 0x5e, 0x4c, 0x7e, 0x6e, 0x6f,
	0x7c, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6,
	0xd7, 0xd8, 0xd9, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0x4a, 0xe0, 0x5a, 0x5f, 0x6d,
	0x79, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96,
	0x97, 0x98, 0x99, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xc0, 0xbb, 0xd0, 0xa1, 0x07,
}

func encodeCP500(s string) ([]byte, error) {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= 128 {
			return nil, fmt.Errorf("DB2 DRDA CP500 field contains non-ASCII byte at offset %d; use an ASCII database/user credential for native qualification", i)
		}
		out[i] = cp500ASCII[s[i]]
	}
	return out, nil
}

func packDDM(code uint16, body []byte) []byte {
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint16(out[:2], uint16(len(out)))
	binary.BigEndian.PutUint16(out[2:4], code)
	copy(out[4:], body)
	return out
}
func packParam(code uint16, body []byte) []byte { return packDDM(code, body) }
func packUint(code uint16, v uint64, n int) []byte {
	b := make([]byte, n)
	for i := n - 1; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return packParam(code, b)
}
func packCP500(code uint16, s string) ([]byte, error) {
	b, e := encodeCP500(s)
	if e != nil {
		return nil, e
	}
	return packParam(code, b), nil
}
func packNullString(s *string) []byte {
	if s == nil {
		return []byte{0xff}
	}
	b := []byte(*s)
	out := make([]byte, 5+len(b))
	binary.BigEndian.PutUint32(out[1:5], uint32(len(b)))
	copy(out[5:], b)
	return out
}
func join(parts ...[]byte) []byte {
	var n int
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func packEXCSAT() ([]byte, error) {
	mgr := []uint16{cpAGENT, 10, cpSQLAM, 11, cpCMNTCPIP, 5, cpRDB, 12, cpSECMGR, 9, cpUNICODEMGR, 1208}
	mb := make([]byte, 0, len(mgr)*2)
	for _, v := range mgr {
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], v)
		mb = append(mb, b[:]...)
	}
	ex, _ := packCP500(cpEXTNAM, "QMigration")
	sn, _ := packCP500(cpSRVNAM, "QMigration")
	rl, _ := packCP500(cpSRVRLSLV, "0.15")
	sc, _ := packCP500(cpSRVCLSNM, "QMigration")
	return packDDM(cpEXCSAT, join(ex, sn, rl, packParam(cpMGRLVLLS, mb), sc)), nil
}
func packACCSEC(database string, secmec uint16, token []byte) ([]byte, error) {
	db, e := packCP500(cpRDBNAM, database)
	if e != nil {
		return nil, e
	}
	body := join(packUint(cpSECMEC, uint64(secmec), 2), db)
	if len(token) > 0 {
		body = append(body, packParam(cpSECTKN, token)...)
	}
	return packDDM(cpACCSEC, body), nil
}
func packSECCHK(database, user, password string, secmec uint16, serverToken []byte, private *big.Int) ([]byte, error) {
	db, e := packCP500(cpRDBNAM, database)
	if e != nil {
		return nil, e
	}
	body := join(packUint(cpSECMEC, uint64(secmec), 2), db)
	switch secmec {
	case secmecEUSRIDPWD:
		if len(serverToken) != 32 || private == nil {
			return nil, errors.New("DB2 DRDA SECMEC 9 requires 32-byte server security token")
		}
		ub, e := encodeCP500(user)
		if e != nil {
			return nil, e
		}
		pb, e := encodeCP500(password)
		if e != nil {
			return nil, e
		}
		ue, e := encryptDRDADH(serverToken, private, ub)
		if e != nil {
			return nil, e
		}
		pe, e := encryptDRDADH(serverToken, private, pb)
		if e != nil {
			return nil, e
		}
		body = append(body, packParam(cpSECTKN, ue)...)
		body = append(body, packParam(cpSECTKN, pe)...)
	case secmecUSRIDPWD:
		u, e := packCP500(cpUSRID, user)
		if e != nil {
			return nil, e
		}
		p, e := packCP500(cpPASSWORD, password)
		if e != nil {
			return nil, e
		}
		body = append(body, u...)
		body = append(body, p...)
	default:
		return nil, fmt.Errorf("DB2 DRDA negotiated unsupported security mechanism %d", secmec)
	}
	return packDDM(cpSECCHK, body), nil
}
func packACCRDB(database string) ([]byte, error) {
	db, e := packCP500(cpRDBNAM, database)
	if e != nil {
		return nil, e
	}
	prd, _ := packCP500(cpPRDID, "SQL12010")
	typ, _ := packCP500(cpTYPDEFNAM, "QTDSQLX86")
	crr, _ := hex.DecodeString("d5c6f0f0f0f0f0f12ec3f0c1f50155630d5a11")
	ovr, _ := hex.DecodeString("0006119c04b80006119d04b00006119e04b8")
	return packDDM(cpACCRDB, join(db, packUint(cpRDBACCCL, cpSQLAM, 2), prd, typ, packParam(cpCRRTKN, crr), packParam(cpTYPDEFOVR, ovr))), nil
}
func packPKGNAMCSN(database string, section uint16) []byte {
	base := fmt.Sprintf("%-18s%-18s%-18s", database, "NULLID", "SYSSH200")
	b := append([]byte(base), []byte("SYSLVL01")...)
	var s [2]byte
	binary.BigEndian.PutUint16(s[:], section)
	b = append(b, s[:]...)
	return packParam(cpPKGNAMCSN, b)
}
func packPRPSQLSTT(database string) []byte {
	return packDDM(cpPRPSQLSTT, join(packPKGNAMCSN(database, 65), packParam(cpRTNSQLDA, []byte{0xf1})))
}
func packEXCSQLIMM(database string) []byte {
	return packDDM(cpEXCSQLIMM, join(packPKGNAMCSN(database, 65), packParam(cpRDBCMTOK, []byte{0xf1})))
}
func packOPNQRY(database string) []byte {
	return packDDM(cpOPNQRY, join(packPKGNAMCSN(database, 65), packUint(cpQRYBLKSZ, drdaQueryBlock, 4), packUint(cpMAXBLKEXT, drdaQueryBlock, 2), packParam(cpQRYCLSIMP, []byte{1})))
}
func packCNTQRY(database string, qryinsid uint64) []byte {
	return packDDM(cpCNTQRY, join(packPKGNAMCSN(database, 65), packUint(cpQRYBLKSZ, drdaQueryBlock, 4), packUint(cpQRYINSID, qryinsid, 8), packParam(cpRTNEXTDTA, []byte{2})))
}
func packSQLSTT(sql string) []byte {
	return packDDM(cpSQLSTT, join(packNullString(&sql), packNullString(nil)))
}
func packRDBCMM() []byte    { return packDDM(cpRDBCMM, nil) }
func packRDBRLLBCK() []byte { return packDDM(cpRDBRLLBCK, nil) }

var dhPrime, _ = new(big.Int).SetString("C62112D73EE613F0947AB31F0F6846A1BFF5B3A4CA0D60BC1E4C7A0D8C16B3E3", 16)
var dhBase, _ = new(big.Int).SetString("4690FA1F7B9E1D4442C86C9114603FDECF071EDCEC5F626E21E256AED9EA34E4", 16)

func newDHPrivate() (*big.Int, error) {
	max := new(big.Int).Sub(dhPrime, big.NewInt(3))
	n, e := rand.Int(rand.Reader, max)
	if e != nil {
		return nil, e
	}
	return n.Add(n, big.NewInt(2)), nil
}
func dhPublic(priv *big.Int) []byte {
	v := new(big.Int).Exp(dhBase, priv, dhPrime).Bytes()
	out := make([]byte, 32)
	copy(out[32-len(v):], v)
	return out
}
func encryptDRDADH(serverToken []byte, private *big.Int, plain []byte) ([]byte, error) {
	pub := new(big.Int).SetBytes(serverToken)
	shared := new(big.Int).Exp(pub, private, dhPrime).Bytes()
	keybuf := make([]byte, 32)
	copy(keybuf[32-len(shared):], shared)
	block, e := des.NewCipher(keybuf[12:20])
	if e != nil {
		return nil, e
	}
	pad := 8 - (len(plain) % 8)
	if pad == 0 {
		pad = 8
	}
	padded := append(append([]byte{}, plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	out := make([]byte, len(padded))
	iv := append([]byte{}, serverToken[12:20]...)
	prev := iv
	tmp := make([]byte, 8)
	for i := 0; i < len(padded); i += 8 {
		for j := 0; j < 8; j++ {
			tmp[j] = padded[i+j] ^ prev[j]
		}
		block.Encrypt(out[i:i+8], tmp)
		prev = out[i : i+8]
	}
	return out, nil
}

type dssPacket struct {
	typ     byte
	chained bool
	corr    uint16
	code    uint16
	body    []byte
	more    bool
}

type drdaClient struct {
	conn          net.Conn
	ds            domain.DataSource
	database      string
	endian        binary.ByteOrder
	inTransaction bool
}

func tlsConfig(ds domain.DataSource) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ds.TLSServerName}
	if cfg.ServerName == "" {
		cfg.ServerName = ds.Host
	}
	if strings.TrimSpace(ds.TLSCACert) != "" {
		roots, e := x509.SystemCertPool()
		if e != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM([]byte(ds.TLSCACert)) {
			return nil, errors.New("DB2 TLS CA certificate is invalid")
		}
		cfg.RootCAs = roots
	}
	if strings.TrimSpace(ds.TLSClientCert) != "" || strings.TrimSpace(ds.TLSClientKey) != "" {
		if strings.TrimSpace(ds.TLSClientCert) == "" || strings.TrimSpace(ds.TLSClientKey) == "" {
			return nil, errors.New("DB2 TLS client certificate and key must be provided together")
		}
		crt, e := tls.X509KeyPair([]byte(ds.TLSClientCert), []byte(ds.TLSClientKey))
		if e != nil {
			return nil, fmt.Errorf("DB2 TLS client certificate: %w", e)
		}
		cfg.Certificates = []tls.Certificate{crt}
	}
	return cfg, nil
}
func dialRaw(ctx context.Context, ds domain.DataSource, useTLS bool) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	raw, e := d.DialContext(ctx, "tcp", net.JoinHostPort(ds.Host, strconv.Itoa(ds.Port)))
	if e != nil {
		return nil, e
	}
	if !useTLS {
		return raw, nil
	}
	cfg, e := tlsConfig(ds)
	if e != nil {
		raw.Close()
		return nil, e
	}
	tc := tls.Client(raw, cfg)
	if dl, ok := ctx.Deadline(); ok {
		_ = tc.SetDeadline(dl)
	}
	if e = tc.HandshakeContext(ctx); e != nil {
		raw.Close()
		return nil, e
	}
	_ = tc.SetDeadline(time.Time{})
	return tc, nil
}
func dialDRDA(ctx context.Context, ds domain.DataSource) (*drdaClient, error) {
	var conn net.Conn
	var e error
	switch ds.TLSMode {
	case domain.TLSModeRequired:
		conn, e = dialRaw(ctx, ds, true)
	case domain.TLSModePreferred:
		conn, e = dialRaw(ctx, ds, true)
		if e != nil {
			conn, e = dialRaw(ctx, ds, false)
		}
	default:
		conn, e = dialRaw(ctx, ds, false)
	}
	if e != nil {
		return nil, e
	}
	db := strings.TrimSpace(ds.Database)
	if db == "" {
		conn.Close()
		return nil, errors.New("DB2 database/RDB name is required")
	}
	if len(db) > 18 {
		conn.Close()
		return nil, errors.New("DB2 database/RDB name exceeds DRDA 18-byte limit")
	}
	db = fmt.Sprintf("%-18s", db)
	c := &drdaClient{conn: conn, ds: ds, database: db, endian: binary.LittleEndian}
	if e = c.handshake(ctx); e != nil {
		conn.Close()
		return nil, e
	}
	return c, nil
}
func probeDRDA(ctx context.Context, ds domain.DataSource) (string, error) {
	var conn net.Conn
	var e error
	switch ds.TLSMode {
	case domain.TLSModeRequired:
		conn, e = dialRaw(ctx, ds, true)
	case domain.TLSModePreferred:
		conn, e = dialRaw(ctx, ds, true)
		if e != nil {
			conn, e = dialRaw(ctx, ds, false)
		}
	default:
		conn, e = dialRaw(ctx, ds, false)
	}
	if e != nil {
		return "", e
	}
	defer conn.Close()
	ex, _ := packEXCSAT()
	if _, e = sendDSS(conn, ex, 1, false, true); e != nil {
		return "", e
	}
	p, e := readDSS(conn)
	if e != nil {
		return "", e
	}
	if p.code != cpEXCSATRD {
		return "", fmt.Errorf("DB2 DRDA probe expected EXCSATRD, got 0x%04x", p.code)
	}
	return "db2-drda", nil
}
func (c *drdaClient) close() error {
	if c.conn == nil {
		return nil
	}
	if c.inTransaction {
		_ = c.rollback(context.Background())
	}
	e := c.conn.Close()
	c.conn = nil
	return e
}
func setDeadline(ctx context.Context, conn net.Conn) func() {
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	return func() { _ = conn.SetDeadline(time.Time{}) }
}
func sendDSS(conn net.Conn, obj []byte, corr uint16, nextSame, last bool) (uint16, error) {
	if len(obj) < 4 {
		return corr, errors.New("DB2 DRDA object is shorter than 4-byte header")
	}
	return sendDSSPayload(conn, binary.BigEndian.Uint16(obj[2:4]), obj[4:], corr, nextSame, last)
}

func sendDSSPayload(conn net.Conn, code uint16, body []byte, corr uint16, nextSame, last bool) (uint16, error) {
	if len(body) > drdaMaxDSSBytes {
		return corr, fmt.Errorf("DB2 DRDA object 0x%04x exceeds %d-byte safety limit: %d", code, drdaMaxDSSBytes, len(body))
	}
	flag := byte(1)
	switch code {
	case cpSQLSTT, cpSQLDTA, cpEXTDTA:
		flag = 3
	}
	if !last {
		flag |= 0x40
	}
	next := corr + 1
	if nextSame {
		flag |= 0x10
		next = corr
	}

	// Normal DSS/object form. Keep it below DRDA's 32767-byte segment
	// boundary so large SQLDTA/EXTDTA always use the protocol's extended
	// object framing instead of relying on uint16 wraparound.
	if len(body)+10 <= drdaMaxSegment {
		total := len(body) + 10
		h := make([]byte, 10)
		binary.BigEndian.PutUint16(h[:2], uint16(total))
		h[2] = 0xd0
		h[3] = flag
		binary.BigEndian.PutUint16(h[4:6], corr)
		binary.BigEndian.PutUint16(h[6:8], uint16(len(body)+4))
		binary.BigEndian.PutUint16(h[8:10], code)
		if e := writeFull(conn, h); e != nil {
			return corr, e
		}
		if e := writeFull(conn, body); e != nil {
			return corr, e
		}
		return next, nil
	}

	// Extended DDM object + continued DSS segments. The object length
	// field is 0x8008: 0x8004 plus four bytes carrying the real body
	// length. Each DSS continuation segment has its own 2-byte length and
	// the high bit marks whether another segment follows.
	const extLen = 4
	firstCap := drdaMaxSegment - 6 - 4 - extLen
	firstN := len(body)
	if firstN > firstCap {
		firstN = firstCap
	}
	segLen := 6 + 4 + extLen + firstN
	if firstN < len(body) {
		segLen |= 0x8000
	}
	h := make([]byte, 14)
	binary.BigEndian.PutUint16(h[:2], uint16(segLen))
	h[2] = 0xd0
	h[3] = flag
	binary.BigEndian.PutUint16(h[4:6], corr)
	binary.BigEndian.PutUint16(h[6:8], uint16(0x8004+extLen))
	binary.BigEndian.PutUint16(h[8:10], code)
	binary.BigEndian.PutUint32(h[10:14], uint32(len(body)))
	if e := writeFull(conn, h); e != nil {
		return corr, e
	}
	if e := writeFull(conn, body[:firstN]); e != nil {
		return corr, e
	}
	rest := body[firstN:]
	for len(rest) > 0 {
		n := len(rest)
		if n > drdaMaxSegment-2 {
			n = drdaMaxSegment - 2
		}
		ln := n + 2
		if n < len(rest) {
			ln |= 0x8000
		}
		var lb [2]byte
		binary.BigEndian.PutUint16(lb[:], uint16(ln))
		if e := writeFull(conn, lb[:]); e != nil {
			return corr, e
		}
		if e := writeFull(conn, rest[:n]); e != nil {
			return corr, e
		}
		rest = rest[n:]
	}
	return next, nil
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n <= 0 {
			return io.ErrUnexpectedEOF
		}
		b = b[n:]
	}
	return nil
}
func readFull(r io.Reader, n int) ([]byte, error) {
	if n < 0 || n > drdaMaxDSSBytes {
		return nil, fmt.Errorf("DB2 DRDA invalid read length %d", n)
	}
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	return b, e
}
func readDSS(conn net.Conn) (dssPacket, error) {
	h, e := readFull(conn, 10)
	if e != nil {
		return dssPacket{}, e
	}
	if len(h) != 10 || h[2] != 0xd0 {
		return dssPacket{}, fmt.Errorf("invalid DB2 DRDA DSS header %x", h)
	}
	totalRaw := int(binary.BigEndian.Uint16(h[:2]))
	p := dssPacket{typ: h[3] & 0x0f, chained: h[3]&0x40 != 0, corr: binary.BigEndian.Uint16(h[4:6])}
	objRaw := int(binary.BigEndian.Uint16(h[6:8]))
	p.code = binary.BigEndian.Uint16(h[8:10])
	continued := totalRaw&0x8000 != 0 || objRaw&0x8000 != 0
	if !continued {
		if totalRaw < 10 || objRaw < 4 || objRaw != totalRaw-6 {
			return p, fmt.Errorf("DB2 DRDA invalid DSS/object lengths total=%d object=%d", totalRaw, objRaw)
		}
		p.body, e = readFull(conn, objRaw-4)
		return p, e
	}

	extLen := objRaw - 0x8004
	if objRaw&0x8000 == 0 {
		extLen = 0
	}
	if extLen < 0 || extLen > 8 {
		return p, fmt.Errorf("DB2 DRDA invalid extended object length 0x%04x", objRaw)
	}
	var declared uint64
	if extLen > 0 {
		eb, er := readFull(conn, extLen)
		if er != nil {
			return p, er
		}
		for _, b := range eb {
			declared = declared<<8 | uint64(b)
		}
		if declared > drdaMaxDSSBytes {
			return p, fmt.Errorf("DB2 DRDA extended object exceeds %d-byte safety limit: %d", drdaMaxDSSBytes, declared)
		}
	}
	segLen := totalRaw & 0x7fff
	more := totalRaw&0x8000 != 0
	first := segLen - 10 - extLen
	if first < 0 {
		return p, fmt.Errorf("DB2 DRDA invalid continued first segment length %d", segLen)
	}
	if declared > 0 {
		p.body = make([]byte, 0, int(declared))
	}
	b, er := readFull(conn, first)
	if er != nil {
		return p, er
	}
	p.body = append(p.body, b...)
	for more {
		lb, er := readFull(conn, 2)
		if er != nil {
			return p, er
		}
		nraw := int(binary.BigEndian.Uint16(lb))
		more = nraw&0x8000 != 0
		n := nraw & 0x7fff
		if n < 2 {
			return p, fmt.Errorf("DB2 DRDA invalid continuation segment length %d", n)
		}
		if len(p.body)+(n-2) > drdaMaxDSSBytes {
			return p, fmt.Errorf("DB2 DRDA continued object exceeds %d-byte safety limit", drdaMaxDSSBytes)
		}
		b, er = readFull(conn, n-2)
		if er != nil {
			return p, er
		}
		p.body = append(p.body, b...)
	}
	if declared > 0 && uint64(len(p.body)) != declared {
		return p, fmt.Errorf("DB2 DRDA extended object length mismatch: declared=%d actual=%d", declared, len(p.body))
	}
	p.more = false
	return p, nil
}

func parseParams(body []byte) map[uint16][][]byte {
	out := map[uint16][][]byte{}
	for len(body) >= 4 {
		n := int(binary.BigEndian.Uint16(body[:2]))
		if n < 4 || n > len(body) {
			break
		}
		cp := binary.BigEndian.Uint16(body[2:4])
		out[cp] = append(out[cp], append([]byte{}, body[4:n]...))
		body = body[n:]
	}
	return out
}
func responseErrorClient(c *drdaClient, p dssPacket) error {
	switch p.code {
	case cpSQLERRRM, cpRDBNFNRM:
		d := parseParams(p.body)
		msg := ""
		if v := d[cpSRVDGN]; len(v) > 0 {
			msg = fmt.Sprintf(" diagnostic=%x", v[0])
		}
		return fmt.Errorf("DB2 DRDA error reply 0x%04x%s", p.code, msg)
	case cpSECCHKRM:
		d := parseParams(p.body)
		if vals := d[0x11A4]; len(vals) > 0 && len(vals[0]) > 0 && vals[0][0] != 0 {
			return fmt.Errorf("DB2 security check failed code=%d", vals[0][0])
		}
	case cpSQLCARD:
		if len(p.body) >= 10 && p.body[0] == 0 {
			code := int32(c.endian.Uint32(p.body[1:5]))
			state := string(p.body[5:10])
			if code < 0 {
				return fmt.Errorf("DB2 SQL error SQLCODE=%d SQLSTATE=%s", code, state)
			}
		}
	}
	return nil
}
func (c *drdaClient) handshake(ctx context.Context) error {
	defer setDeadline(ctx, c.conn)()
	priv, e := newDHPrivate()
	if e != nil {
		return e
	}
	ex, _ := packEXCSAT()
	ac, e := packACCSEC(c.database, secmecEUSRIDPWD, dhPublic(priv))
	if e != nil {
		return e
	}
	id := uint16(1)
	if id, e = sendDSS(c.conn, ex, id, false, false); e != nil {
		return e
	}
	if _, e = sendDSS(c.conn, ac, id, false, true); e != nil {
		return e
	}
	packets, e := c.readChainNoDeadline()
	if e != nil {
		return e
	}
	secmec := uint16(0)
	var token []byte
	for _, p := range packets {
		if p.code == cpACCSECRD {
			d := parseParams(p.body)
			if v := d[cpSECMEC]; len(v) > 0 && len(v[0]) >= 2 {
				secmec = binary.BigEndian.Uint16(v[0][:2])
			}
			if v := d[cpSECTKN]; len(v) > 0 {
				token = append([]byte{}, v[0]...)
			}
		}
	}
	if secmec == 0 {
		return errors.New("DB2 DRDA ACCSEC did not return a security mechanism")
	}
	if secmec == secmecUSRIDPWD {
		if _, ok := c.conn.(*tls.Conn); !ok {
			return errors.New("DB2 DRDA server negotiated plaintext SECMEC 3 without TLS; refusing to send credentials")
		}
	}
	if secmec != secmecEUSRIDPWD {
		ac, e = packACCSEC(c.database, secmec, func() []byte {
			if secmec == secmecEUSRIDPWD {
				return dhPublic(priv)
			}
			return nil
		}())
		if e != nil {
			return e
		}
		if _, e = sendDSS(c.conn, ac, 1, false, true); e != nil {
			return e
		}
		packets, e = c.readChainNoDeadline()
		if e != nil {
			return e
		}
		token = nil
		for _, p := range packets {
			if p.code == cpACCSECRD {
				d := parseParams(p.body)
				if v := d[cpSECTKN]; len(v) > 0 {
					token = append([]byte{}, v[0]...)
				}
			}
		}
	}
	sec, e := packSECCHK(c.database, c.ds.Username, c.ds.Password, secmec, token, priv)
	if e != nil {
		return e
	}
	rdb, e := packACCRDB(c.database)
	if e != nil {
		return e
	}
	id = 1
	if id, e = sendDSS(c.conn, sec, id, false, false); e != nil {
		return e
	}
	if _, e = sendDSS(c.conn, rdb, id, false, true); e != nil {
		return e
	}
	_, e = c.readChainNoDeadline()
	return e
}
func (c *drdaClient) readChainNoDeadline() ([]dssPacket, error) {
	var out []dssPacket
	for {
		p, e := readDSS(c.conn)
		if e != nil {
			return nil, e
		}
		if e = responseErrorClient(c, p); e != nil {
			return nil, e
		}
		out = append(out, p)
		if !p.chained {
			return out, nil
		}
	}
}

type drdaFieldDesc struct {
	typ   byte
	param [2]byte
}
type drdaCell struct {
	Null      bool
	Data      []byte
	lob       bool
	clob      bool
	inlineLOB bool
}
type byteCursor struct {
	b []byte
	i int
}

func (r *byteCursor) take(n int) ([]byte, error) {
	if n < 0 || r.i+n > len(r.b) {
		return nil, io.ErrUnexpectedEOF
	}
	v := r.b[r.i : r.i+n]
	r.i += n
	return v, nil
}
func (r *byteCursor) remaining() int { return len(r.b) - r.i }
func nullableDRDA(t byte) bool {
	switch t {
	case drdaNInteger, drdaNSmall, drdaNFloat8, drdaNFloat4, drdaNDecimal, drdaNInteger8, drdaNLOBLoc, drdaNCLOBLoc, drdaNDBCLOBLoc, drdaNRowID, drdaNDate, drdaNTime, drdaNTimestamp, drdaNFixByte, drdaNVarByte, drdaNLongVarByte, drdaNChar, drdaNVarchar, drdaNLong, drdaNGraphic, drdaNVarGraph, drdaNMix, drdaNVarMix, drdaNBoolean, drdaNFixBytes, drdaNVarBinary, drdaNLOBBytes, drdaNLOBCSBCS:
		return true
	}
	return false
}
func parseQRYDSC(body []byte) ([]drdaFieldDesc, error) {
	if len(body) < 3 {
		return nil, errors.New("DB2 QRYDSC truncated")
	}
	ln := int(body[0])
	if ln > len(body) {
		return nil, errors.New("DB2 QRYDSC length overflow")
	}
	b := body[1:ln]
	if len(b) < 2 || b[0] != 0x76 || b[1] != 0xd0 {
		return nil, fmt.Errorf("DB2 QRYDSC unsupported header %x", b)
	}
	b = b[2:]
	if len(b)%3 != 0 {
		return nil, errors.New("DB2 QRYDSC descriptor is not 3-byte aligned")
	}
	out := make([]drdaFieldDesc, 0, len(b)/3)
	for len(b) >= 3 {
		out = append(out, drdaFieldDesc{typ: b[0], param: [2]byte{b[1], b[2]}})
		b = b[3:]
	}
	return out, nil
}
func packedDecimalString(b []byte, precision, scale int) (string, error) {
	if len(b) == 0 {
		return "", errors.New("empty packed decimal")
	}
	hexs := hex.EncodeToString(b)
	sign := ""
	last := hexs[len(hexs)-1]
	if last == 'd' || last == 'b' {
		sign = "-"
	} else if last != 'c' && last != 'f' && last != 'a' && last != 'e' {
		return "", fmt.Errorf("invalid packed decimal sign %c", last)
	}
	digits := hexs[:len(hexs)-1]
	if precision > 0 && len(digits) > precision {
		digits = digits[len(digits)-precision:]
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		p := len(digits) - scale
		digits = digits[:p] + "." + digits[p:]
	}
	if sign != "" && strings.Trim(digits, "0.") != "" {
		digits = sign + digits
	}
	return digits, nil
}
func readDRDAField(r *byteCursor, d drdaFieldDesc, endian binary.ByteOrder) (drdaCell, error) {
	if nullableDRDA(d.typ) {
		b, e := r.take(1)
		if e != nil {
			return drdaCell{}, e
		}
		if b[0] == 0xff {
			return drdaCell{Null: true}, nil
		}
	}
	plen := int(binary.BigEndian.Uint16(d.param[:]))
	takeRaw := func(n int) (drdaCell, error) { b, e := r.take(n); return drdaCell{Data: append([]byte{}, b...)}, e }
	switch d.typ {
	case drdaChar, drdaNChar, drdaMix, drdaNMix, drdaGraphic, drdaNGraphic, drdaVarGraph, drdaNVarGraph:
		c, e := takeRaw(plen)
		c.Data = bytes.TrimRight(c.Data, " ")
		return c, e
	case drdaVarchar, drdaNVarchar, drdaLong, drdaNLong, drdaVarMix, drdaNVarMix:
		lb, e := r.take(2)
		if e != nil {
			return drdaCell{}, e
		}
		n := int(binary.BigEndian.Uint16(lb))
		return takeRaw(n)
	case drdaFixByte, drdaNFixByte, drdaFixBytes, drdaNFixBytes, drdaRowID, drdaNRowID:
		return takeRaw(plen & 0x7fff)
	case drdaVarByte, drdaNVarByte, drdaVarBinary, drdaNVarBinary:
		lb, e := r.take(2)
		if e != nil {
			return drdaCell{}, e
		}
		return takeRaw(int(binary.BigEndian.Uint16(lb)))
	case drdaLongVarByte, drdaNLongVarByte:
		lb, e := r.take(4)
		if e != nil {
			return drdaCell{}, e
		}
		return takeRaw(int(binary.BigEndian.Uint32(lb)))
	case drdaSmall, drdaNSmall, drdaInteger, drdaNInteger, drdaInteger8, drdaNInteger8:
		b, e := r.take(plen)
		if e != nil {
			return drdaCell{}, e
		}
		var v int64
		switch len(b) {
		case 1:
			v = int64(int8(b[0]))
		case 2:
			v = int64(int16(endian.Uint16(b)))
		case 4:
			v = int64(int32(endian.Uint32(b)))
		case 8:
			v = int64(endian.Uint64(b))
		default:
			return drdaCell{}, fmt.Errorf("DB2 integer DRDA length %d", len(b))
		}
		return drdaCell{Data: []byte(strconv.FormatInt(v, 10))}, nil
	case drdaFloat4, drdaNFloat4:
		b, e := r.take(plen)
		if e != nil {
			return drdaCell{}, e
		}
		if len(b) != 4 {
			return drdaCell{}, errors.New("DB2 FLOAT4 invalid length")
		}
		v := math.Float32frombits(endian.Uint32(b))
		return drdaCell{Data: []byte(strconv.FormatFloat(float64(v), 'g', -1, 32))}, nil
	case drdaFloat8, drdaNFloat8:
		b, e := r.take(plen)
		if e != nil {
			return drdaCell{}, e
		}
		if len(b) != 8 {
			return drdaCell{}, errors.New("DB2 FLOAT8 invalid length")
		}
		v := math.Float64frombits(endian.Uint64(b))
		return drdaCell{Data: []byte(strconv.FormatFloat(v, 'g', -1, 64))}, nil
	case drdaDecimal, drdaNDecimal:
		precision, scale := int(d.param[0]), int(d.param[1])
		n := (precision + 1 + 1) / 2
		b, e := r.take(n)
		if e != nil {
			return drdaCell{}, e
		}
		s, e := packedDecimalString(b, precision, scale)
		return drdaCell{Data: []byte(s)}, e
	case drdaDate, drdaNDate:
		return takeRaw(plen)
	case drdaTime, drdaNTime:
		c, e := takeRaw(plen)
		if e == nil {
			c.Data = []byte(strings.ReplaceAll(strings.TrimSpace(string(c.Data)), ".", ":"))
		}
		return c, e
	case drdaTimestamp, drdaNTimestamp:
		c, e := takeRaw(plen)
		if e == nil {
			v := strings.TrimSpace(string(c.Data))
			if len(v) >= 19 {
				v = v[:10] + " " + strings.ReplaceAll(v[11:], ".", ":")
				// Restore the fractional-second separator after HH:MM:SS.
				if len(v) > 19 {
					v = v[:19] + "." + strings.ReplaceAll(v[20:], ":", "")
				}
			}
			c.Data = []byte(v)
		}
		return c, e
	case drdaBoolean, drdaNBoolean:
		b, e := r.take(plen)
		if e != nil {
			return drdaCell{}, e
		}
		truth := false
		for _, x := range b {
			truth = truth || x != 0
		}
		if truth {
			return drdaCell{Data: []byte("1")}, nil
		}
		return drdaCell{Data: []byte("0")}, nil
	case drdaLOBLoc, drdaNLOBLoc, drdaCLOBLoc, drdaNCLOBLoc, drdaDBCLOBLoc, drdaNDBCLOBLoc:
		_, e := r.take(plen)
		return drdaCell{lob: true, clob: d.typ == drdaCLOBLoc || d.typ == drdaNCLOBLoc || d.typ == drdaDBCLOBLoc || d.typ == drdaNDBCLOBLoc}, e
	case drdaLOBBytes, drdaNLOBBytes, drdaLOBCSBCS, drdaNLOBCSBCS:
		_, e := r.take(plen & 0x7fff)
		return drdaCell{lob: true, clob: d.typ == drdaLOBCSBCS || d.typ == drdaNLOBCSBCS, inlineLOB: true}, e
	default:
		return drdaCell{}, fmt.Errorf("unsupported DB2 DRDA field type 0x%02x", d.typ)
	}
}
func parseQRYDTA(body []byte, desc []drdaFieldDesc, endian binary.ByteOrder) ([][]drdaCell, error) {
	r := &byteCursor{b: body}
	var rows [][]drdaCell
	for r.remaining() >= 2 {
		h, e := r.take(2)
		if e != nil {
			return nil, e
		}
		if h[0] != 0xff {
			break
		}
		row := make([]drdaCell, 0, len(desc))
		for _, d := range desc {
			c, e := readDRDAField(r, d, endian)
			if e != nil {
				return nil, e
			}
			row = append(row, c)
		}
		rows = append(rows, row)
	}
	return rows, nil
}
func applyEXTDTA(rows [][]drdaCell, ext [][]byte) {
	idx := 0
	for i := range rows {
		for j := range rows[i] {
			c := &rows[i][j]
			if !c.lob || c.Null {
				continue
			}
			if idx >= len(ext) {
				continue
			}
			b := append([]byte{}, ext[idx]...)
			idx++
			if c.inlineLOB && len(b) > 0 {
				b = b[1:]
			}
			c.Data = b
		}
	}
}

func (c *drdaClient) query(ctx context.Context, sql string) ([][]drdaCell, error) {
	defer setDeadline(ctx, c.conn)()
	stmt := packSQLSTT(sql)
	if len(stmt)+6 > 0xffff {
		return nil, fmt.Errorf("DB2 native query SQLSTT is %d bytes and exceeds the inline DRDA statement limit", len(stmt)+6)
	}
	id := uint16(1)
	var e error
	if id, e = sendDSS(c.conn, packPRPSQLSTT(c.database), id, true, false); e != nil {
		return nil, e
	}
	if id, e = sendDSS(c.conn, stmt, id, false, false); e != nil {
		return nil, e
	}
	if _, e = sendDSS(c.conn, packOPNQRY(c.database), id, false, true); e != nil {
		return nil, e
	}
	var desc []drdaFieldDesc
	var rows [][]drdaCell
	var ext [][]byte
	needContinue := false
	var qryinsid uint64
	corr := uint16(1)
	for {
		for {
			p, e := readDSS(c.conn)
			if e != nil {
				return nil, e
			}
			if e = responseErrorClient(c, p); e != nil {
				return nil, e
			}
			switch p.code {
			case cpQRYDSC:
				desc, e = parseQRYDSC(p.body)
				if e != nil {
					return nil, e
				}
			case cpQRYDTA:
				if len(desc) == 0 {
					return nil, errors.New("DB2 QRYDTA arrived before QRYDSC")
				}
				rr, e := parseQRYDTA(p.body, desc, c.endian)
				if e != nil {
					return nil, e
				}
				rows = append(rows, rr...)
			case cpEXTDTA:
				ext = append(ext, append([]byte{}, p.body...))
			case cpOPNQRYRM:
				corr = p.corr
				d := parseParams(p.body)
				if v := d[cpQRYINSID]; len(v) > 0 && len(v[0]) >= 8 {
					qryinsid = binary.BigEndian.Uint64(v[0][:8])
				}
				needContinue = true
			case cpENDQRYRM, cpENDUOWRM:
				needContinue = false
			}
			if p.more {
				needContinue = true
			}
			if !p.chained {
				break
			}
		}
		if !needContinue {
			break
		}
		if _, e = sendDSS(c.conn, packCNTQRY(c.database, qryinsid), corr, false, true); e != nil {
			return nil, e
		}
	}
	applyEXTDTA(rows, ext)
	return rows, nil
}
func (c *drdaClient) exec(ctx context.Context, sql string, autoCommit bool) error {
	defer setDeadline(ctx, c.conn)()
	stmt := packSQLSTT(sql)
	if len(stmt)+6 > 0xffff {
		return fmt.Errorf("DB2 native target SQLSTT is %d bytes; RC5 fails closed above the DRDA inline statement limit until prepared/EXTDTA target streaming is enabled", len(stmt)+6)
	}
	id := uint16(1)
	var e error
	if id, e = sendDSS(c.conn, packEXCSQLIMM(c.database), id, true, false); e != nil {
		return e
	}
	if autoCommit {
		if id, e = sendDSS(c.conn, stmt, id, false, false); e != nil {
			return e
		}
		if _, e = sendDSS(c.conn, packRDBCMM(), id, false, true); e != nil {
			return e
		}
	} else {
		if _, e = sendDSS(c.conn, stmt, id, false, true); e != nil {
			return e
		}
	}
	_, e = c.readChainNoDeadline()
	return e
}
func (c *drdaClient) commit(ctx context.Context) error {
	defer setDeadline(ctx, c.conn)()
	if _, e := sendDSS(c.conn, packRDBCMM(), 1, false, true); e != nil {
		return e
	}
	_, e := c.readChainNoDeadline()
	if e == nil {
		c.inTransaction = false
	}
	return e
}
func (c *drdaClient) rollback(ctx context.Context) error {
	defer setDeadline(ctx, c.conn)()
	if _, e := sendDSS(c.conn, packRDBRLLBCK(), 1, false, true); e != nil {
		return e
	}
	_, e := c.readChainNoDeadline()
	if e == nil {
		c.inTransaction = false
	}
	return e
}
