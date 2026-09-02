package oracleconnector

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ttcAuthDictionaryMessage byte   = 8
	oracleLogonNoNewPassword uint64 = 0x1
	oracleLogonUserPassword  uint64 = 0x100
)

type oracleAuthChallenge struct {
	EncryptedServerSessionKey string
	Salt                      string
	VerifierType              int
}

type oracleAuthProof struct {
	EncryptedClientSessionKey string
	EncryptedPassword         string
	KeyHash                   []byte
}

// parseOracleAuthChallenge decodes a TTC dictionary message and extracts only
// the authentication material QMigration needs. Unknown keys are intentionally
// ignored so newer Oracle releases can add session properties without breaking
// the authentication boundary.
func parseOracleAuthChallenge(payload []byte) (oracleAuthChallenge, error) {
	var out oracleAuthChallenge
	r := newTTCDecoder(payload)
	code, err := r.byte()
	if err != nil {
		return out, err
	}
	if code != ttcAuthDictionaryMessage {
		return out, fmt.Errorf("Oracle TTC auth challenge message code %d, expected %d", code, ttcAuthDictionaryMessage)
	}
	n, err := r.compactUint(4)
	if err != nil {
		return out, err
	}
	if n > 256 {
		return out, fmt.Errorf("Oracle TTC auth dictionary too large: %d", n)
	}
	for i := uint64(0); i < n; i++ {
		k, v, f, err := r.keyVal()
		if err != nil {
			return out, err
		}
		switch string(k) {
		case "AUTH_SESSKEY":
			out.EncryptedServerSessionKey = string(v)
		case "AUTH_VFR_DATA":
			out.Salt = string(v)
			out.VerifierType = int(f)
		}
	}
	if out.EncryptedServerSessionKey == "" {
		return out, errors.New("Oracle TTC auth challenge missing AUTH_SESSKEY")
	}
	return out, nil
}

func buildOracleAuthInit(username string, client oracleClientIdentity) ([]byte, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, errors.New("Oracle username is required for TTC authentication")
	}
	w := &ttcEncoder{}
	w.Write([]byte{3, 118, 0, 1})
	w.compactUint(uint64(len(username)), 4)
	w.compactUint(oracleLogonNoNewPassword, 4)
	w.Write([]byte{1, 1, 5, 1, 1})
	w.clr([]byte(username))
	for _, kv := range [][3]string{{"AUTH_TERMINAL", client.Terminal, "0"}, {"AUTH_PROGRAM_NM", client.Program, "0"}, {"AUTH_MACHINE", client.Machine, "0"}, {"AUTH_PID", client.PID, "0"}, {"AUTH_SID", client.OSUser, "0"}} {
		flag, _ := strconv.Atoi(kv[2])
		w.keyVal([]byte(kv[0]), []byte(kv[1]), uint32(flag))
	}
	return w.Bytes(), nil
}

type oracleClientIdentity struct {
	Terminal, Program, Machine, PID, OSUser, ConnectString string
	Charset                                                uint16
}

func buildOracleAuthResponse(username string, proof oracleAuthProof, client oracleClientIdentity) ([]byte, error) {
	username = strings.TrimSpace(username)
	if username == "" || proof.EncryptedPassword == "" {
		return nil, errors.New("Oracle TTC auth response requires username and encrypted password")
	}
	const slots = 22
	w := &ttcEncoder{}
	w.Write([]byte{3, 0x73, 0})
	w.byte(1)
	w.compactUint(uint64(len(username)), 4)
	w.compactUint(oracleLogonNoNewPassword|oracleLogonUserPassword, 4)
	w.byte(1)
	w.compactUint(slots, 4)
	w.Write([]byte{1, 1})
	w.clr([]byte(username))
	used := 0
	add := func(k, v string, flag uint32) { w.keyVal([]byte(k), []byte(v), flag); used++ }
	if proof.EncryptedClientSessionKey != "" {
		add("AUTH_SESSKEY", proof.EncryptedClientSessionKey, 1)
	}
	add("AUTH_PASSWORD", proof.EncryptedPassword, 0)
	add("AUTH_TERMINAL", client.Terminal, 0)
	add("AUTH_PROGRAM_NM", client.Program, 0)
	add("AUTH_MACHINE", client.Machine, 0)
	add("AUTH_PID", client.PID, 0)
	add("AUTH_SID", client.OSUser, 0)
	add("AUTH_CONNECT_STRING", client.ConnectString, 0)
	add("SESSION_CLIENT_CHARSET", strconv.Itoa(int(client.Charset)), 0)
	add("SESSION_CLIENT_LIB_TYPE", "0", 0)
	add("SESSION_CLIENT_DRIVER_NAME", "QMigration", 0)
	add("SESSION_CLIENT_VERSION", "0.15", 0)
	add("SESSION_CLIENT_LOBATTR", "1", 0)
	_, off := time.Now().Zone()
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	tz := fmt.Sprintf("%s%02d:%02d", sign, off/3600, (off/60)%60)
	add("AUTH_ALTER_SESSION", fmt.Sprintf("ALTER SESSION SET NLS_LANGUAGE='AMERICAN' NLS_TERRITORY='AMERICA' TIME_ZONE='%s'\x00", tz), 1)
	for used < slots {
		w.keyVal(nil, nil, 0)
		used++
	}
	return w.Bytes(), nil
}

func deriveOracleAuthProof(username, password string, challenge oracleAuthChallenge, compileCaps []byte, random io.Reader) (oracleAuthProof, error) {
	var out oracleAuthProof
	if password == "" {
		return out, errors.New("Oracle password is required")
	}
	if random == nil {
		random = rand.Reader
	}
	var key []byte
	padding := false
	switch challenge.VerifierType {
	case 0:
		k, err := legacyOraclePasswordKey(username, password)
		if err != nil {
			return out, err
		}
		server, err := desCBCDecryptHex(k[:8], challenge.EncryptedServerSessionKey)
		if err != nil {
			return out, err
		}
		ep, err := desCBCEncryptPassword(password, server)
		if err != nil {
			return out, err
		}
		out.EncryptedPassword = ep
		return out, nil
	case 2361:
		k, err := legacyOraclePasswordKey(username, password)
		if err != nil {
			return out, err
		}
		key = k
	case 6949:
		if len(compileCaps) > 4 && compileCaps[4]&2 == 0 {
			padding = true
		}
		salt, err := hex.DecodeString(challenge.Salt)
		if err != nil {
			return out, fmt.Errorf("Oracle verifier salt: %w", err)
		}
		h := sha1.Sum(append([]byte(password), salt...))
		key = append(append([]byte(nil), h[:]...), 0, 0, 0, 0)
	default:
		return out, fmt.Errorf("unsupported Oracle password verifier type %d", challenge.VerifierType)
	}
	server, err := aesCBCDecryptHex(key, challenge.EncryptedServerSessionKey, padding)
	if err != nil {
		return out, err
	}
	if len(server) < 32 {
		return out, fmt.Errorf("Oracle server session key is too short: %d", len(server))
	}
	client := make([]byte, len(server))
	if _, err = io.ReadFull(random, client); err != nil {
		return out, err
	}
	encClient, err := aesCBCEncryptHex(key, client, padding)
	if err != nil {
		return out, err
	}
	kh, err := oracleSessionKeyHash(challenge.VerifierType, server[16:], client[16:])
	if err != nil {
		return out, err
	}
	encPassword, err := aesCBCEncryptPassword(password, kh, random)
	if err != nil {
		return out, err
	}
	out.EncryptedClientSessionKey = encClient
	out.EncryptedPassword = encPassword
	out.KeyHash = kh
	return out, nil
}

func legacyOraclePasswordKey(username, password string) ([]byte, error) {
	expand := func(s string) []byte {
		b := []byte(strings.ToUpper(s))
		out := make([]byte, len(b)*2)
		for i, v := range b {
			out[i*2+1] = v
		}
		return out
	}
	in := append(expand(username), expand(password)...)
	if rem := len(in) % 8; rem != 0 {
		in = append(in, make([]byte, 8-rem)...)
	}
	run := func(in, key []byte) ([]byte, error) {
		blk, err := des.NewCipher(key)
		if err != nil {
			return nil, err
		}
		state := make([]byte, 8)
		tmp := make([]byte, 8)
		for off := 0; off < len(in); off += 8 {
			for i := 0; i < 8; i++ {
				state[i] ^= in[off+i]
			}
			blk.Encrypt(tmp, state)
			copy(state, tmp)
		}
		return state, nil
	}
	k1, err := run(in, []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef})
	if err != nil {
		return nil, err
	}
	k2, err := run(in, k1)
	if err != nil {
		return nil, err
	}
	return append(k2, make([]byte, 8)...), nil
}
func pkcs7(b []byte, n int) []byte {
	p := n - len(b)%n
	if p == 0 {
		p = n
	}
	return append(append([]byte(nil), b...), bytes.Repeat([]byte{byte(p)}, p)...)
}
func unpadPKCS7(b []byte, n int) ([]byte, error) {
	if len(b) == 0 || len(b)%n != 0 {
		return nil, errors.New("invalid Oracle padded block")
	}
	p := int(b[len(b)-1])
	if p == 0 || p > n || p > len(b) {
		return nil, errors.New("invalid Oracle padding")
	}
	for _, v := range b[len(b)-p:] {
		if int(v) != p {
			return nil, errors.New("invalid Oracle padding")
		}
	}
	return b[:len(b)-p], nil
}
func aesCBCDecryptHex(key []byte, s string, padded bool) ([]byte, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(raw)%blk.BlockSize() != 0 {
		return nil, errors.New("Oracle encrypted session key is not block aligned")
	}
	out := make([]byte, len(raw))
	cipher.NewCBCDecrypter(blk, make([]byte, blk.BlockSize())).CryptBlocks(out, raw)
	if padded {
		return unpadPKCS7(out, blk.BlockSize())
	}
	return out, nil
}
func aesCBCEncryptHex(key, data []byte, padded bool) (string, error) {
	blk, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	if padded {
		data = pkcs7(data, blk.BlockSize())
	}
	if len(data)%blk.BlockSize() != 0 {
		return "", errors.New("Oracle session key is not block aligned")
	}
	out := make([]byte, len(data))
	cipher.NewCBCEncrypter(blk, make([]byte, blk.BlockSize())).CryptBlocks(out, data)
	return strings.ToUpper(hex.EncodeToString(out)), nil
}
func oracleSessionKeyHash(verifier int, a, b []byte) ([]byte, error) {
	switch verifier {
	case 2361:
		if len(a) < 16 || len(b) < 16 {
			return nil, errors.New("Oracle verifier 2361 session key is short")
		}
		x := make([]byte, 16)
		for i := range x {
			x[i] = a[i] ^ b[i]
		}
		h := md5.Sum(x)
		return h[:], nil
	case 6949:
		if len(a) < 24 || len(b) < 24 {
			return nil, errors.New("Oracle verifier 6949 session key is short")
		}
		x := make([]byte, 24)
		for i := range x {
			x[i] = a[i] ^ b[i]
		}
		h1 := md5.Sum(x[:16])
		h2 := md5.Sum(x[16:])
		return append(append([]byte(nil), h1[:]...), h2[:8]...), nil
	}
	return nil, fmt.Errorf("unsupported Oracle verifier type %d", verifier)
}
func aesCBCEncryptPassword(password string, key []byte, random io.Reader) (string, error) {
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(random, prefix); err != nil {
		return "", err
	}
	return aesCBCEncryptHex(key, append(prefix, []byte(password)...), true)
}
func desCBCDecryptHex(key []byte, s string) ([]byte, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	blk, err := des.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(raw)%8 != 0 {
		return nil, errors.New("Oracle legacy session key is not block aligned")
	}
	out := make([]byte, len(raw))
	cipher.NewCBCDecrypter(blk, make([]byte, 8)).CryptBlocks(out, raw)
	return out, nil
}
func desCBCEncryptPassword(password string, key []byte) (string, error) {
	blk, err := des.NewCipher(key)
	if err != nil {
		return "", err
	}
	b := []byte(password)
	pad := 0
	if len(b)%8 != 0 {
		pad = 8 - len(b)%8
		b = append(b, make([]byte, pad)...)
	}
	out := make([]byte, len(b))
	cipher.NewCBCEncrypter(blk, make([]byte, 8)).CryptBlocks(out, b)
	return strings.ToUpper(hex.EncodeToString(out) + strconv.Itoa(pad)), nil
}

type oracleAuthResult struct {
	SessionProperties map[string]string
}

func parseOracleAuthResult(payload []byte, ttcVersion byte, hasEOS, hasFSAP bool) (oracleAuthResult, error) {
	out := oracleAuthResult{SessionProperties: map[string]string{}}
	r := newTTCDecoder(payload)
	for r.remaining() > 0 {
		code, err := r.byte()
		if err != nil {
			return out, err
		}
		switch code {
		case 8:
			n, err := r.compactUint(4)
			if err != nil {
				return out, err
			}
			if n > 256 {
				return out, fmt.Errorf("Oracle auth result dictionary too large: %d", n)
			}
			for i := uint64(0); i < n; i++ {
				k, v, _, err := r.keyVal()
				if err != nil {
					return out, err
				}
				out.SessionProperties[string(k)] = string(v)
			}
		case 4:
			if hasEOS {
				if _, err = r.compactUint(4); err != nil {
					return out, err
				}
			}
			if ttcVersion >= 3 && hasFSAP {
				if _, err = r.compactUint(2); err != nil {
					return out, err
				}
			}
			if _, err = r.compactUint(4); err != nil {
				return out, err
			} // current row
			ret, err := r.compactUint(2)
			if err != nil {
				return out, err
			}
			if ret != 0 {
				return out, fmt.Errorf("Oracle authentication failed with ORA-%05d", ret)
			}
			return out, nil
		case 15:
			return out, errors.New("Oracle authentication returned an unsupported TTC warning before success")
		default:
			return out, fmt.Errorf("unexpected Oracle TTC authentication result code %d", code)
		}
	}
	return out, errors.New("Oracle authentication response ended without summary")
}

func (c *Connector) authenticateTTC(ctx context.Context, accepted *acceptedSession, proto ttcProtocolInfo, data ttcDataTypeInfo) (oracleAuthResult, error) {
	var out oracleAuthResult
	if strings.TrimSpace(c.ds.Username) == "" || c.ds.Password == "" {
		return out, errors.New("Oracle TTC authentication requires datasource username and password")
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "qmigration"
	}
	identity := oracleClientIdentity{Terminal: host, Program: "QMigration", Machine: host, PID: strconv.Itoa(os.Getpid()), OSUser: "qmigration", Charset: proto.ServerCharset, ConnectString: c.ds.Database}
	init, err := buildOracleAuthInit(c.ds.Username, identity)
	if err != nil {
		return out, err
	}
	if err = accepted.Session.WriteData(ctx, 0, init); err != nil {
		return out, fmt.Errorf("Oracle TTC auth init: %w", err)
	}
	flags, payload, err := accepted.Session.ReadData(ctx)
	if err != nil {
		return out, fmt.Errorf("Oracle TTC auth challenge: %w", err)
	}
	if flags != 0 {
		return out, fmt.Errorf("Oracle TTC auth challenge DATA flags 0x%x unsupported", flags)
	}
	challenge, err := parseOracleAuthChallenge(payload)
	if err != nil {
		return out, err
	}
	proof, err := deriveOracleAuthProof(c.ds.Username, c.ds.Password, challenge, proto.CompileTimeCaps, nil)
	if err != nil {
		return out, err
	}
	response, err := buildOracleAuthResponse(c.ds.Username, proof, identity)
	if err != nil {
		return out, err
	}
	if err = accepted.Session.WriteData(ctx, 0, response); err != nil {
		return out, fmt.Errorf("Oracle TTC auth response: %w", err)
	}
	flags, payload, err = accepted.Session.ReadData(ctx)
	if err != nil {
		return out, fmt.Errorf("Oracle TTC auth result: %w", err)
	}
	if flags != 0 {
		return out, fmt.Errorf("Oracle TTC auth result DATA flags 0x%x unsupported", flags)
	}
	hasEOS := len(proto.CompileTimeCaps) > 15 && proto.CompileTimeCaps[15]&1 != 0
	hasFSAP := len(proto.CompileTimeCaps) > 16 && proto.CompileTimeCaps[16]&1 != 0
	return parseOracleAuthResult(payload, data.TTCVersion, hasEOS, hasFSAP)
}
