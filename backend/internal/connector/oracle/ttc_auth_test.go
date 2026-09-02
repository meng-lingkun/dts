package oracleconnector

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"testing"
)

func TestTTCCodecKeyValRoundTrip(t *testing.T) {
	w := &ttcEncoder{}
	w.keyVal([]byte("AUTH_SESSKEY"), []byte("ABCDEF"), 1)
	r := newTTCDecoder(w.Bytes())
	k, v, f, err := r.keyVal()
	if err != nil {
		t.Fatal(err)
	}
	if string(k) != "AUTH_SESSKEY" || string(v) != "ABCDEF" || f != 1 {
		t.Fatalf("%q %q %d", k, v, f)
	}
}

func TestParseOracleAuthChallenge(t *testing.T) {
	w := &ttcEncoder{}
	w.byte(8)
	w.compactUint(2, 4)
	w.keyVal([]byte("AUTH_SESSKEY"), []byte("AABB"), 1)
	w.keyVal([]byte("AUTH_VFR_DATA"), []byte("01020304"), 6949)
	c, err := parseOracleAuthChallenge(w.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if c.EncryptedServerSessionKey != "AABB" || c.Salt != "01020304" || c.VerifierType != 6949 {
		t.Fatalf("challenge=%+v", c)
	}
}

func TestDeriveOracle6949AuthProofDeterministic(t *testing.T) {
	password := "Secret123!"
	saltHex := "00112233445566778899AABBCCDDEEFF"
	salt, _ := hex.DecodeString(saltHex)
	h := sha1.Sum(append([]byte(password), salt...))
	key := append(append([]byte(nil), h[:]...), 0, 0, 0, 0)
	serverKey := bytes.Repeat([]byte{0x5a}, 48)
	encrypted, err := aesCBCEncryptHex(key, serverKey, false)
	if err != nil {
		t.Fatal(err)
	}
	random := bytes.NewReader(bytes.Repeat([]byte{0x33}, 128))
	proof, err := deriveOracleAuthProof("SCOTT", password, oracleAuthChallenge{EncryptedServerSessionKey: encrypted, Salt: saltHex, VerifierType: 6949}, []byte{0, 0, 0, 0, 2}, random)
	if err != nil {
		t.Fatal(err)
	}
	if proof.EncryptedClientSessionKey == "" || proof.EncryptedPassword == "" || len(proof.KeyHash) != 24 {
		t.Fatalf("proof=%+v", proof)
	}
	if strings.Contains(proof.EncryptedPassword, password) {
		t.Fatal("encrypted password contains plaintext")
	}
	client, err := aesCBCDecryptHex(key, proof.EncryptedClientSessionKey, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(client, bytes.Repeat([]byte{0x33}, 48)) {
		t.Fatalf("client key=%x", client)
	}
}

func TestBuildOracleAuthMessagesDoNotContainPassword(t *testing.T) {
	init, err := buildOracleAuthInit("scott", oracleClientIdentity{Terminal: "tty", Program: "QMigration", Machine: "host", PID: "123", OSUser: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(init, []byte("scott")) {
		t.Fatal("auth init missing user")
	}
	resp, err := buildOracleAuthResponse("scott", oracleAuthProof{EncryptedClientSessionKey: "AA11", EncryptedPassword: "BB22"}, oracleClientIdentity{Program: "QMigration", Machine: "host", PID: "123", OSUser: "svc", ConnectString: "ORCL", Charset: 873})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("AUTH_PASSWORD")) || bytes.Contains(resp, []byte("secret")) {
		t.Fatalf("bad auth response")
	}
}
