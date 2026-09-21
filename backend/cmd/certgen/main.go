package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var output, address string
	flag.StringVar(&output, "output", "", "registry PKI output directory")
	flag.StringVar(&address, "ip", "", "registry IPv4 address")
	flag.Parse()
	if err := generate(output, address, time.Now()); err != nil {
		fmt.Fprintln(os.Stderr, "dts-certgen:", err)
		os.Exit(1)
	}
}

func generate(output, address string, now time.Time) error {
	if output == "" {
		return errors.New("--output is required")
	}
	ip := net.ParseIP(address)
	if ip == nil || ip.To4() == nil {
		return errors.New("--ip must be an IPv4 address")
	}
	if err := os.MkdirAll(filepath.Join(output, "client"), 0o700); err != nil {
		return err
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serialNumber(),
		Subject:               pkix.Name{CommonName: "DTS Registry CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		SignatureAlgorithm:    x509.SHA256WithRSA,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create CA certificate: %w", err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("parse generated CA certificate: %w", err)
	}

	serverKey, serverDER, err := leafCertificate(caCertificate, caKey, pkix.Name{CommonName: address}, []net.IP{ip.To4()}, x509.ExtKeyUsageServerAuth, now)
	if err != nil {
		return fmt.Errorf("create server certificate: %w", err)
	}
	clientKey, clientDER, err := leafCertificate(caCertificate, caKey, pkix.Name{CommonName: "DTS Registry Node Client"}, nil, x509.ExtKeyUsageClientAuth, now)
	if err != nil {
		return fmt.Errorf("create client certificate: %w", err)
	}
	if err := verifyChain(caCertificate, serverDER, clientDER, address, now); err != nil {
		return err
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate registry HTTP secret: %w", err)
	}
	files := map[string][]byte{
		"ca.crt":            pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		"ca.key":            pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)}),
		"server.crt":        pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		"server.key":        pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)}),
		"client/client.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		"client/client.key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)}),
		"client/ca.crt":     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		"client/endpoint":   []byte(address + ":5000\n"),
		"http.secret":       []byte(hex.EncodeToString(secret) + "\n"),
	}
	for name, contents := range files {
		if err := writeAtomic(filepath.Join(output, name), contents, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	return nil
}

func serialNumber() *big.Int {
	value := make([]byte, 20)
	if _, err := rand.Read(value); err != nil {
		panic(err)
	}
	value[0] &= 0x7f
	value[0] |= 0x01
	return new(big.Int).SetBytes(value)
}

func leafCertificate(ca *x509.Certificate, caKey *rsa.PrivateKey, subject pkix.Name, ips []net.IP, usage x509.ExtKeyUsage, now time.Time) (*rsa.PrivateKey, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber:       serialNumber(),
		Subject:            subject,
		NotBefore:          now.Add(-5 * time.Minute),
		NotAfter:           now.AddDate(0, 0, 825),
		KeyUsage:           x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:        []x509.ExtKeyUsage{usage},
		IPAddresses:        ips,
		SignatureAlgorithm: x509.SHA256WithRSA,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	return key, der, err
}

func verifyChain(ca *x509.Certificate, serverDER, clientDER []byte, address string, now time.Time) error {
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	server, err := x509.ParseCertificate(serverDER)
	if err != nil {
		return fmt.Errorf("parse generated server certificate: %w", err)
	}
	if _, err := server.Verify(x509.VerifyOptions{Roots: roots, DNSName: address, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, CurrentTime: now}); err != nil {
		return fmt.Errorf("verify generated server certificate: %w", err)
	}
	client, err := x509.ParseCertificate(clientDER)
	if err != nil {
		return fmt.Errorf("parse generated client certificate: %w", err)
	}
	if _, err := client.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: now}); err != nil {
		return fmt.Errorf("verify generated client certificate: %w", err)
	}
	return nil
}

func writeAtomic(path string, contents []byte, mode os.FileMode) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, contents, mode); err != nil {
		return err
	}
	if err := os.Chmod(temporary, mode); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}
