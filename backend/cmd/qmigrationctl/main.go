package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"qmigration/backend/internal/validationreport"
)

type client struct {
	base  string
	token string
	http  *http.Client
}

func (c client) request(method, path string, body []byte) ([]byte, error) {
	url := strings.TrimRight(c.base, "/") + path
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("X-QMigration-API-Token", c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func printJSON(data []byte) error {
	if len(bytes.TrimSpace(data)) == 0 {
		fmt.Println("ok")
		return nil
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Println(string(data))
		return nil
	}
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(out))
	return nil
}

func readFile(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func usage() {
	fmt.Fprintln(os.Stderr, `qmigrationctl [--server URL] [--token TOKEN] COMMAND [args]

Commands:
  health                         server health
  datasources                    list datasources
  create-datasource FILE|-       create datasource from JSON
  migrations                     list migration tasks
  migration ID                   show one migration
  create-migration FILE|-        create migration from JSON
  start|pause|resume|cancel ID   control migration
  validate ID                    start validation
  ready-cutover ID               require CDC lag <= 5s and prepare cutover
  cutover ID                     execute cutover
  rollback-prepare ID            start reverse-sync preparation
  rollback-ready ID              require reverse CDC lag <= 5s
  rollback ID                    execute rollback
  logs ID                        task logs
  cdc ID                         CDC positions
  workers                        list workers
  alerts                         list alerts
  engines                        list engine capabilities
  validation-public-key          download Ed25519 report verification public key
  validation-key-transition FILE request transition certificate to new public key
  validation-key-revocation FILE request revocation certificate for target public key
  trust-init [flags]             initialize local report trust store
  trust-apply-transition [flags] apply signed key-transition certificate
  trust-apply-revocation [flags] apply signed key-revocation certificate
  trust-show --store FILE        show local trust store
  verify-report [flags] DIR      offline verify report directory
    --public-key FILE            pin one externally trusted public key
    --trust-store FILE           verify through rotated/revoked local trust store

Environment: QMIGRATION_SERVER, QMIGRATION_API_TOKEN`)
}

func main() {
	fs := flag.NewFlagSet("qmigrationctl", flag.ContinueOnError)
	server := fs.String("server", env("QMIGRATION_SERVER", "http://127.0.0.1:8080"), "QMigration server")
	token := fs.String("token", env("QMIGRATION_API_TOKEN", ""), "API token")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	args := fs.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	c := client{base: *server, token: *token, http: &http.Client{Timeout: 60 * time.Second}}
	cmd := args[0]
	if cmd == "verify-report" {
		vf := flag.NewFlagSet("verify-report", flag.ContinueOnError)
		publicKey := vf.String("public-key", "", "trusted Ed25519 public key JSON/PEM/base64 file")
		trustStorePath := vf.String("trust-store", "", "rotating/revoking QMigration report trust store")
		tsaCA := vf.String("tsa-ca", "", "trusted RFC3161 TSA CA certificate file")
		if err := vf.Parse(args[1:]); err != nil {
			os.Exit(2)
		}
		if vf.NArg() != 1 || (*publicKey != "" && *trustStorePath != "") {
			fmt.Fprintln(os.Stderr, "usage: qmigrationctl verify-report [--public-key FILE | --trust-store FILE] [--tsa-ca FILE] DIR")
			os.Exit(2)
		}
		var result validationreport.VerificationResult
		var err error
		if *trustStorePath != "" {
			store, loadErr := validationreport.LoadTrustStore(*trustStorePath)
			if loadErr != nil {
				fmt.Fprintln(os.Stderr, loadErr)
				os.Exit(1)
			}
			result, err = validationreport.VerifyReportDirectoryWithTrustStore(vf.Arg(0), store)
		} else {
			trusted, loadErr := validationreport.LoadTrustedPublicKeyFile(*publicKey)
			if loadErr != nil {
				fmt.Fprintln(os.Stderr, loadErr)
				os.Exit(1)
			}
			result, err = validationreport.VerifyReportDirectory(vf.Arg(0), trusted)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "verification failed:", err)
			os.Exit(1)
		}
		if *tsaCA != "" {
			proof, e := validationreport.VerifyTimestampDirectory(context.Background(), vf.Arg(0), *tsaCA)
			if e != nil {
				fmt.Fprintln(os.Stderr, "timestamp verification failed:", e)
				os.Exit(1)
			}
			result.TimestampVerified = true
			result.TimestampPolicyOID = proof.PolicyOID
			result.TimestampSerial = proof.SerialNumber
			result.TimestampGenTime = proof.GenTime
		}
		b, _ := json.Marshal(result)
		_ = printJSON(b)
		return
	}
	if cmd == "trust-init" {
		vf := flag.NewFlagSet("trust-init", flag.ContinueOnError)
		storePath := vf.String("store", "", "trust store path")
		publicKey := vf.String("public-key", "", "initial trusted public key JSON/PEM/base64 file")
		if err := vf.Parse(args[1:]); err != nil {
			os.Exit(2)
		}
		if *storePath == "" || *publicKey == "" || vf.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "usage: qmigrationctl trust-init --store FILE --public-key FILE")
			os.Exit(2)
		}
		doc, err := validationreport.LoadPublicKeyDocumentFile(*publicKey)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		store, err := validationreport.NewTrustStore(doc, time.Now().UTC())
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := validationreport.SaveTrustStore(*storePath, store); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		b, _ := json.Marshal(store)
		_ = printJSON(b)
		return
	}
	if cmd == "trust-apply-transition" {
		vf := flag.NewFlagSet("trust-apply-transition", flag.ContinueOnError)
		storePath := vf.String("store", "", "trust store path")
		certPath := vf.String("certificate", "", "signed transition certificate JSON")
		if err := vf.Parse(args[1:]); err != nil {
			os.Exit(2)
		}
		if *storePath == "" || *certPath == "" || vf.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "usage: qmigrationctl trust-apply-transition --store FILE --certificate FILE")
			os.Exit(2)
		}
		store, err := validationreport.LoadTrustStore(*storePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		b, err := os.ReadFile(*certPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		cert, err := validationreport.ParseKeyTransitionCertificate(b)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := store.ApplyTransition(cert); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := validationreport.SaveTrustStore(*storePath, store); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		out, _ := json.Marshal(store)
		_ = printJSON(out)
		return
	}
	if cmd == "trust-apply-revocation" {
		vf := flag.NewFlagSet("trust-apply-revocation", flag.ContinueOnError)
		storePath := vf.String("store", "", "trust store path")
		certPath := vf.String("certificate", "", "signed revocation certificate JSON")
		if err := vf.Parse(args[1:]); err != nil {
			os.Exit(2)
		}
		if *storePath == "" || *certPath == "" || vf.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "usage: qmigrationctl trust-apply-revocation --store FILE --certificate FILE")
			os.Exit(2)
		}
		store, err := validationreport.LoadTrustStore(*storePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		b, err := os.ReadFile(*certPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		cert, err := validationreport.ParseKeyRevocationCertificate(b)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := store.ApplyRevocation(cert); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := validationreport.SaveTrustStore(*storePath, store); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		out, _ := json.Marshal(store)
		_ = printJSON(out)
		return
	}
	if cmd == "trust-show" {
		vf := flag.NewFlagSet("trust-show", flag.ContinueOnError)
		storePath := vf.String("store", "", "trust store path")
		if err := vf.Parse(args[1:]); err != nil {
			os.Exit(2)
		}
		if *storePath == "" || vf.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "usage: qmigrationctl trust-show --store FILE")
			os.Exit(2)
		}
		store, err := validationreport.LoadTrustStore(*storePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		out, _ := json.Marshal(store)
		_ = printJSON(out)
		return
	}
	needID := func() string {
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		return args[1]
	}
	var method, path string
	var body []byte
	var err error
	switch cmd {
	case "health":
		method, path = http.MethodGet, "/healthz"
	case "datasources":
		method, path = http.MethodGet, "/api/v1/datasources"
	case "migrations":
		method, path = http.MethodGet, "/api/v1/migrations"
	case "migration":
		method, path = http.MethodGet, "/api/v1/migrations/"+needID()
	case "workers":
		method, path = http.MethodGet, "/api/v1/workers"
	case "alerts":
		method, path = http.MethodGet, "/api/v1/alerts"
	case "engines":
		method, path = http.MethodGet, "/api/v1/engines"
	case "validation-public-key":
		method, path = http.MethodGet, "/api/v1/validation-report/public-key"
	case "validation-key-transition":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		doc, loadErr := validationreport.LoadPublicKeyDocumentFile(args[1])
		if loadErr != nil {
			err = loadErr
			break
		}
		body, _ = json.Marshal(map[string]any{"new_public_key": doc, "reason": "operator-requested key rotation"})
		method, path = http.MethodPost, "/api/v1/validation-report/key-transition"
	case "validation-key-revocation":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		doc, loadErr := validationreport.LoadPublicKeyDocumentFile(args[1])
		if loadErr != nil {
			err = loadErr
			break
		}
		body, _ = json.Marshal(map[string]any{"target_public_key": doc, "reason": "operator-requested revocation"})
		method, path = http.MethodPost, "/api/v1/validation-report/key-revocation"
	case "logs":
		method, path = http.MethodGet, "/api/v1/migrations/"+needID()+"/logs?limit=500"
	case "cdc":
		method, path = http.MethodGet, "/api/v1/migrations/"+needID()+"/cdc"
	case "create-datasource":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		body, err = readFile(args[1])
		method, path = http.MethodPost, "/api/v1/datasources"
	case "create-migration":
		if len(args) < 2 {
			usage()
			os.Exit(2)
		}
		body, err = readFile(args[1])
		method, path = http.MethodPost, "/api/v1/migrations"
	case "start", "pause", "resume", "cancel", "validate", "cutover":
		method, path = http.MethodPost, "/api/v1/migrations/"+needID()+"/"+cmd
	case "ready-cutover":
		method, path = http.MethodPost, "/api/v1/migrations/"+needID()+"/ready-cutover"
		body = []byte(`{"max_lag_ms":5000}`)
	case "rollback-prepare":
		method, path = http.MethodPost, "/api/v1/migrations/"+needID()+"/rollback/prepare"
	case "rollback-ready":
		method, path = http.MethodPost, "/api/v1/migrations/"+needID()+"/rollback/ready"
		body = []byte(`{"max_lag_ms":5000}`)
	case "rollback":
		method, path = http.MethodPost, "/api/v1/migrations/"+needID()+"/rollback"
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	data, err := c.request(method, path, body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = printJSON(data)
}

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
