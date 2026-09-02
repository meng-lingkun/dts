package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"qmigration/backend/internal/version"
)

type evidenceFlags []string

func (e *evidenceFlags) String() string     { return strings.Join(*e, ",") }
func (e *evidenceFlags) Set(v string) error { *e = append(*e, v); return nil }

type evidence struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Path   string `json:"path,omitempty"`
	Status string `json:"status"`
	SHA256 string `json:"sha256,omitempty"`
	Bytes  int64  `json:"bytes,omitempty"`
	Note   string `json:"note,omitempty"`
}
type manifest struct {
	Product             string     `json:"product"`
	Version             string     `json:"version"`
	GeneratedAtUTC      string     `json:"generated_at_utc"`
	SoftwareComplete    bool       `json:"software_complete"`
	ProductionQualified bool       `json:"production_qualified"`
	Evidence            []evidence `json:"evidence"`
	Boundary            string     `json:"boundary"`
}

func parseSpec(raw string) (string, string, error) {
	p := strings.SplitN(raw, "=", 2)
	if len(p) != 2 || strings.TrimSpace(p[0]) == "" || strings.TrimSpace(p[1]) == "" {
		return "", "", fmt.Errorf("evidence must be name=path, got %q", raw)
	}
	return strings.TrimSpace(p[0]), strings.TrimSpace(p[1]), nil
}
func inferJSONStatus(b []byte) string {
	var v map[string]any
	if json.Unmarshal(b, &v) != nil {
		return "RETAINED"
	}
	if s, ok := v["status"].(string); ok {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			return s
		}
	}
	if x, ok := v["verified"].(bool); ok {
		if x {
			return "PASS"
		}
		return "FAIL"
	}
	if r, ok := v["results"].(map[string]any); ok && len(r) > 0 {
		all := true
		for _, x := range r {
			s, ok := x.(string)
			if !ok || strings.ToUpper(strings.TrimSpace(s)) != "PASS" {
				all = false
				break
			}
		}
		if all {
			return "PASS"
		}
	}
	return "RETAINED"
}
func load(kind, name, path string) evidence {
	e := evidence{Name: name, Kind: kind, Path: path}
	b, err := os.ReadFile(path)
	if err != nil {
		if kind == "software" {
			e.Status = "FAIL"
		} else {
			e.Status = "NOT_QUALIFIED"
		}
		e.Note = err.Error()
		return e
	}
	sum := sha256.Sum256(b)
	e.SHA256 = hex.EncodeToString(sum[:])
	e.Bytes = int64(len(b))
	e.Status = inferJSONStatus(b)
	if kind == "external" && e.Status == "RETAINED" {
		e.Status = "NOT_QUALIFIED"
		e.Note = "evidence exists but does not declare a machine-verifiable PASS"
	}
	return e
}
func main() {
	var software, external evidenceFlags
	out := flag.String("output", "", "output JSON path (stdout when empty)")
	flag.Var(&software, "software", "required software evidence name=path; repeatable")
	flag.Var(&external, "external", "production qualification evidence name=path; repeatable")
	flag.Parse()
	m := manifest{Product: "QMigration", Version: version.Version, GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), SoftwareComplete: true, ProductionQualified: len(external) > 0, Boundary: "Software completeness and production qualification are separate. Missing real vendor/HSM/TSA/WORM/large-soak evidence never becomes PASS by implication."}
	for _, raw := range software {
		n, p, err := parseSpec(raw)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		e := load("software", n, p)
		if e.Status != "PASS" {
			m.SoftwareComplete = false
		}
		m.Evidence = append(m.Evidence, e)
	}
	for _, raw := range external {
		n, p, err := parseSpec(raw)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		e := load("external", n, p)
		if e.Status != "PASS" {
			m.ProductionQualified = false
		}
		m.Evidence = append(m.Evidence, e)
	}
	sort.Slice(m.Evidence, func(i, j int) bool {
		if m.Evidence[i].Kind == m.Evidence[j].Kind {
			return m.Evidence[i].Name < m.Evidence[j].Name
		}
		return m.Evidence[i].Kind < m.Evidence[j].Kind
	})
	b, _ := json.MarshalIndent(m, "", "  ")
	b = append(b, '\n')
	if strings.TrimSpace(*out) == "" {
		_, _ = os.Stdout.Write(b)
		return
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !m.SoftwareComplete {
		os.Exit(3)
	}
}
