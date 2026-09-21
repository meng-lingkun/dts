package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testInspector() *Inspector {
	return &Inspector{cfg: defaults(), ctx: context.Background(), report: Report{Inventory: map[string]any{}}}
}
func TestGreeting(t *testing.T) {
	payload := append([]byte{10}, []byte("8.0.41\x00rest")...)
	packet := append([]byte{byte(len(payload)), 0, 0, 0}, payload...)
	v, e := readGreeting(bytes.NewReader(packet))
	if e != nil || v != "8.0.41" {
		t.Fatal(v, e)
	}
	for _, b := range [][]byte{{1, 0, 0, 0, 10}, {2, 0, 0, 0, 255, 0}, packet[:6], {0, 0, 2, 0}} {
		if _, e := readGreeting(bytes.NewReader(b)); e == nil {
			t.Fatalf("accepted malformed %v", b)
		}
	}
}
func TestHTTPRejectsFalseSuccessAndRedirect(t *testing.T) {
	for _, tc := range []struct {
		status     int
		body, want string
	}{{200, `{"Code":500}`, "CRITICAL"}, {200, `{"code":"200","successResponse":false}`, "CRITICAL"}, {200, `{"code":"200","successResponse":true}`, "OK"}, {200, `{"hello":"world"}`, "UNKNOWN"}, {200, `<html>login</html>`, "CRITICAL"}, {302, ``, "CRITICAL"}, {403, ``, "UNKNOWN"}} {
		t.Run(tc.body+tc.want, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				w.Header().Set("Location", "/login")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			r := testInspector()
			r.httpCheck(HTTPCheck{Name: "health", URL: server.URL + "/health", JSON: true})
			if got := r.report.Checks[0].Status; got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
func TestNoBusinessGETsInHealthConfig(t *testing.T) {
	for _, u := range []string{"https://example.com/?Action=DeleteDB", "http://example.com/api/v1.0/accesskeys", "http://example.com/common/healthCheck?Action=DeleteDB"} {
		if validateHTTP(HTTPCheck{URL: u}) == nil {
			t.Fatal("unsafe endpoint accepted", u)
		}
	}
	if e := validateHTTP(HTTPCheck{URL: "http://example.com/common/healthCheck"}); e != nil {
		t.Fatal(e)
	}
}
func TestMissingReplicaAndOldRestart(t *testing.T) {
	r := testInspector()
	r.cfg.Namespaces = []string{"dbstack-dbaas"}
	r.cfg.ExpectedGroups = map[string]int{"gone": 3}
	p := Obj{"metadata": Obj{"name": "p", "namespace": "dbstack-dbaas"}, "status": Obj{"phase": "Running", "conditions": []any{Obj{"type": "Ready", "status": "True"}}, "containerStatuses": []any{Obj{"name": "c", "restartCount": 1000, "lastState": Obj{"terminated": Obj{"finishedAt": time.Now().Add(-48 * time.Hour).Format(time.RFC3339)}}}}}}
	r.pods([]any{p})
	for _, c := range r.report.Checks {
		if c.Target == "gone" && c.Status != "CRITICAL" {
			t.Fatal(c)
		}
		if c.Category == "Pod" && c.Status != "OK" {
			t.Fatal("historical restarts must not alert", c)
		}
	}
	if len(r.report.Checks) != 2 {
		t.Fatal(r.report.Checks)
	}
}
func TestRunningButNotReady(t *testing.T) {
	r := testInspector()
	p := Obj{"metadata": Obj{"name": "keycloak", "namespace": "apsarabase"}, "status": Obj{"phase": "Running", "conditions": []any{Obj{"type": "Ready", "status": "False"}}}}
	r.pods([]any{p})
	if r.report.Checks[0].Status != "CRITICAL" {
		t.Fatal(r.report.Checks)
	}
}
func TestDiskAndExit(t *testing.T) {
	r := testInspector()
	r.disk("node/data", Obj{"capacityBytes": 100, "availableBytes": 9, "inodes": 100, "inodesFree": 90})
	if r.report.Checks[0].Status != "CRITICAL" {
		t.Fatal(r.report.Checks)
	}
	r.disk("missing", Obj{})
	if r.report.Checks[1].Status != "UNKNOWN" {
		t.Fatal(r.report.Checks)
	}
	if reportExit(map[string]int{"CRITICAL": 1, "UNKNOWN": 1}) != 2 || reportExit(map[string]int{"UNKNOWN": 1, "WARN": 1}) != 3 {
		t.Fatal("exit precedence")
	}
}
func TestQuantity(t *testing.T) {
	for s, want := range map[string]float64{"30": 30, "1000m": 1, "1Gi": 1073741824, "250000000n": 0.25} {
		v, ok := quantity(s)
		if !ok || v != want {
			t.Fatal(s, v, ok)
		}
	}
	if _, ok := quantity("NaN"); ok {
		t.Fatal("NaN accepted")
	}
}
func TestRPCSignature(t *testing.T) {
	q := url.Values{"Action": {"DescribeInstancePatrolInfo"}, "Space": {"a b+~"}}
	sig := rpcSignature(q, "secret")
	if sig != "RBO3siO2/VzK1zhVd8ea2f3wqRI=" {
		t.Fatalf("signature %s", sig)
	}
	if strings.Contains(q.Encode(), "Signature=") {
		t.Fatal("signature function mutated params")
	}
}
func TestOpenAPIMissingCredentials(t *testing.T) {
	r := testInspector()
	r.cfg.OpenAPI = APIConfig{Endpoint: "https://example.invalid", AccessKeyEnv: "TEST_INSPECTOR_AK", SecretKeyEnv: "TEST_INSPECTOR_SK"}
	t.Setenv("TEST_INSPECTOR_AK", "")
	t.Setenv("TEST_INSPECTOR_SK", "")
	r.openAPI()
	if r.report.Checks[0].Status != "UNKNOWN" {
		t.Fatal(r.report.Checks)
	}
}
