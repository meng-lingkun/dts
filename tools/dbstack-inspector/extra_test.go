package main

import (
	"crypto/hmac"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPendingGrace(t *testing.T) {
	for _, tc := range []struct {
		age          time.Duration
		reason, want string
	}{{time.Minute, "ContainerCreating", "WARN"}, {10 * time.Minute, "ContainerCreating", "CRITICAL"}, {time.Minute, "ImagePullBackOff", "CRITICAL"}} {
		r := testInspector()
		p := Obj{"metadata": Obj{"name": "job", "namespace": "dbstack-dbaas", "creationTimestamp": time.Now().Add(-tc.age).Format(time.RFC3339)}, "status": Obj{"phase": "Pending", "containerStatuses": []any{Obj{"name": "job", "state": Obj{"waiting": Obj{"reason": tc.reason}}}}}}
		r.pods([]any{p})
		if r.report.Checks[0].Status != tc.want {
			t.Fatal(tc, r.report.Checks[0])
		}
	}
}

func TestMySQLAccessDeniedPacket(t *testing.T) {
	_, e := readGreeting(strings.NewReader(string([]byte{3, 0, 0, 0, 255, 106, 4})))
	var se *greetingError
	if !errors.As(e, &se) || se.code != 1130 {
		t.Fatalf("wrong server error %v", e)
	}
}
func TestTLSIsUnknown(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"code":200}`) }))
	defer srv.Close()
	r := testInspector()
	r.httpCheck(HTTPCheck{Name: "tls", URL: srv.URL + "/health", JSON: true})
	if r.report.Checks[0].Status != "UNKNOWN" {
		t.Fatal(r.report.Checks)
	}
}
func TestExpectedRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://console.example/health")
		w.WriteHeader(308)
	}))
	defer srv.Close()
	r := testInspector()
	r.httpCheck(HTTPCheck{Name: "portal", URL: srv.URL + "/health", ExpectedStatus: 308, ExpectedLocation: "https://console.example/health"})
	if r.report.Checks[0].Status != "OK" {
		t.Fatal(r.report.Checks)
	}
}
func TestServiceResolution(t *testing.T) {
	h, e := resolveServiceHTTP(HTTPCheck{URL: "http://openapi.dbstack-dbaas.svc.cluster.local/common/healthCheck"}, []any{Obj{"metadata": Obj{"name": "openapi", "namespace": "dbstack-dbaas"}, "spec": Obj{"clusterIP": "172.24.235.254"}}})
	if e != nil || h.URL != "http://172.24.235.254:80/common/healthCheck" {
		t.Fatal(h, e)
	}
}
func TestPatrolPaginationSignatureAndNoSecrets(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls++
		q := req.URL.Query()
		sig := q.Get("Signature")
		q.Del("Signature")
		if req.Method != "GET" || q.Get("Action") != "DescribeInstancePatrolInfo" || q.Get("ServiceCode") != "polardbx" || q.Get("OpenApiVersion") != "v1" {
			t.Error("incorrect protocol")
		}
		if !hmac.Equal([]byte(sig), []byte(rpcSignature(q, "test-secret"))) {
			t.Error("signature mismatch")
		}
		json.NewEncoder(w).Encode(Obj{"Code": "200", "totalPage": 2, "dbInstances": []any{Obj{"dbInstanceName": "instance-" + q.Get("PageNum"), "statusDesc": "Running", "cnNodeCount": 2}}, "sensitive": "test-secret"})
	}))
	defer srv.Close()
	t.Setenv("TEST_INSPECTOR_AK", "test-ak")
	t.Setenv("TEST_INSPECTOR_SK", "test-secret")
	r := testInspector()
	r.cfg.OpenAPI = APIConfig{Endpoint: srv.URL, Region: "cn-hangzhou", AccessKeyEnv: "TEST_INSPECTOR_AK", SecretKeyEnv: "TEST_INSPECTOR_SK", MaxPages: 3, HealthyStatusDescriptions: []string{"Running"}}
	r.openAPI()
	if calls != 2 {
		t.Fatal("wrong pagination", calls)
	}
	b, _ := json.Marshal(r.report)
	if strings.Contains(string(b), "test-secret") || strings.Contains(string(b), "test-ak") {
		t.Fatal("credential/raw response leak")
	}
	if len(r.report.Inventory["api_instances"].([]Obj)) != 2 {
		t.Fatal(r.report.Inventory)
	}
}
func TestRPCEncoding(t *testing.T) {
	if percent("a b+~") != "a%20b%2B~" {
		t.Fatal("RFC3986")
	}
	q := url.Values{"Action": {"DescribeInstancePatrolInfo"}, "PageNum": {"1"}}
	if rpcSignature(q, "x") == rpcSignature(q, "y") {
		t.Fatal("secret ignored")
	}
}
