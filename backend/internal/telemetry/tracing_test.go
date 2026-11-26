package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	collect "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestOTLPExportsHTTPSpanAndOmitsSensitiveQueries(t *testing.T) {
	packets := make(chan *collect.ExportTraceServiceRequest, 2)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		req := new(collect.ExportTraceServiceRequest)
		if err != nil || proto.Unmarshal(body, req) != nil {
			http.Error(w, "bad OTLP", 400)
			return
		}
		packets <- req
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer receiver.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", receiver.URL+"/v1/traces")
	shutdown, err := Enable(context.Background(), "dts-qualification")
	if err != nil {
		t.Fatal(err)
	}
	h := HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	for _, url := range []string{"/readyz", "/api/v1/auth/oidc/callback?code=test-secret"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	}
	shutdown()
	select {
	case req := <-packets:
		n := 0
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				n += len(ss.Spans)
			}
		}
		if n != 1 {
			t.Fatalf("exported %d spans, want one request without a query", n)
		}
	default:
		t.Fatal("no OTLP trace received")
	}
}
