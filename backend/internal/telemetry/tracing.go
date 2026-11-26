package telemetry

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Enable creates an OTLP/HTTP pipeline when an operator has configured a
// collector. The exporter honors the standard OTEL_EXPORTER_OTLP_* settings.
func Enable(ctx context.Context, service string) (func(), error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil, nil
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", service)), resource.WithFromEnv())
	if err != nil {
		return nil, fmt.Errorf("trace resource: %w", err)
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provider.Shutdown(shutdown); err != nil {
			log.Printf("trace shutdown: %v", err)
		}
	}, nil
}

func HTTPHandler(next http.Handler) http.Handler {
	// Query strings can contain SSO codes or user-supplied data. Do not
	// submit those URLs or WebSocket handshake protocols to the collector.
	return otelhttp.NewHandler(next, "DTS API", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.RawQuery == "" && r.URL.Path != "/api/v1/ws"
	}))
}

func HTTPTransport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base)
}
