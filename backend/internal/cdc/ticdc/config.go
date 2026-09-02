package ticdc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Endpoint describes the TiCDC control plane plus the Kafka bootstrap cluster
// used by QMigration's native TiCDC consumer. Credentials are deliberately not
// accepted in cdc_url. Kafka SASL credentials are injected at runtime through
// QMIGRATION_TIDB_KAFKA_SASL_USERNAME / QMIGRATION_TIDB_KAFKA_SASL_PASSWORD.
//
// Examples:
//
//	ticdc://cdc:8300?brokers=k1:9092,k2:9092
//	ticdcs://cdc:8300?brokers=k1:9093,k2:9093&kafka_partitions=8&kafka_tls=true&kafka_server_name=kafka.internal
//	ticdc://cdc:8300?brokers=k1:9093&kafka_tls=true&kafka_sasl_mechanism=plain
//
// kafka_ca/kafka_cert/kafka_key are filesystem paths that must be readable by
// both the QMigration TiCDC worker and TiCDC when QMigration creates the sink.
type Endpoint struct {
	ControlURL         string
	Brokers            []string
	KafkaPartitions    int
	KafkaTLS           bool
	KafkaServerName    string
	KafkaCA            string
	KafkaCert          string
	KafkaKey           string
	KafkaSASLMechanism string
}

func ParseEndpoint(raw string) (Endpoint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Endpoint{}, errors.New("TiDB source CDC requires cdc_url (ticdc://...?...brokers=...)")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, fmt.Errorf("parse TiCDC cdc_url: %w", err)
	}
	if u.User != nil {
		return Endpoint{}, errors.New("TiCDC cdc_url must not embed credentials")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "ticdc" && scheme != "ticdcs" {
		return Endpoint{}, fmt.Errorf("unsupported TiCDC cdc_url scheme %q; expected ticdc or ticdcs", u.Scheme)
	}
	if strings.TrimSpace(u.Host) == "" {
		return Endpoint{}, errors.New("TiCDC cdc_url requires control-plane host:port")
	}
	controlScheme := "http"
	if scheme == "ticdcs" {
		controlScheme = "https"
	}
	q := u.Query()
	brokersRaw := strings.TrimSpace(q.Get("brokers"))
	if brokersRaw == "" {
		return Endpoint{}, errors.New("TiCDC cdc_url requires brokers query parameter")
	}
	seen := map[string]bool{}
	brokers := []string{}
	for _, item := range strings.Split(brokersRaw, ",") {
		b := strings.TrimSpace(item)
		if b == "" || seen[b] {
			continue
		}
		host, port, e := net.SplitHostPort(b)
		if e != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
			return Endpoint{}, fmt.Errorf("invalid TiCDC Kafka broker %q; expected host:port", b)
		}
		seen[b] = true
		brokers = append(brokers, b)
	}
	if len(brokers) == 0 {
		return Endpoint{}, errors.New("TiCDC cdc_url has no valid Kafka brokers")
	}
	sort.Strings(brokers)

	partitions := 1
	if rawPartitions := strings.TrimSpace(q.Get("kafka_partitions")); rawPartitions != "" {
		v, err := strconv.Atoi(rawPartitions)
		if err != nil || v < 1 || v > 4096 {
			return Endpoint{}, fmt.Errorf("invalid kafka_partitions %q; expected 1..4096", rawPartitions)
		}
		partitions = v
	}
	kafkaTLS := false
	if rawTLS := strings.TrimSpace(q.Get("kafka_tls")); rawTLS != "" {
		v, err := strconv.ParseBool(rawTLS)
		if err != nil {
			return Endpoint{}, fmt.Errorf("invalid kafka_tls %q", rawTLS)
		}
		kafkaTLS = v
	}
	mechanism := strings.ToLower(strings.TrimSpace(q.Get("kafka_sasl_mechanism")))
	switch mechanism {
	case "", "plain", "scram-sha-256", "scram-sha-512", "oauthbearer", "gssapi":
	default:
		return Endpoint{}, fmt.Errorf("unsupported kafka_sasl_mechanism %q; supports plain, scram-sha-256/512, oauthbearer and external gssapi provider", mechanism)
	}
	kafkaCA := strings.TrimSpace(q.Get("kafka_ca"))
	kafkaCert := strings.TrimSpace(q.Get("kafka_cert"))
	kafkaKey := strings.TrimSpace(q.Get("kafka_key"))
	if (kafkaCert == "") != (kafkaKey == "") {
		return Endpoint{}, errors.New("kafka_cert and kafka_key must be configured together")
	}
	if (kafkaCA != "" || kafkaCert != "" || kafkaKey != "" || strings.TrimSpace(q.Get("kafka_server_name")) != "") && !kafkaTLS {
		return Endpoint{}, errors.New("Kafka TLS certificate/server-name settings require kafka_tls=true")
	}
	return Endpoint{
		ControlURL:         controlScheme + "://" + u.Host,
		Brokers:            brokers,
		KafkaPartitions:    partitions,
		KafkaTLS:           kafkaTLS,
		KafkaServerName:    strings.TrimSpace(q.Get("kafka_server_name")),
		KafkaCA:            kafkaCA,
		KafkaCert:          kafkaCert,
		KafkaKey:           kafkaKey,
		KafkaSASLMechanism: mechanism,
	}, nil
}

func ProbeBrokers(ep Endpoint, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	client, err := NewKafkaClientForEndpoint(ep, "qmigration-ticdc-probe")
	if err != nil {
		return err
	}
	ctx, cancel := contextWithTimeout(timeout)
	defer cancel()
	var errs []string
	for _, addr := range ep.Brokers {
		err := client.probeConnection(ctx, addr)
		if err == nil {
			return nil
		}
		errs = append(errs, addr+": "+err.Error())
	}
	return fmt.Errorf("no TiCDC Kafka broker reachable: %s", strings.Join(errs, "; "))
}

// contextWithTimeout is a tiny indirection kept here so ProbeBrokers does not
// duplicate deadline handling from the native Kafka client.
func contextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
