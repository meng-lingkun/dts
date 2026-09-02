package ticdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"qmigration/backend/internal/domain"
)

func TestPositionRoundTrip(t *testing.T) {
	p, err := ParsePosition("tso=429918007904436226;kafka=19")
	if err != nil || p.TSO != 429918007904436226 || p.Offset != 19 {
		t.Fatalf("position=%+v err=%v", p, err)
	}
	if p.String() != "tso=429918007904436226;kafka=19" {
		t.Fatalf("roundtrip=%s", p.String())
	}
	if _, err := ParsePosition("tso=1;kafka=-1"); err == nil {
		t.Fatal("negative offset accepted")
	}
	bare, err := ParsePosition("123")
	if err != nil || bare.TSO != 123 || bare.Offset != 0 {
		t.Fatalf("bare=%+v err=%v", bare, err)
	}
}

func TestParseEndpoint(t *testing.T) {
	ep, err := ParseEndpoint("ticdc://cdc.internal:8300?brokers=k2:9092,k1:9092")
	if err != nil {
		t.Fatal(err)
	}
	if ep.ControlURL != "http://cdc.internal:8300" || strings.Join(ep.Brokers, ",") != "k1:9092,k2:9092" {
		t.Fatalf("endpoint=%+v", ep)
	}
	for _, bad := range []string{"", "http://x?brokers=k:1", "ticdc://u:p@x:8300?brokers=k:1", "ticdc://x:8300"} {
		if _, err := ParseEndpoint(bad); err == nil {
			t.Fatalf("accepted bad endpoint %q", bad)
		}
	}
}

func TestDecodeCanalJSON(t *testing.T) {
	insert := []byte(`{"database":"app","table":"t","isDdl":false,"type":"INSERT","es":1000,"mysqlType":{"id":"bigint","b":"blob"},"data":[{"id":"7","b":"\u0000ÿ"}],"_tidb":{"commitTs":262144000}}`)
	events, tso, err := DecodeCanalJSON(insert, map[string]bool{"app.t": true})
	if err != nil {
		t.Fatal(err)
	}
	if tso != 262144000 || len(events) != 1 || events[0].Operation != domain.CDCInsert {
		t.Fatalf("events=%+v tso=%d", events, tso)
	}
	if len(events[0].After) != 2 || events[0].After[0].Column != "b" || events[0].After[0].Encoding != "base64" || events[0].After[0].Value != "AP8=" {
		t.Fatalf("binary field=%+v", events[0].After)
	}

	update := []byte(`{"database":"app","table":"t","isDdl":false,"type":"UPDATE","mysqlType":{"id":"int","name":"varchar"},"data":[{"id":"1","name":"new"}],"old":[{"name":"old"}],"_tidb":{"commitTs":300}}`)
	events, _, err = DecodeCanalJSON(update, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events[0].Before) != 2 || events[0].Before[1].Value != "old" || events[0].After[1].Value != "new" {
		t.Fatalf("update=%+v", events[0])
	}

	ddl := []byte(`{"database":"app","table":"","isDdl":true,"type":"QUERY","sql":"ALTER TABLE t ADD c INT","_tidb":{"commitTs":400}}`)
	events, _, err = DecodeCanalJSON(ddl, nil)
	if err != nil || events[0].Operation != domain.CDCDDL {
		t.Fatalf("ddl=%+v err=%v", events, err)
	}
	wm := []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":500}}`)
	events, tso, err = DecodeCanalJSON(wm, nil)
	if err != nil || tso != 500 || events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("watermark=%+v tso=%d err=%v", events, tso, err)
	}
}

func TestControlEnsureChangefeed(t *testing.T) {
	var created map[string]any
	state := "missing"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v2/health":
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{}`))
		case r.URL.Path == "/api/v2/changefeeds/cf" && r.Method == http.MethodGet:
			if state == "missing" {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "cf", "state": "normal", "start_ts": uint64(100), "sink_uri": buildKafkaSinkURI([]string{"k:9092"}, "topic")})
		case r.URL.Path == "/api/v2/changefeeds/cf" && r.Method == http.MethodDelete:
			state = "missing"
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v2/changefeeds" && r.Method == http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
				t.Errorf("decode create: %v", err)
			}
			state = "normal"
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "cf"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	c := NewControlClient(Endpoint{ControlURL: ts.URL, Brokers: []string{"k:9092"}}, ts.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.EnsureChangefeed(ctx, ChangefeedPlan{ID: "cf", Topic: "topic", StartTS: 100, Tables: []string{"app.t"}}); err != nil {
		t.Fatal(err)
	}
	if created["changefeed_id"] != "cf" || created["start_ts"].(float64) != 100 {
		t.Fatalf("create=%+v", created)
	}
	sink := created["sink_uri"].(string)
	if !strings.Contains(sink, "protocol=canal-json") || !strings.Contains(sink, "partition-num=1") || !strings.Contains(sink, "enable-tidb-extension=true") {
		t.Fatalf("sink=%s", sink)
	}
	// Reuse must not create a second changefeed.
	created = nil
	if err := c.EnsureChangefeed(ctx, ChangefeedPlan{ID: "cf", Topic: "topic", StartTS: 101, Tables: []string{"app.t"}}); err != nil {
		t.Fatal(err)
	}
	if created != nil {
		t.Fatalf("unexpected recreate %+v", created)
	}
	if err := c.DeleteChangefeed(ctx, "cf"); err != nil {
		t.Fatal(err)
	}
	if cf, _, err := c.GetChangefeed(ctx, "cf"); err != nil || cf != nil {
		t.Fatalf("delete did not remove changefeed: cf=%+v err=%v", cf, err)
	}
}

func TestKafkaLegacyRecordSet(t *testing.T) {
	msgBody := &bytes.Buffer{}
	msgBody.WriteByte(1)
	msgBody.WriteByte(0)
	_ = binary.Write(msgBody, binary.BigEndian, int64(123))
	_ = binary.Write(msgBody, binary.BigEndian, int32(-1))
	_ = binary.Write(msgBody, binary.BigEndian, int32(5))
	msgBody.WriteString("hello")
	msg := &bytes.Buffer{}
	_ = binary.Write(msg, binary.BigEndian, crc32.ChecksumIEEE(msgBody.Bytes()))
	msg.Write(msgBody.Bytes())
	set := &bytes.Buffer{}
	_ = binary.Write(set, binary.BigEndian, int64(9))
	_ = binary.Write(set, binary.BigEndian, int32(msg.Len()))
	set.Write(msg.Bytes())
	recs, err := parseRecordSet(set.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Offset != 9 || string(recs[0].Value) != "hello" {
		t.Fatalf("records=%+v", recs)
	}
	msg.Bytes()[5] = 1 // compression attr, CRC now invalid first; rebuild CRC to test codec rejection
}

func TestParseMetadataAndFetchV0(t *testing.T) {
	m := &kbuf{}
	m.i32(1)
	m.i32(1)
	m.str("leader")
	m.i32(9092)
	m.i32(1)
	m.i16(0)
	m.str("topic")
	m.i32(1)
	m.i16(0)
	m.i32(0)
	m.i32(1)
	m.i32(1)
	m.i32(1)
	m.i32(1)
	m.i32(1)
	meta, err := parseMetadataV0(m.Bytes(), "topic")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Leader != "leader:9092" || meta.Count != 1 {
		t.Fatalf("meta=%+v", meta)
	}
	f := &kbuf{}
	f.i32(1)
	f.str("topic")
	f.i32(1)
	f.i32(0)
	f.i16(0)
	f.i64(10)
	f.i32(0)
	recs, hwm, err := parseFetchV0(f.Bytes(), "topic")
	if err != nil || len(recs) != 0 || hwm != 10 {
		t.Fatalf("recs=%+v hwm=%d err=%v", recs, hwm, err)
	}
}

type fakeKafka struct{ records []KafkaRecord }

func (f *fakeKafka) Fetch(_ context.Context, _ string, offset int64, _ int32) ([]KafkaRecord, int64, error) {
	out := []KafkaRecord{}
	for _, r := range f.records {
		if r.Offset >= offset {
			out = append(out, r)
		}
	}
	return out, int64(len(f.records)), nil
}
func TestReaderAdvancesOnlyAfterAcknowledge(t *testing.T) {
	f := &fakeKafka{records: []KafkaRecord{
		{Offset: 3, Value: []byte(`{"database":"app","table":"t","isDdl":false,"type":"INSERT","mysqlType":{"id":"int"},"data":[{"id":"1"}],"_tidb":{"commitTs":900}}`)},
		{Offset: 4, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":901}}`)},
	}}
	r, err := NewReader(f, "topic", "cf", Position{TSO: 800, Offset: 3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Acknowledged(); got.Offset != 3 {
		t.Fatalf("advanced before ack: %+v", got)
	}
	if tx.Checkpoint.PositionValue != "tso=900;kafka=4" {
		t.Fatalf("checkpoint=%s", tx.Checkpoint.PositionValue)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if got := r.Acknowledged(); got.Offset != 4 || got.TSO != 900 {
		t.Fatalf("acked=%+v", got)
	}
}

func TestReaderGroupsSameCommitTSIntoOneTransaction(t *testing.T) {
	f := &fakeKafka{records: []KafkaRecord{
		{Offset: 10, Value: []byte(`{"database":"app","table":"orders","isDdl":false,"type":"INSERT","mysqlType":{"id":"bigint"},"data":[{"id":"1"}],"_tidb":{"commitTs":1000}}`)},
		{Offset: 11, Value: []byte(`{"database":"app","table":"items","isDdl":false,"type":"INSERT","mysqlType":{"id":"bigint"},"data":[{"id":"2"}],"_tidb":{"commitTs":1000}}`)},
		{Offset: 12, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":1001}}`)},
	}}
	r, err := NewReader(f, "topic", "cf", Position{TSO: 999, Offset: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 {
		t.Fatalf("expected two DML events in one transaction, got %+v", tx.Events)
	}
	if tx.Events[0].SourceTable != "orders" || tx.Events[1].SourceTable != "items" {
		t.Fatalf("unexpected grouped tables: %+v", tx.Events)
	}
	if tx.Checkpoint.PositionValue != "tso=1000;kafka=12" {
		t.Fatalf("checkpoint=%s", tx.Checkpoint.PositionValue)
	}
	if !strings.Contains(tx.Label, "offsets=10-11") {
		t.Fatalf("label=%s", tx.Label)
	}
	if got := r.Acknowledged(); got.Offset != 10 || got.TSO != 999 {
		t.Fatalf("advanced before ack: %+v", got)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if got := r.Acknowledged(); got.Offset != 12 || got.TSO != 1000 {
		t.Fatalf("acked=%+v", got)
	}
}

func TestReaderSuppressesAlreadyDurableTransactionAfterRestart(t *testing.T) {
	f := &fakeKafka{records: []KafkaRecord{
		{Offset: 20, Value: []byte(`{"database":"app","table":"t","isDdl":false,"type":"INSERT","mysqlType":{"id":"int"},"data":[{"id":"1"}],"_tidb":{"commitTs":2000}}`)},
		{Offset: 21, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":2001}}`)},
	}}
	// Simulate a restart where QMigration already durably applied TSO 2000 but
	// Kafka resumes at the last stored offset. The replayed row must become only
	// a checkpoint transaction, never a second DML apply.
	r, err := NewReader(f, "topic", "cf", Position{TSO: 2000, Offset: 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("durable duplicate was re-emitted as DML: %+v", tx.Events)
	}
	if tx.Checkpoint.PositionValue != "tso=2000;kafka=21" {
		t.Fatalf("duplicate checkpoint=%s", tx.Checkpoint.PositionValue)
	}
	if err := r.Acknowledge(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if got := r.Acknowledged(); got.Offset != 21 || got.TSO != 2000 {
		t.Fatalf("acked duplicate=%+v", got)
	}
}

func TestValidateTransactionBoundsChecksFirstRecord(t *testing.T) {
	if err := validateTransactionBounds(7, ticdcMaxTransactionEvents+1, 1); err == nil {
		t.Fatal("event bound accepted")
	}
	if err := validateTransactionBounds(7, 1, ticdcMaxTransactionBytes+1); err == nil {
		t.Fatal("byte bound accepted")
	}
	if err := validateTransactionBounds(7, ticdcMaxTransactionEvents, ticdcMaxTransactionBytes); err != nil {
		t.Fatalf("exact bound rejected: %v", err)
	}
}

func TestMultiPartitionPositionRoundTrip(t *testing.T) {
	p, err := ParsePosition("tso=1000;kafka=0:12,1:7,3:99")
	if err != nil {
		t.Fatal(err)
	}
	if p.TSO != 1000 || p.PartitionOffset(0) != 12 || p.PartitionOffset(1) != 7 || p.PartitionOffset(3) != 99 {
		t.Fatalf("position=%+v", p)
	}
	if got := p.String(); got != "tso=1000;kafka=0:12,1:7,3:99" {
		t.Fatalf("roundtrip=%s", got)
	}
	if _, err := ParsePosition("tso=1000;kafka=0:1,0:2"); err == nil {
		t.Fatal("duplicate partition cursor accepted")
	}
}

func TestParseEndpointKafkaProductionOptions(t *testing.T) {
	ep, err := ParseEndpoint("ticdc://cdc:8300?brokers=k2:9093,k1:9093&kafka_partitions=4&kafka_tls=true&kafka_server_name=kafka.example&kafka_ca=/etc/kafka/ca.pem&kafka_sasl_mechanism=plain")
	if err != nil {
		t.Fatal(err)
	}
	if ep.KafkaPartitions != 4 || !ep.KafkaTLS || ep.KafkaServerName != "kafka.example" || ep.KafkaSASLMechanism != "plain" || ep.KafkaCA != "/etc/kafka/ca.pem" {
		t.Fatalf("endpoint=%+v", ep)
	}
	if _, err := ParseEndpoint("ticdc://cdc:8300?brokers=k:9092&kafka_partitions=0"); err == nil {
		t.Fatal("invalid partition count accepted")
	}
	if _, err := ParseEndpoint("ticdc://cdc:8300?brokers=k:9092&kafka_server_name=x"); err == nil {
		t.Fatal("TLS server name accepted without kafka_tls")
	}
	for _, mechanism := range []string{"scram-sha-256", "scram-sha-512", "oauthbearer", "gssapi"} {
		ep, err := ParseEndpoint("ticdc://cdc:8300?brokers=k:9092&kafka_sasl_mechanism=" + mechanism)
		if err != nil || ep.KafkaSASLMechanism != mechanism {
			t.Fatalf("%s endpoint rejected: ep=%+v err=%v", mechanism, ep, err)
		}
	}
}

type fakePartitionKafka struct {
	records map[int32][]KafkaRecord
	parts   []int32
}

func (f *fakePartitionKafka) Fetch(_ context.Context, _ string, _ int64, _ int32) ([]KafkaRecord, int64, error) {
	return nil, 0, errors.New("single-partition Fetch must not be used")
}
func (f *fakePartitionKafka) Partitions(_ context.Context, _ string) ([]int32, error) {
	return append([]int32(nil), f.parts...), nil
}
func (f *fakePartitionKafka) FetchPartition(_ context.Context, _ string, partition int32, offset int64, _ int32) ([]KafkaRecord, int64, error) {
	out := []KafkaRecord{}
	for _, r := range f.records[partition] {
		if r.Offset >= offset {
			r.Partition = partition
			out = append(out, r)
		}
	}
	return out, int64(len(f.records[partition])), nil
}

func TestReaderMultiPartitionResolvedTSMerge(t *testing.T) {
	f := &fakePartitionKafka{parts: []int32{0, 1}, records: map[int32][]KafkaRecord{
		0: {
			{Offset: 0, Value: []byte(`{"database":"app","table":"orders","isDdl":false,"type":"INSERT","mysqlType":{"id":"bigint"},"data":[{"id":"1"}],"_tidb":{"commitTs":1000}}`)},
			{Offset: 1, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":1001}}`)},
		},
		1: {
			{Offset: 0, Value: []byte(`{"database":"app","table":"items","isDdl":false,"type":"INSERT","mysqlType":{"id":"bigint"},"data":[{"id":"2"}],"_tidb":{"commitTs":1000}}`)},
			{Offset: 1, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":1001}}`)},
		},
	}}
	r, err := NewReader(f, "topic", "cf", Position{TSO: 999, Offsets: map[int32]int64{0: 0, 1: 0}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, err := r.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 2 || tx.Events[0].SourceTable != "orders" || tx.Events[1].SourceTable != "items" {
		t.Fatalf("unexpected merged events: %+v", tx.Events)
	}
	if got := tx.Checkpoint.PositionValue; got != "tso=1000;kafka=0:2,1:2" {
		t.Fatalf("checkpoint=%s", got)
	}
	if err := r.Acknowledge(ctx, tx); err != nil {
		t.Fatal(err)
	}
	tx, err = r.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].Operation != domain.CDCCheckpoint {
		t.Fatalf("expected resolved checkpoint, got %+v", tx.Events)
	}
	if got := tx.Checkpoint.PositionValue; got != "tso=1001;kafka=0:2,1:2" {
		t.Fatalf("resolved checkpoint=%s", got)
	}
}

func TestReaderMultiPartitionUsesResolvedTSAsFence(t *testing.T) {
	f := &fakePartitionKafka{parts: []int32{0, 1}, records: map[int32][]KafkaRecord{
		0: {
			{Offset: 0, Value: []byte(`{"database":"app","table":"orders","isDdl":false,"type":"INSERT","mysqlType":{"id":"bigint"},"data":[{"id":"1"}],"_tidb":{"commitTs":2000}}`)},
			{Offset: 1, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":2001}}`)},
		},
		1: {
			{Offset: 0, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":2000}}`)},
			{Offset: 1, Value: []byte(`{"database":"","table":"","isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":2001}}`)},
			{Offset: 2, Value: []byte(`{"database":"app","table":"later","isDdl":false,"type":"INSERT","mysqlType":{"id":"bigint"},"data":[{"id":"9"}],"_tidb":{"commitTs":2002}}`)},
		},
	}}
	r, err := NewReader(f, "topic", "cf", Position{TSO: 1999, Offsets: map[int32]int64{0: 0, 1: 0}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, err := r.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.Events) != 1 || tx.Events[0].SourceTable != "orders" {
		t.Fatalf("unexpected events: %+v", tx.Events)
	}
	if got := tx.Checkpoint.PositionValue; got != "tso=2000;kafka=0:2,1:2" {
		t.Fatalf("checkpoint=%s", got)
	}
}

func TestReaderMultiPartitionRejectsTopologyExpansionAfterAck(t *testing.T) {
	f := &fakePartitionKafka{parts: []int32{0, 1}, records: map[int32][]KafkaRecord{}}
	r, err := NewReader(f, "topic", "cf", Position{TSO: 1000, Offset: 5, Offsets: map[int32]int64{0: 5}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := r.Next(ctx); err == nil || !strings.Contains(err.Error(), "changed from a single partition") {
		t.Fatalf("expected topology-change failure, got %v", err)
	}
}

func TestKafkaSASLPlainWire(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	k, err := newKafkaClient([]string{"unused:9092"}, "test-client", KafkaSecurityConfig{SASLMechanism: "plain", SASLUsername: "alice", SASLPassword: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		for step := 0; step < 2; step++ {
			var sizeBuf [4]byte
			if _, err := io.ReadFull(server, sizeBuf[:]); err != nil {
				errCh <- err
				return
			}
			sz := int(binary.BigEndian.Uint32(sizeBuf[:]))
			frame := make([]byte, sz)
			if _, err := io.ReadFull(server, frame); err != nil {
				errCh <- err
				return
			}
			apiKey := int16(binary.BigEndian.Uint16(frame[:2]))
			corr := int32(binary.BigEndian.Uint32(frame[4:8]))
			body := &kbuf{}
			switch step {
			case 0:
				if apiKey != 17 {
					errCh <- fmt.Errorf("expected SASL handshake api, got %d", apiKey)
					return
				}
				body.i16(0)
				body.i32(1)
				body.str("PLAIN")
			case 1:
				if apiKey != 36 {
					errCh <- fmt.Errorf("expected SASL authenticate api, got %d", apiKey)
					return
				}
				// Skip request header: api key/version/correlation/client-id.
				r := newKReader(frame[8:])
				if _, err := r.str(); err != nil {
					errCh <- err
					return
				}
				token, err := r.nullableBytes32()
				if err != nil {
					errCh <- err
					return
				}
				if string(token) != "\x00alice\x00secret" {
					errCh <- fmt.Errorf("unexpected PLAIN token %q", token)
					return
				}
				body.i16(0)
				body.i16(-1)
				body.i32(0)
				body.i64(0)
			}
			resp := &kbuf{}
			resp.i32(corr)
			resp.Write(body.Bytes())
			wire := &kbuf{}
			wire.i32(int32(resp.Len()))
			wire.Write(resp.Bytes())
			if _, err := server.Write(wire.Bytes()); err != nil {
				errCh <- err
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := k.authenticate(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestPBKDF2HMACSHA256Vector(t *testing.T) {
	got := pbkdf2HMAC(sha256.New, []byte("pencil"), []byte("salt"), 4096, 32)
	if hex.EncodeToString(got) != "0997564f292923271312698037b6b0a06a8be7fbd912480847c5a1ace4b8d1c7" {
		t.Fatalf("PBKDF2-HMAC-SHA256 mismatch: %x", got)
	}
}

func TestSCRAMAttributeValidation(t *testing.T) {
	attrs, err := parseSCRAMAttributes("r=nonce,s=c2FsdA==,i=4096")
	if err != nil || attrs["r"] != "nonce" || attrs["i"] != "4096" {
		t.Fatalf("attrs=%v err=%v", attrs, err)
	}
	if _, err := parseSCRAMAttributes("r=a,r=b"); err == nil {
		t.Fatal("duplicate SCRAM attribute accepted")
	}
	if got := scramEscapeUsername("a,b=c"); got != "a=2Cb=3Dc" {
		t.Fatalf("escaped username=%q", got)
	}
}

func TestKafkaSASLSCRAMSHA256Wire(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	k, err := newKafkaClient([]string{"unused:9092"}, "test-client", KafkaSecurityConfig{SASLMechanism: "scram-sha-256", SASLUsername: "alice", SASLPassword: "pencil"})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		var clientFirstBare, serverFirst string
		for step := 0; step < 3; step++ {
			var sizeBuf [4]byte
			if _, err := io.ReadFull(server, sizeBuf[:]); err != nil {
				errCh <- err
				return
			}
			sz := int(binary.BigEndian.Uint32(sizeBuf[:]))
			frame := make([]byte, sz)
			if _, err := io.ReadFull(server, frame); err != nil {
				errCh <- err
				return
			}
			apiKey := int16(binary.BigEndian.Uint16(frame[:2]))
			corr := int32(binary.BigEndian.Uint32(frame[4:8]))
			body := &kbuf{}
			switch step {
			case 0:
				if apiKey != 17 {
					errCh <- fmt.Errorf("expected SASL handshake api, got %d", apiKey)
					return
				}
				body.i16(0)
				body.i32(1)
				body.str("SCRAM-SHA-256")
			case 1:
				if apiKey != 36 {
					errCh <- fmt.Errorf("expected SCRAM first authenticate api, got %d", apiKey)
					return
				}
				r := newKReader(frame[8:])
				if _, err := r.str(); err != nil {
					errCh <- err
					return
				}
				token, err := r.nullableBytes32()
				if err != nil {
					errCh <- err
					return
				}
				first := string(token)
				if !strings.HasPrefix(first, "n,,n=alice,r=") {
					errCh <- fmt.Errorf("unexpected SCRAM client-first %q", first)
					return
				}
				clientFirstBare = strings.TrimPrefix(first, "n,,")
				attrs, err := parseSCRAMAttributes(clientFirstBare)
				if err != nil {
					errCh <- err
					return
				}
				serverFirst = "r=" + attrs["r"] + "SERVER,s=c2FsdA==,i=4096"
				body.i16(0)
				body.i16(-1)
				body.i32(int32(len(serverFirst)))
				body.Write([]byte(serverFirst))
				body.i64(0)
			case 2:
				if apiKey != 36 {
					errCh <- fmt.Errorf("expected SCRAM final authenticate api, got %d", apiKey)
					return
				}
				r := newKReader(frame[8:])
				if _, err := r.str(); err != nil {
					errCh <- err
					return
				}
				token, err := r.nullableBytes32()
				if err != nil {
					errCh <- err
					return
				}
				final := string(token)
				parts := strings.Split(final, ",p=")
				if len(parts) != 2 {
					errCh <- fmt.Errorf("SCRAM final missing proof: %q", final)
					return
				}
				authMessage := clientFirstBare + "," + serverFirst + "," + parts[0]
				salted := pbkdf2HMAC(sha256.New, []byte("pencil"), []byte("salt"), 4096, 32)
				clientKey := hmacBytes(sha256.New, salted, []byte("Client Key"))
				h := sha256.New()
				_, _ = h.Write(clientKey)
				clientSig := hmacBytes(sha256.New, h.Sum(nil), []byte(authMessage))
				expectedProof := make([]byte, len(clientKey))
				for i := range expectedProof {
					expectedProof[i] = clientKey[i] ^ clientSig[i]
				}
				gotProof, err := base64.StdEncoding.DecodeString(parts[1])
				if err != nil || !bytes.Equal(gotProof, expectedProof) {
					errCh <- fmt.Errorf("SCRAM client proof mismatch")
					return
				}
				serverKey := hmacBytes(sha256.New, salted, []byte("Server Key"))
				serverSig := hmacBytes(sha256.New, serverKey, []byte(authMessage))
				serverFinal := "v=" + base64.StdEncoding.EncodeToString(serverSig)
				body.i16(0)
				body.i16(-1)
				body.i32(int32(len(serverFinal)))
				body.Write([]byte(serverFinal))
				body.i64(0)
			}
			resp := &kbuf{}
			resp.i32(corr)
			resp.Write(body.Bytes())
			wire := &kbuf{}
			wire.i32(int32(resp.Len()))
			wire.Write(resp.Bytes())
			if _, err := server.Write(wire.Bytes()); err != nil {
				errCh <- err
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := k.authenticate(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestRawSnappyLiteralDecode(t *testing.T) {
	// uncompressed length=5, literal tag for 5 bytes.
	got, err := decodeRawSnappy([]byte{5, 4 << 2, 'h', 'e', 'l', 'l', 'o'}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}
