package ticdc

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	kafkaMaxResponse = 128 << 20
	kafkaMaxRecord   = 64 << 20
)

type KafkaRecord struct {
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

type KafkaSecurityConfig struct {
	TLS           bool
	ServerName    string
	CAFile        string
	ClientCert    string
	ClientKey     string
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	SASLToken     string
	SASLProvider  string
}

type KafkaClient struct {
	brokers            []string
	clientID           string
	correlationID      atomic.Int32
	dialTimeout        time.Duration
	security           KafkaSecurityConfig
	expectedPartitions int
}

func NewKafkaClient(brokers []string, clientID string) (*KafkaClient, error) {
	return newKafkaClient(brokers, clientID, KafkaSecurityConfig{})
}

func NewKafkaClientForEndpoint(ep Endpoint, clientID string) (*KafkaClient, error) {
	security := KafkaSecurityConfig{
		TLS:           ep.KafkaTLS,
		ServerName:    ep.KafkaServerName,
		CAFile:        ep.KafkaCA,
		ClientCert:    ep.KafkaCert,
		ClientKey:     ep.KafkaKey,
		SASLMechanism: strings.ToLower(strings.TrimSpace(ep.KafkaSASLMechanism)),
		SASLUsername:  os.Getenv("QMIGRATION_TIDB_KAFKA_SASL_USERNAME"),
		SASLPassword:  os.Getenv("QMIGRATION_TIDB_KAFKA_SASL_PASSWORD"),
		SASLToken:     os.Getenv("QMIGRATION_TIDB_KAFKA_SASL_TOKEN"),
		SASLProvider:  os.Getenv("QMIGRATION_TIDB_KAFKA_SASL_PROVIDER"),
	}
	client, err := newKafkaClient(ep.Brokers, clientID, security)
	if err != nil {
		return nil, err
	}
	if ep.KafkaPartitions > 0 {
		client.expectedPartitions = ep.KafkaPartitions
	}
	return client, nil
}

func newKafkaClient(brokers []string, clientID string, security KafkaSecurityConfig) (*KafkaClient, error) {
	if len(brokers) == 0 {
		return nil, errors.New("Kafka client requires bootstrap brokers")
	}
	if strings.TrimSpace(clientID) == "" {
		clientID = "qmigration-ticdc"
	}
	security.SASLMechanism = strings.ToLower(strings.TrimSpace(security.SASLMechanism))
	switch security.SASLMechanism {
	case "", "plain", "scram-sha-256", "scram-sha-512", "oauthbearer", "gssapi":
	default:
		return nil, fmt.Errorf("unsupported Kafka SASL mechanism %q; native consumer supports PLAIN, SCRAM-SHA-256/512, OAUTHBEARER and external GSSAPI provider", security.SASLMechanism)
	}
	switch security.SASLMechanism {
	case "plain", "scram-sha-256", "scram-sha-512":
		if security.SASLUsername == "" || security.SASLPassword == "" {
			return nil, errors.New("Kafka password SASL requires username and password")
		}
	case "oauthbearer":
		if strings.TrimSpace(security.SASLToken) == "" {
			return nil, errors.New("Kafka OAUTHBEARER requires QMIGRATION_TIDB_KAFKA_SASL_TOKEN")
		}
	case "gssapi":
		if strings.TrimSpace(security.SASLProvider) == "" {
			return nil, errors.New("Kafka GSSAPI requires QMIGRATION_TIDB_KAFKA_SASL_PROVIDER")
		}
	}
	if (security.ClientCert == "") != (security.ClientKey == "") {
		return nil, errors.New("Kafka client certificate and key must be configured together")
	}
	return &KafkaClient{brokers: append([]string(nil), brokers...), clientID: clientID, dialTimeout: 5 * time.Second, security: security}, nil
}

type kafkaPartitionMeta struct {
	Leader  string
	Count   int
	Leaders map[int32]string
}

func (k *KafkaClient) Metadata(ctx context.Context, topic string) (kafkaPartitionMeta, error) {
	if strings.TrimSpace(topic) == "" {
		return kafkaPartitionMeta{}, errors.New("Kafka topic is empty")
	}
	body := &kbuf{}
	body.i32(1)
	body.str(topic)
	var last error
	for _, broker := range k.brokers {
		resp, err := k.request(ctx, broker, 3, 0, body.Bytes())
		if err != nil {
			last = err
			continue
		}
		meta, err := parseMetadataV0(resp, topic)
		if err != nil {
			last = err
			continue
		}
		if k.expectedPartitions > 0 && meta.Count != k.expectedPartitions {
			return kafkaPartitionMeta{}, fmt.Errorf("Kafka topic %q partition topology changed: expected %d, got %d; QMigration refuses to infer new partition offsets during an active capture", topic, k.expectedPartitions, meta.Count)
		}
		return meta, nil
	}
	if last == nil {
		last = errors.New("no Kafka bootstrap broker available")
	}
	return kafkaPartitionMeta{}, last
}

func (k *KafkaClient) Partitions(ctx context.Context, topic string) ([]int32, error) {
	meta, err := k.Metadata(ctx, topic)
	if err != nil {
		return nil, err
	}
	parts := make([]int32, 0, len(meta.Leaders))
	for partition := range meta.Leaders {
		parts = append(parts, partition)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	if len(parts) == 0 {
		return nil, fmt.Errorf("Kafka topic %q has no available partition leaders", topic)
	}
	return parts, nil
}

func (k *KafkaClient) Fetch(ctx context.Context, topic string, offset int64, maxBytes int32) ([]KafkaRecord, int64, error) {
	meta, err := k.Metadata(ctx, topic)
	if err != nil {
		return nil, 0, err
	}
	if meta.Count != 1 {
		return nil, 0, fmt.Errorf("TiCDC topic %q has %d partitions; use partition-aware reader", topic, meta.Count)
	}
	return k.fetchPartitionWithMeta(ctx, topic, 0, offset, maxBytes, meta)
}

func (k *KafkaClient) FetchPartition(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) ([]KafkaRecord, int64, error) {
	if partition < 0 {
		return nil, 0, errors.New("Kafka partition cannot be negative")
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		meta, err := k.Metadata(ctx, topic)
		if err != nil {
			last = err
		} else {
			recs, hwm, fetchErr := k.fetchPartitionWithMeta(ctx, topic, partition, offset, maxBytes, meta)
			if fetchErr == nil {
				return recs, hwm, nil
			}
			last = fetchErr
		}
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
			}
		}
	}
	return nil, 0, fmt.Errorf("Kafka partition %d fetch failed after leader metadata refresh: %w", partition, last)
}

func (k *KafkaClient) fetchPartitionWithMeta(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32, meta kafkaPartitionMeta) ([]KafkaRecord, int64, error) {
	if offset < 0 {
		return nil, 0, errors.New("Kafka offset cannot be negative")
	}
	if maxBytes <= 0 || maxBytes > kafkaMaxResponse {
		maxBytes = 16 << 20
	}
	leader := meta.Leaders[partition]
	if leader == "" {
		return nil, 0, fmt.Errorf("Kafka partition %d leader unavailable", partition)
	}
	body := &kbuf{}
	body.i32(-1)
	body.i32(2000)
	body.i32(1)
	body.i32(1)
	body.str(topic)
	body.i32(1)
	body.i32(partition)
	body.i64(offset)
	body.i32(maxBytes)
	resp, err := k.request(ctx, leader, 1, 0, body.Bytes())
	if err != nil {
		return nil, 0, err
	}
	recs, hwm, err := parseFetchPartitionV0(resp, topic, partition)
	for i := range recs {
		recs[i].Partition = partition
	}
	return recs, hwm, err
}

func (k *KafkaClient) request(ctx context.Context, addr string, apiKey, apiVersion int16, body []byte) ([]byte, error) {
	conn, err := k.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := k.authenticate(ctx, conn); err != nil {
		return nil, err
	}
	return k.requestConn(ctx, conn, apiKey, apiVersion, body)
}

func (k *KafkaClient) dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: k.dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Kafka dial %s: %w", addr, err)
	}
	if !k.security.TLS {
		return conn, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	host, _, _ := net.SplitHostPort(addr)
	tlsConfig.ServerName = strings.TrimSpace(k.security.ServerName)
	if tlsConfig.ServerName == "" {
		tlsConfig.ServerName = host
	}
	if k.security.CAFile != "" {
		pem, err := os.ReadFile(k.security.CAFile)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("read Kafka CA file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			_ = conn.Close()
			return nil, errors.New("Kafka CA file contains no valid certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if k.security.ClientCert != "" {
		cert, err := tls.LoadX509KeyPair(k.security.ClientCert, k.security.ClientKey)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("load Kafka mTLS certificate/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	tlsConn := tls.Client(conn, tlsConfig)
	if deadline, ok := ctx.Deadline(); ok {
		_ = tlsConn.SetDeadline(deadline)
	} else {
		_ = tlsConn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("Kafka TLS handshake %s: %w", addr, err)
	}
	return tlsConn, nil
}

func (k *KafkaClient) authenticate(ctx context.Context, conn net.Conn) error {
	if k.security.SASLMechanism == "" {
		return nil
	}
	mechanism := strings.ToUpper(k.security.SASLMechanism)
	h := &kbuf{}
	h.str(mechanism)
	resp, err := k.requestConn(ctx, conn, 17, 1, h.Bytes())
	if err != nil {
		return fmt.Errorf("Kafka SASL handshake: %w", err)
	}
	r := newKReader(resp)
	code, err := r.i16()
	if err != nil {
		return fmt.Errorf("Kafka SASL handshake response: %w", err)
	}
	count, err := r.i32()
	if err != nil || count < 0 || count > 1000 {
		return errors.New("invalid Kafka SASL mechanism list")
	}
	enabled := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		m, err := r.str()
		if err != nil {
			return err
		}
		enabled = append(enabled, m)
	}
	if code != 0 {
		return fmt.Errorf("Kafka SASL mechanism %s rejected (error=%d enabled=%s)", mechanism, code, strings.Join(enabled, ","))
	}
	switch mechanism {
	case "PLAIN":
		token := []byte("\x00" + k.security.SASLUsername + "\x00" + k.security.SASLPassword)
		_, err := k.saslAuthenticateBytes(ctx, conn, token)
		if err != nil {
			return fmt.Errorf("Kafka SASL/PLAIN authentication failed: %w", err)
		}
		return nil
	case "SCRAM-SHA-256", "SCRAM-SHA-512":
		return k.authenticateSCRAM(ctx, conn, mechanism)
	case "OAUTHBEARER":
		token := []byte("n,,\x01auth=Bearer " + k.security.SASLToken + "\x01\x01")
		_, err := k.saslAuthenticateBytes(ctx, conn, token)
		if err != nil {
			return fmt.Errorf("Kafka SASL/OAUTHBEARER authentication failed: %w", err)
		}
		return nil
	case "GSSAPI":
		return k.authenticateProvider(ctx, conn, mechanism)
	default:
		return fmt.Errorf("Kafka SASL mechanism %s is not implemented", mechanism)
	}
}

func (k *KafkaClient) saslAuthenticateBytes(ctx context.Context, conn net.Conn, token []byte) ([]byte, error) {
	b := &kbuf{}
	b.i32(int32(len(token)))
	b.Write(token)
	resp, err := k.requestConn(ctx, conn, 36, 1, b.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Kafka SASL authenticate: %w", err)
	}
	r := newKReader(resp)
	code, err := r.i16()
	if err != nil {
		return nil, err
	}
	message, err := r.nullableString()
	if err != nil {
		return nil, err
	}
	authBytes, err := r.nullableBytes32()
	if err != nil {
		return nil, err
	}
	if _, err := r.i64(); err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, fmt.Errorf("broker error=%d: %s", code, message)
	}
	return authBytes, nil
}

func (k *KafkaClient) authenticateSCRAM(ctx context.Context, conn net.Conn, mechanism string) error {
	var hashNew func() hash.Hash
	switch mechanism {
	case "SCRAM-SHA-256":
		hashNew = sha256.New
	case "SCRAM-SHA-512":
		hashNew = sha512.New
	default:
		return fmt.Errorf("unsupported SCRAM mechanism %s", mechanism)
	}
	nonceBytes := make([]byte, 24)
	if _, err := rand.Read(nonceBytes); err != nil {
		return fmt.Errorf("generate SCRAM nonce: %w", err)
	}
	nonce := base64.RawStdEncoding.EncodeToString(nonceBytes)
	user := scramEscapeUsername(k.security.SASLUsername)
	clientFirstBare := "n=" + user + ",r=" + nonce
	serverFirstBytes, err := k.saslAuthenticateBytes(ctx, conn, []byte("n,,"+clientFirstBare))
	if err != nil {
		return fmt.Errorf("SCRAM client-first: %w", err)
	}
	serverFirst := string(serverFirstBytes)
	attrs, err := parseSCRAMAttributes(serverFirst)
	if err != nil {
		return fmt.Errorf("SCRAM server-first: %w", err)
	}
	if attrs["m"] != "" {
		return errors.New("SCRAM server-first contains unsupported mandatory extension")
	}
	serverNonce := attrs["r"]
	if !strings.HasPrefix(serverNonce, nonce) || len(serverNonce) <= len(nonce) {
		return errors.New("SCRAM server nonce does not extend client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(attrs["s"])
	if err != nil || len(salt) == 0 || len(salt) > 1<<20 {
		return errors.New("SCRAM server salt is invalid")
	}
	iterations, err := strconv.Atoi(attrs["i"])
	if err != nil || iterations < 4096 || iterations > 1000000 {
		return fmt.Errorf("SCRAM iteration count %q is outside 4096..1000000", attrs["i"])
	}
	salted := pbkdf2HMAC(hashNew, []byte(k.security.SASLPassword), salt, iterations, hashNew().Size())
	clientKey := hmacBytes(hashNew, salted, []byte("Client Key"))
	storedHash := hashNew()
	_, _ = storedHash.Write(clientKey)
	storedKey := storedHash.Sum(nil)
	clientFinalWithoutProof := "c=biws,r=" + serverNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	clientSignature := hmacBytes(hashNew, storedKey, []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	serverKey := hmacBytes(hashNew, salted, []byte("Server Key"))
	expectedServerSignature := hmacBytes(hashNew, serverKey, []byte(authMessage))
	clientFinal := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	serverFinalBytes, err := k.saslAuthenticateBytes(ctx, conn, []byte(clientFinal))
	if err != nil {
		return fmt.Errorf("SCRAM client-final: %w", err)
	}
	finalAttrs, err := parseSCRAMAttributes(string(serverFinalBytes))
	if err != nil {
		return fmt.Errorf("SCRAM server-final: %w", err)
	}
	if message := finalAttrs["e"]; message != "" {
		return fmt.Errorf("SCRAM server rejected authentication: %s", message)
	}
	sigRaw := finalAttrs["v"]
	if sigRaw == "" {
		return errors.New("SCRAM server-final is missing verifier")
	}
	serverSignature, err := base64.StdEncoding.DecodeString(sigRaw)
	if err != nil || !hmac.Equal(serverSignature, expectedServerSignature) {
		return errors.New("SCRAM server signature verification failed")
	}
	return nil
}

func scramEscapeUsername(username string) string {
	username = strings.ReplaceAll(username, "=", "=3D")
	return strings.ReplaceAll(username, ",", "=2C")
}

func parseSCRAMAttributes(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("empty SCRAM message")
	}
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 || len(kv[0]) != 1 {
			return nil, fmt.Errorf("malformed SCRAM attribute %q", part)
		}
		if _, exists := out[kv[0]]; exists {
			return nil, fmt.Errorf("duplicate SCRAM attribute %q", kv[0])
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}

func hmacBytes(hashNew func() hash.Hash, key, data []byte) []byte {
	h := hmac.New(hashNew, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func pbkdf2HMAC(hashNew func() hash.Hash, password, salt []byte, iterations, keyLen int) []byte {
	hLen := hashNew().Size()
	blocks := (keyLen + hLen - 1) / hLen
	out := make([]byte, 0, blocks*hLen)
	for block := 1; block <= blocks; block++ {
		buf := make([]byte, len(salt)+4)
		copy(buf, salt)
		binary.BigEndian.PutUint32(buf[len(salt):], uint32(block))
		u := hmacBytes(hashNew, password, buf)
		t := append([]byte(nil), u...)
		for i := 1; i < iterations; i++ {
			u = hmacBytes(hashNew, password, u)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func (k *KafkaClient) requestConn(ctx context.Context, conn net.Conn, apiKey, apiVersion int16, body []byte) ([]byte, error) {
	correlation := k.correlationID.Add(1)
	req := &kbuf{}
	req.i16(apiKey)
	req.i16(apiVersion)
	req.i32(correlation)
	req.str(k.clientID)
	req.Write(body)
	frame := &kbuf{}
	frame.i32(int32(req.Len()))
	frame.Write(req.Bytes())
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	if _, err := conn.Write(frame.Bytes()); err != nil {
		return nil, fmt.Errorf("Kafka write: %w", err)
	}
	var szBuf [4]byte
	if _, err := io.ReadFull(conn, szBuf[:]); err != nil {
		return nil, fmt.Errorf("Kafka read response size: %w", err)
	}
	sz := int(binary.BigEndian.Uint32(szBuf[:]))
	if sz < 4 || sz > kafkaMaxResponse {
		return nil, fmt.Errorf("Kafka response size %d exceeds bounds", sz)
	}
	resp := make([]byte, sz)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("Kafka read response: %w", err)
	}
	if int32(binary.BigEndian.Uint32(resp[:4])) != correlation {
		return nil, errors.New("Kafka correlation ID mismatch")
	}
	return resp[4:], nil
}

func (k *KafkaClient) probeConnection(ctx context.Context, addr string) error {
	conn, err := k.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	return k.authenticate(ctx, conn)
}

func parseMetadataV0(raw []byte, topic string) (kafkaPartitionMeta, error) {
	r := newKReader(raw)
	brokerCount, err := r.i32()
	if err != nil || brokerCount < 0 || brokerCount > 10000 {
		return kafkaPartitionMeta{}, errors.New("invalid Kafka metadata broker count")
	}
	brokers := map[int32]string{}
	for i := int32(0); i < brokerCount; i++ {
		id, err := r.i32()
		if err != nil {
			return kafkaPartitionMeta{}, err
		}
		host, err := r.str()
		if err != nil {
			return kafkaPartitionMeta{}, err
		}
		port, err := r.i32()
		if err != nil {
			return kafkaPartitionMeta{}, err
		}
		brokers[id] = net.JoinHostPort(host, fmt.Sprint(port))
	}
	topicCount, err := r.i32()
	if err != nil || topicCount < 0 || topicCount > 100000 {
		return kafkaPartitionMeta{}, errors.New("invalid Kafka metadata topic count")
	}
	for i := int32(0); i < topicCount; i++ {
		errCode, err := r.i16()
		if err != nil {
			return kafkaPartitionMeta{}, err
		}
		name, err := r.str()
		if err != nil {
			return kafkaPartitionMeta{}, err
		}
		partCount, err := r.i32()
		if err != nil || partCount < 0 || partCount > 100000 {
			return kafkaPartitionMeta{}, errors.New("invalid Kafka partition count")
		}
		leaders := map[int32]string{}
		for p := int32(0); p < partCount; p++ {
			pErr, err := r.i16()
			if err != nil {
				return kafkaPartitionMeta{}, err
			}
			partition, err := r.i32()
			if err != nil {
				return kafkaPartitionMeta{}, err
			}
			leaderID, err := r.i32()
			if err != nil {
				return kafkaPartitionMeta{}, err
			}
			replicas, err := r.i32()
			if err != nil || replicas < 0 || replicas > 10000 {
				return kafkaPartitionMeta{}, errors.New("invalid Kafka replica count")
			}
			for x := int32(0); x < replicas; x++ {
				if _, err := r.i32(); err != nil {
					return kafkaPartitionMeta{}, err
				}
			}
			isr, err := r.i32()
			if err != nil || isr < 0 || isr > 10000 {
				return kafkaPartitionMeta{}, errors.New("invalid Kafka ISR count")
			}
			for x := int32(0); x < isr; x++ {
				if _, err := r.i32(); err != nil {
					return kafkaPartitionMeta{}, err
				}
			}
			if name == topic {
				if pErr != 0 {
					return kafkaPartitionMeta{}, fmt.Errorf("Kafka partition %d metadata error %d", partition, pErr)
				}
				if leader := brokers[leaderID]; leader != "" {
					leaders[partition] = leader
				}
			}
		}
		if name == topic {
			if errCode != 0 {
				return kafkaPartitionMeta{}, fmt.Errorf("Kafka topic metadata error %d", errCode)
			}
			if len(leaders) != int(partCount) {
				return kafkaPartitionMeta{}, fmt.Errorf("Kafka topic %q has %d partitions but only %d leaders available", topic, partCount, len(leaders))
			}
			return kafkaPartitionMeta{Leader: leaders[0], Count: int(partCount), Leaders: leaders}, nil
		}
	}
	return kafkaPartitionMeta{}, fmt.Errorf("Kafka topic %q not found", topic)
}

func parseFetchV0(raw []byte, topic string) ([]KafkaRecord, int64, error) {
	return parseFetchPartitionV0(raw, topic, 0)
}

func parseFetchPartitionV0(raw []byte, topic string, wantedPartition int32) ([]KafkaRecord, int64, error) {
	r := newKReader(raw)
	topics, err := r.i32()
	if err != nil || topics < 0 || topics > 100000 {
		return nil, 0, errors.New("invalid Kafka fetch topic count")
	}
	for i := int32(0); i < topics; i++ {
		name, err := r.str()
		if err != nil {
			return nil, 0, err
		}
		parts, err := r.i32()
		if err != nil || parts < 0 || parts > 100000 {
			return nil, 0, errors.New("invalid Kafka fetch partition count")
		}
		for p := int32(0); p < parts; p++ {
			partition, err := r.i32()
			if err != nil {
				return nil, 0, err
			}
			errCode, err := r.i16()
			if err != nil {
				return nil, 0, err
			}
			hwm, err := r.i64()
			if err != nil {
				return nil, 0, err
			}
			size, err := r.i32()
			if err != nil || size < 0 || size > kafkaMaxResponse {
				return nil, 0, errors.New("invalid Kafka record-set size")
			}
			set, err := r.bytes(int(size))
			if err != nil {
				return nil, 0, err
			}
			if name == topic && partition == wantedPartition {
				if errCode != 0 {
					return nil, hwm, fmt.Errorf("Kafka fetch partition %d error code %d", partition, errCode)
				}
				recs, err := parseRecordSet(set)
				for i := range recs {
					recs[i].Partition = partition
				}
				return recs, hwm, err
			}
		}
	}
	return nil, 0, fmt.Errorf("Kafka fetch response missing %s partition %d", topic, wantedPartition)
}
func parseRecordSet(raw []byte) ([]KafkaRecord, error) {
	out := []KafkaRecord{}
	for len(raw) >= 12 {
		baseOffset := int64(binary.BigEndian.Uint64(raw[:8]))
		size := int(int32(binary.BigEndian.Uint32(raw[8:12])))
		if size < 0 || size > kafkaMaxRecord || 12+size > len(raw) {
			// A fetch may end with a partial trailing record when MaxBytes cuts the
			// response. Kafka consumers are expected to ignore the partial tail.
			break
		}
		entry := raw[12 : 12+size]
		if len(entry) < 6 {
			return nil, errors.New("truncated Kafka message/batch")
		}
		magic := entry[4]
		var recs []KafkaRecord
		var err error
		if magic <= 1 {
			recs, err = parseLegacyMessage(baseOffset, entry)
		} else if magic == 2 {
			// For record batches the record-set element starts at baseOffset and
			// size includes the bytes after batchLength; reconstruct the 12-byte
			// batch prefix consumed above for the parser.
			batch := raw[:12+size]
			recs, err = parseRecordBatch(batch)
		} else {
			err = fmt.Errorf("unsupported Kafka message magic %d", magic)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		raw = raw[12+size:]
	}
	return out, nil
}

func parseLegacyMessage(offset int64, msg []byte) ([]KafkaRecord, error) {
	if len(msg) < 6 {
		return nil, errors.New("truncated legacy Kafka message")
	}
	want := binary.BigEndian.Uint32(msg[:4])
	if crc32.ChecksumIEEE(msg[4:]) != want {
		return nil, errors.New("legacy Kafka message CRC mismatch")
	}
	magic, attrs := msg[4], msg[5]
	if attrs&0x07 != 0 {
		return nil, fmt.Errorf("compressed Kafka legacy messages are not supported (codec=%d)", attrs&0x07)
	}
	r := newKReader(msg[6:])
	if magic == 1 {
		if _, err := r.i64(); err != nil {
			return nil, err
		}
	}
	key, err := r.nullableBytes32()
	if err != nil {
		return nil, err
	}
	value, err := r.nullableBytes32()
	if err != nil {
		return nil, err
	}
	return []KafkaRecord{{Offset: offset, Key: key, Value: value}}, nil
}

func parseRecordBatch(batch []byte) ([]KafkaRecord, error) {
	if len(batch) < 61 {
		return nil, errors.New("truncated Kafka record batch")
	}
	baseOffset := int64(binary.BigEndian.Uint64(batch[:8]))
	batchLen := int(int32(binary.BigEndian.Uint32(batch[8:12])))
	if batchLen < 49 || 12+batchLen > len(batch) {
		return nil, errors.New("invalid Kafka record batch length")
	}
	if batch[16] != 2 {
		return nil, errors.New("Kafka record batch magic is not 2")
	}
	wantCRC := binary.BigEndian.Uint32(batch[17:21])
	crcTable := crc32.MakeTable(crc32.Castagnoli)
	if crc32.Checksum(batch[21:12+batchLen], crcTable) != wantCRC {
		return nil, errors.New("Kafka record batch CRC32C mismatch")
	}
	attrs := int16(binary.BigEndian.Uint16(batch[21:23]))
	codec := int(attrs & 0x07)
	count := int(int32(binary.BigEndian.Uint32(batch[57:61])))
	if count < 0 || count > 1_000_000 {
		return nil, errors.New("invalid Kafka record count")
	}
	raw := batch[61 : 12+batchLen]
	if codec != 0 {
		var err error
		raw, err = decompressKafkaRecords(codec, raw)
		if err != nil {
			return nil, err
		}
	}
	out := make([]KafkaRecord, 0, count)
	for i := 0; i < count; i++ {
		length, n := binary.Varint(raw)
		if n <= 0 || length < 0 || length > kafkaMaxRecord || n+int(length) > len(raw) {
			return nil, errors.New("invalid Kafka record length varint")
		}
		record := raw[n : n+int(length)]
		rr := &varReader{b: record}
		if _, err := rr.byte(); err != nil {
			return nil, err
		} // attributes
		if _, err := rr.varint(); err != nil {
			return nil, err
		} // timestamp delta
		offsetDelta, err := rr.varint()
		if err != nil {
			return nil, err
		}
		key, err := rr.varBytes()
		if err != nil {
			return nil, err
		}
		value, err := rr.varBytes()
		if err != nil {
			return nil, err
		}
		headers, err := rr.varint()
		if err != nil || headers < 0 || headers > 100000 {
			return nil, errors.New("invalid Kafka record header count")
		}
		for h := int64(0); h < headers; h++ {
			if _, err := rr.varBytes(); err != nil {
				return nil, err
			}
			if _, err := rr.varBytes(); err != nil {
				return nil, err
			}
		}
		out = append(out, KafkaRecord{Offset: baseOffset + offsetDelta, Key: key, Value: value})
		raw = raw[n+int(length):]
	}
	return out, nil
}

type kbuf struct{ bytes.Buffer }

func (b *kbuf) i16(v int16) {
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], uint16(v))
	b.Write(x[:])
}
func (b *kbuf) i32(v int32) {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], uint32(v))
	b.Write(x[:])
}
func (b *kbuf) i64(v int64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], uint64(v))
	b.Write(x[:])
}
func (b *kbuf) str(s string) { b.i16(int16(len(s))); b.WriteString(s) }

type kreader struct {
	b   []byte
	off int
}

func newKReader(b []byte) *kreader { return &kreader{b: b} }
func (r *kreader) bytes(n int) ([]byte, error) {
	if n < 0 || r.off+n > len(r.b) {
		return nil, io.ErrUnexpectedEOF
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v, nil
}
func (r *kreader) i16() (int16, error) {
	b, e := r.bytes(2)
	if e != nil {
		return 0, e
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}
func (r *kreader) i32() (int32, error) {
	b, e := r.bytes(4)
	if e != nil {
		return 0, e
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}
func (r *kreader) i64() (int64, error) {
	b, e := r.bytes(8)
	if e != nil {
		return 0, e
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}
func (r *kreader) nullableString() (string, error) {
	n, e := r.i16()
	if e != nil {
		return "", e
	}
	if n < 0 {
		return "", nil
	}
	b, e := r.bytes(int(n))
	return string(b), e
}
func (r *kreader) str() (string, error) {
	n, e := r.i16()
	if e != nil {
		return "", e
	}
	if n < 0 {
		return "", errors.New("null Kafka string")
	}
	b, e := r.bytes(int(n))
	return string(b), e
}
func (r *kreader) nullableBytes32() ([]byte, error) {
	n, e := r.i32()
	if e != nil {
		return nil, e
	}
	if n < 0 {
		return nil, nil
	}
	return r.bytes(int(n))
}

type varReader struct {
	b   []byte
	off int
}

func (r *varReader) byte() (byte, error) {
	if r.off >= len(r.b) {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}
func (r *varReader) varint() (int64, error) {
	v, n := binary.Varint(r.b[r.off:])
	if n <= 0 {
		return 0, errors.New("invalid Kafka varint")
	}
	r.off += n
	return v, nil
}
func (r *varReader) varBytes() ([]byte, error) {
	n, e := r.varint()
	if e != nil {
		return nil, e
	}
	if n < 0 {
		return nil, nil
	}
	if n > kafkaMaxRecord || r.off+int(n) > len(r.b) {
		return nil, io.ErrUnexpectedEOF
	}
	v := r.b[r.off : r.off+int(n)]
	r.off += int(n)
	return v, nil
}

type saslProviderRequest struct {
	Mechanism string `json:"mechanism"`
	Challenge string `json:"challenge_b64,omitempty"`
	Round     int    `json:"round"`
}
type saslProviderResponse struct {
	Response string `json:"response_b64"`
	Done     bool   `json:"done"`
	Error    string `json:"error,omitempty"`
}

func (k *KafkaClient) authenticateProvider(ctx context.Context, conn net.Conn, mechanism string) error {
	cmd := exec.CommandContext(ctx, k.security.SASLProvider)
	in, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	enc := json.NewEncoder(in)
	dec := json.NewDecoder(io.LimitReader(out, 4<<20))
	challenge := []byte(nil)
	for round := 0; round < 16; round++ {
		req := saslProviderRequest{Mechanism: mechanism, Round: round}
		if len(challenge) > 0 {
			req.Challenge = base64.StdEncoding.EncodeToString(challenge)
		}
		if err := enc.Encode(req); err != nil {
			return err
		}
		var resp saslProviderResponse
		if err := dec.Decode(&resp); err != nil {
			return fmt.Errorf("SASL provider: %w: %s", err, stderr.String())
		}
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		token, err := base64.StdEncoding.DecodeString(resp.Response)
		if err != nil {
			return errors.New("SASL provider returned invalid base64")
		}
		challenge, err = k.saslAuthenticateBytes(ctx, conn, token)
		if err != nil {
			return err
		}
		if resp.Done {
			_ = in.Close()
			if err := cmd.Wait(); err != nil {
				return fmt.Errorf("SASL provider exit: %w: %s", err, stderr.String())
			}
			return nil
		}
	}
	_ = cmd.Process.Kill()
	return errors.New("SASL provider exceeded 16 challenge rounds")
}

func decompressKafkaRecords(codec int, raw []byte) ([]byte, error) {
	switch codec {
	case 1:
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("Kafka gzip: %w", err)
		}
		defer zr.Close()
		return readBounded(zr, kafkaMaxResponse)
	case 2:
		return decodeXerialSnappy(raw, kafkaMaxResponse)
	case 3:
		return decompressKafkaHelper("QMIGRATION_KAFKA_LZ4_BIN", raw)
	case 4:
		return decompressKafkaHelper("QMIGRATION_KAFKA_ZSTD_BIN", raw)
	default:
		return nil, fmt.Errorf("unsupported Kafka compression codec %d", codec)
	}
}
func readBounded(r io.Reader, max int) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, errors.New("Kafka decompressed record batch exceeds safety limit")
	}
	return b, nil
}
func decompressKafkaHelper(envName string, raw []byte) ([]byte, error) {
	bin := strings.TrimSpace(os.Getenv(envName))
	if bin == "" {
		return nil, fmt.Errorf("Kafka compression helper %s is required", envName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Stdin = bytes.NewReader(raw)
	var out, er bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &er
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("Kafka compression helper: %w: %s", err, strings.TrimSpace(er.String()))
	}
	if out.Len() > kafkaMaxResponse {
		return nil, errors.New("Kafka compression helper output exceeds safety limit")
	}
	return out.Bytes(), nil
}

func decodeXerialSnappy(in []byte, max int) ([]byte, error) {
	magic := []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}
	if len(in) >= 16 && bytes.Equal(in[:8], magic) {
		pos := 16
		out := []byte{}
		for pos < len(in) {
			if pos+4 > len(in) {
				return nil, errors.New("truncated xerial snappy chunk length")
			}
			n := int(binary.BigEndian.Uint32(in[pos : pos+4]))
			pos += 4
			if n < 0 || pos+n > len(in) {
				return nil, errors.New("invalid xerial snappy chunk")
			}
			chunk, err := decodeRawSnappy(in[pos:pos+n], max-len(out))
			if err != nil {
				return nil, err
			}
			out = append(out, chunk...)
			if len(out) > max {
				return nil, errors.New("snappy output exceeds safety limit")
			}
			pos += n
		}
		return out, nil
	}
	return decodeRawSnappy(in, max)
}
func decodeRawSnappy(in []byte, max int) ([]byte, error) {
	expected, n := binary.Uvarint(in)
	if n <= 0 || expected > uint64(max) {
		return nil, errors.New("invalid snappy uncompressed length")
	}
	in = in[n:]
	out := make([]byte, 0, int(expected))
	for len(in) > 0 && len(out) < int(expected) {
		tag := in[0]
		in = in[1:]
		typ := tag & 3
		switch typ {
		case 0:
			l := int(tag >> 2)
			if l < 60 {
				l++
			} else {
				nb := l - 59
				if nb < 1 || nb > 4 || len(in) < nb {
					return nil, errors.New("invalid snappy literal length")
				}
				l = 0
				for i := 0; i < nb; i++ {
					l |= int(in[i]) << (8 * i)
				}
				l++
				in = in[nb:]
			}
			if l < 0 || len(in) < l || len(out)+l > int(expected) {
				return nil, errors.New("truncated snappy literal")
			}
			out = append(out, in[:l]...)
			in = in[l:]
		case 1:
			if len(in) < 1 {
				return nil, errors.New("truncated snappy copy1")
			}
			l := 4 + int((tag>>2)&7)
			off := int(tag&0xe0)<<3 | int(in[0])
			in = in[1:]
			var err error
			out, err = snappyCopy(out, off, l, int(expected))
			if err != nil {
				return nil, err
			}
		case 2:
			if len(in) < 2 {
				return nil, errors.New("truncated snappy copy2")
			}
			l := 1 + int(tag>>2)
			off := int(in[0]) | int(in[1])<<8
			in = in[2:]
			var err error
			out, err = snappyCopy(out, off, l, int(expected))
			if err != nil {
				return nil, err
			}
		case 3:
			if len(in) < 4 {
				return nil, errors.New("truncated snappy copy4")
			}
			l := 1 + int(tag>>2)
			off64 := uint64(binary.LittleEndian.Uint32(in[:4]))
			in = in[4:]
			if off64 > uint64(len(out)) {
				return nil, errors.New("snappy copy offset outside output")
			}
			var err error
			out, err = snappyCopy(out, int(off64), l, int(expected))
			if err != nil {
				return nil, err
			}
		}
	}
	if len(out) != int(expected) {
		return nil, fmt.Errorf("snappy length mismatch got=%d want=%d", len(out), expected)
	}
	return out, nil
}
func snappyCopy(out []byte, off, l, expected int) ([]byte, error) {
	if off <= 0 || off > len(out) || len(out)+l > expected {
		return nil, errors.New("invalid snappy copy")
	}
	for i := 0; i < l; i++ {
		out = append(out, out[len(out)-off])
	}
	return out, nil
}
