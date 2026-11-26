package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type store struct {
	client         *s3.Client
	bucket, prefix string
	page           int32
}
type object struct {
	Key      string `json:"key"`
	ETag     string `json:"etag"`
	Modified string `json:"modified"`
	Size     int64  `json:"size"`
	SHA      string `json:"sha256,omitempty"`
}

func validKey(key, prefix string, size int64) bool {
	if size < 0 || !strings.HasPrefix(key, prefix) {
		return false
	}
	// Console-created directory markers are ordinary zero-byte S3 objects.
	if strings.HasSuffix(key, "/") {
		if size != 0 {
			return false
		}
		key = strings.TrimSuffix(key, "/")
	}
	return key != "" && path.Clean(key) == key
}

func configuredStore() (*store, error) {
	get := func(key string) string { return os.Getenv("DTS_CDC_SPOOL_S3_" + key) }
	endpoint, e := url.Parse(strings.TrimSpace(get("ENDPOINT")))
	if e != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, fmt.Errorf("invalid DTS_CDC_SPOOL_S3_ENDPOINT")
	}
	prefix := strings.Trim(strings.TrimSpace(get("PREFIX")), "/")
	if prefix == "" {
		prefix = "dts/cdc-spool"
	}
	if path.Clean(prefix) != prefix || prefix == "." || strings.HasPrefix(prefix, "..") || get("BUCKET") == "" || get("ACCESS_KEY") == "" || get("SECRET_KEY") == "" {
		return nil, fmt.Errorf("require dedicated prefix, bucket and S3 credentials")
	}
	t := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: get("TLS_SERVER_NAME")}
	if pem := get("CA_CERT"); pem != "" {
		t.RootCAs = x509.NewCertPool()
		if !t.RootCAs.AppendCertsFromPEM([]byte(pem)) {
			return nil, fmt.Errorf("invalid S3 CA")
		}
	}
	if cert, key := get("TLS_CLIENT_CERT"), get("TLS_CLIENT_KEY"); cert != "" || key != "" {
		pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
		if err != nil {
			return nil, err
		}
		t.Certificates = []tls.Certificate{pair}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = t
	transport.ResponseHeaderTimeout = 60 * time.Second
	pathStyle := true
	switch strings.ToLower(strings.TrimSpace(get("PATH_STYLE"))) {
	case "0", "false", "no", "off":
		pathStyle = false
	case "", "1", "true", "yes", "on":
	default:
		return nil, fmt.Errorf("invalid S3 path-style setting")
	}
	c := s3.New(s3.Options{
		BaseEndpoint:               aws.String(endpoint.String()),
		Region:                     defaultTo(get("REGION"), "us-east-1"),
		UsePathStyle:               pathStyle,
		Credentials:                credentials.NewStaticCredentialsProvider(get("ACCESS_KEY"), get("SECRET_KEY"), get("SESSION_TOKEN")),
		HTTPClient:                 &http.Client{Transport: transport},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return &store{client: c, bucket: get("BUCKET"), prefix: prefix + "/", page: 1000}, nil
}

func (s *store) list(ctx context.Context) ([]object, error) {
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &s.prefix, MaxKeys: &s.page})
	seen := map[string]bool{}
	tokens := map[string]bool{}
	result := []object{}
	for p.HasMorePages() {
		res, e := p.NextPage(ctx)
		if e != nil {
			return nil, e
		}
		if aws.ToBool(res.IsTruncated) {
			next := aws.ToString(res.NextContinuationToken)
			if next == "" || tokens[next] {
				return nil, fmt.Errorf("invalid repeated/empty S3 page token")
			}
			tokens[next] = true
		}
		for _, item := range res.Contents {
			key := aws.ToString(item.Key)
			etag := aws.ToString(item.ETag)
			if seen[key] || etag == "" || item.Size == nil || item.LastModified == nil || !validKey(key, s.prefix, *item.Size) {
				return nil, fmt.Errorf("invalid or duplicate S3 key in response")
			}
			seen[key] = true
			result = append(result, object{Key: key, ETag: etag, Size: *item.Size, Modified: item.LastModified.UTC().Format(time.RFC3339Nano)})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *store) inactive(ctx context.Context) error {
	out, e := s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &s.bucket, Prefix: &s.prefix, MaxUploads: aws.Int32(1)})
	if e != nil {
		return e
	}
	if len(out.Uploads) > 0 || aws.ToBool(out.IsTruncated) {
		return fmt.Errorf("outstanding multipart uploads in spool prefix")
	}
	return nil
}
func (s *store) virgin(ctx context.Context) error {
	items, e := s.list(ctx)
	if e != nil {
		return e
	}
	if len(items) > 0 {
		return fmt.Errorf("recovery prefix is not empty")
	}
	v, e := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &s.bucket, Prefix: &s.prefix, MaxKeys: aws.Int32(1)})
	if e != nil {
		return e
	}
	if len(v.Versions) > 0 || len(v.DeleteMarkers) > 0 || aws.ToBool(v.IsTruncated) {
		return fmt.Errorf("recovery prefix contains object history")
	}
	return s.inactive(ctx)
}
func (s *store) copyOut(ctx context.Context, x object, w io.Writer) (string, error) {
	r, e := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &x.Key, IfMatch: &x.ETag})
	if e != nil {
		return "", e
	}
	defer r.Body.Close()
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(h, w), r.Body)
	if e != nil {
		return "", e
	}
	if n != x.Size || aws.ToString(r.ETag) != x.ETag {
		return "", fmt.Errorf("object changed during copy: %s", x.Key)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (s *store) put(ctx context.Context, x object, f *os.File) error {
	const minPart = int64(16 << 20)
	if x.Size <= minPart {
		_, e := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &x.Key, IfNoneMatch: aws.String("*"), ContentLength: &x.Size, Body: f})
		return e
	}
	up, e := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.bucket, Key: &x.Key})
	if e != nil {
		return e
	}
	var parts []types.CompletedPart
	complete := false
	defer func() {
		if !complete {
			c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = s.client.AbortMultipartUpload(c, &s3.AbortMultipartUploadInput{Bucket: &s.bucket, Key: &x.Key, UploadId: up.UploadId})
		}
	}()
	partSize := max(minPart, (x.Size+9999)/10000)
	for pos := int64(0); pos < x.Size; pos += partSize {
		n := min(partSize, x.Size-pos)
		num := int32(len(parts) + 1)
		res, err := s.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &s.bucket, Key: &x.Key, UploadId: up.UploadId, PartNumber: &num, ContentLength: &n, Body: io.NewSectionReader(f, pos, n)})
		if err != nil {
			return err
		}
		parts = append(parts, types.CompletedPart{PartNumber: &num, ETag: res.ETag})
	}
	_, e = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &s.bucket, Key: &x.Key, UploadId: up.UploadId, IfNoneMatch: aws.String("*"), MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	complete = e == nil
	return e
}

const marker = ".dts-recovery-incomplete"

func (s *store) reserve(ctx context.Context) (string, error) {
	var nonce [32]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return "", e
	}
	key := s.prefix + marker
	res, e := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(nonce[:]), IfNoneMatch: aws.String("*")})
	if e != nil {
		return "", e
	}
	return aws.ToString(res.ETag), nil
}
func (s *store) finish(ctx context.Context, etag string) error {
	key := s.prefix + marker
	_, e := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key, IfMatch: &etag})
	return e
}
