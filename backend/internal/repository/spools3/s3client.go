package spools3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

type s3Client struct {
	endpoint     *url.URL
	bucket       string
	region       string
	accessKey    string
	secretKey    string
	sessionToken string
	pathStyle    bool
	http         *http.Client
}

func newS3Client(cfg Config) (*s3Client, error) {
	raw := strings.TrimSpace(cfg.Endpoint)
	if raw == "" {
		return nil, errors.New("S3 endpoint is required")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid S3 endpoint %q", raw)
	}
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, errors.New("S3 bucket is required")
	}
	if strings.TrimSpace(cfg.AccessKey) == "" || strings.TrimSpace(cfg.SecretKey) == "" {
		return nil, errors.New("S3 access key and secret key are required")
	}
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = "us-east-1"
	}
	hc := cfg.HTTPClient
	if hc == nil {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(cfg.TLSServerName)}
		if strings.TrimSpace(cfg.CACert) != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(cfg.CACert)) {
				return nil, errors.New("invalid S3-compatible CA PEM")
			}
			tlsCfg.RootCAs = pool
		}
		certPEM, keyPEM := strings.TrimSpace(cfg.TLSClientCert), strings.TrimSpace(cfg.TLSClientKey)
		if certPEM != "" || keyPEM != "" {
			if certPEM == "" || keyPEM == "" {
				return nil, errors.New("S3-compatible mTLS requires both client certificate and private key")
			}
			cert, e := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
			if e != nil {
				return nil, fmt.Errorf("load S3-compatible client certificate: %w", e)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsCfg
		hc = &http.Client{Timeout: 20 * time.Second, Transport: transport}
	}
	return &s3Client{endpoint: u, bucket: cfg.Bucket, region: region, accessKey: cfg.AccessKey, secretKey: cfg.SecretKey, sessionToken: cfg.SessionToken, pathStyle: cfg.PathStyle, http: hc}, nil
}

func uriEncodePath(v string) string {
	// AWS SigV4 path encoding keeps '/' separators while percent-encoding each segment.
	parts := strings.Split(v, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	out := strings.Join(parts, "/")
	out = strings.ReplaceAll(out, "+", "%20")
	if !strings.HasPrefix(out, "/") {
		out = "/" + out
	}
	return out
}

func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		if len(vals) == 0 {
			vals = []string{""}
		}
		for _, v := range vals {
			parts = append(parts, awsEscape(k)+"="+awsEscape(v))
		}
	}
	return strings.Join(parts, "&")
}
func awsEscape(v string) string {
	x := url.QueryEscape(v)
	x = strings.ReplaceAll(x, "+", "%20")
	x = strings.ReplaceAll(x, "%7E", "~")
	return x
}
func shaHex(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func hmacSHA(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func (c *s3Client) objectURL(key string, query url.Values) (*url.URL, string) {
	u := *c.endpoint
	prefix := strings.TrimSuffix(u.Path, "/")
	cleanKey := strings.TrimPrefix(path.Clean("/"+key), "/")
	if c.pathStyle {
		u.Path = prefix + "/" + c.bucket
		if cleanKey != "." && cleanKey != "" {
			u.Path += "/" + cleanKey
		}
	} else {
		u.Host = c.bucket + "." + u.Host
		u.Path = prefix
		if cleanKey != "." && cleanKey != "" {
			u.Path += "/" + cleanKey
		}
		if u.Path == "" {
			u.Path = "/"
		}
	}
	u.RawQuery = canonicalQuery(query)
	return &u, u.EscapedPath()
}

func (c *s3Client) do(ctx context.Context, method, key string, query url.Values, body []byte, extra http.Header) (*http.Response, error) {
	if query == nil {
		query = url.Values{}
	}
	u, canonicalURI := c.objectURL(key, query)
	payloadHash := shaHex(body)
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if c.sessionToken != "" {
		req.Header.Set("x-amz-security-token", c.sessionToken)
	}
	for k, vals := range extra {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	headers := map[string]string{"host": u.Host, "x-amz-content-sha256": payloadHash, "x-amz-date": amzDate}
	if c.sessionToken != "" {
		headers["x-amz-security-token"] = c.sessionToken
	}
	for k, vals := range extra {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == "authorization" {
			continue
		}
		vv := make([]string, 0, len(vals))
		for _, v := range vals {
			vv = append(vv, strings.Join(strings.Fields(v), " "))
		}
		headers[lk] = strings.Join(vv, ",")
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var ch strings.Builder
	for _, k := range names {
		ch.WriteString(k)
		ch.WriteByte(':')
		ch.WriteString(headers[k])
		ch.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")
	canonicalReq := method + "\n" + canonicalURI + "\n" + canonicalQuery(query) + "\n" + ch.String() + "\n" + signedHeaders + "\n" + payloadHash
	scope := date + "/" + c.region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + shaHex([]byte(canonicalReq))
	kDate := hmacSHA([]byte("AWS4"+c.secretKey), date)
	kRegion := hmacSHA(kDate, c.region)
	kService := hmacSHA(kRegion, "s3")
	kSigning := hmacSHA(kService, "aws4_request")
	sig := hex.EncodeToString(hmacSHA(kSigning, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+sig)
	return c.http.Do(req)
}

func expect(resp *http.Response, allowed ...int) error {
	defer resp.Body.Close()
	for _, s := range allowed {
		if resp.StatusCode == s {
			return nil
		}
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("S3 request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
}
func (c *s3Client) HeadBucket(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodHead, "", nil, nil, nil)
	if err != nil {
		return err
	}
	return expect(resp, 200, 204)
}

type multipartUpload struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

type listMultipartUploadsResult struct {
	IsTruncated        bool   `xml:"IsTruncated"`
	NextKeyMarker      string `xml:"NextKeyMarker"`
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	Uploads            []struct {
		Key       string    `xml:"Key"`
		UploadID  string    `xml:"UploadId"`
		Initiated time.Time `xml:"Initiated"`
	} `xml:"Upload"`
}

func (c *s3Client) ListMultipartUploads(ctx context.Context, prefix string) ([]multipartUpload, error) {
	var out []multipartUpload
	keyMarker, uploadMarker := "", ""
	for pages := 0; pages < 10000; pages++ {
		q := url.Values{"uploads": {""}, "max-uploads": {"1000"}, "prefix": {prefix}}
		if keyMarker != "" {
			q.Set("key-marker", keyMarker)
		}
		if uploadMarker != "" {
			q.Set("upload-id-marker", uploadMarker)
		}
		resp, err := c.do(ctx, http.MethodGet, "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, expect(resp, http.StatusOK)
		}
		var x listMultipartUploadsResult
		err = xml.NewDecoder(resp.Body).Decode(&x)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("decode S3 ListMultipartUploads: %w", err)
		}
		for _, u := range x.Uploads {
			if strings.TrimSpace(u.Key) == "" || strings.TrimSpace(u.UploadID) == "" {
				continue
			}
			out = append(out, multipartUpload{Key: u.Key, UploadID: u.UploadID, Initiated: u.Initiated})
		}
		if !x.IsTruncated {
			return out, nil
		}
		if x.NextKeyMarker == "" {
			return nil, errors.New("S3 ListMultipartUploads truncated without next key marker")
		}
		keyMarker, uploadMarker = x.NextKeyMarker, x.NextUploadIDMarker
	}
	return nil, errors.New("S3 ListMultipartUploads exceeded page safety limit")
}

func (c *s3Client) AbortStaleMultipartUploads(ctx context.Context, prefix string, cutoff time.Time) error {
	uploads, err := c.ListMultipartUploads(ctx, prefix)
	if err != nil {
		return err
	}
	for _, u := range uploads {
		// Some S3-compatible implementations omit Initiated. Never abort an
		// upload whose age cannot be proven.
		if u.Initiated.IsZero() || !u.Initiated.Before(cutoff) {
			continue
		}
		if err := c.AbortMultipart(ctx, u.Key, u.UploadID); err != nil {
			return fmt.Errorf("abort stale multipart upload key=%s upload_id=%s: %w", u.Key, u.UploadID, err)
		}
	}
	return nil
}

type initiateMultipartUploadResult struct {
	UploadID string `xml:"UploadId"`
}

type completeMultipartUpload struct {
	XMLName xml.Name                `xml:"CompleteMultipartUpload"`
	Parts   []completeMultipartPart `xml:"Part"`
}

type completeMultipartPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

func (c *s3Client) PutAuto(ctx context.Context, key string, b []byte, threshold, partSize int64) error {
	if threshold <= 0 || int64(len(b)) < threshold {
		return c.Put(ctx, key, b)
	}
	return c.PutMultipart(ctx, key, b, partSize)
}

func (c *s3Client) PutMultipart(ctx context.Context, key string, b []byte, partSize int64) error {
	const minimum = int64(5 << 20)
	if partSize < minimum {
		return fmt.Errorf("S3 multipart part size must be at least %d bytes", minimum)
	}
	q := url.Values{"uploads": {""}}
	resp, err := c.do(ctx, http.MethodPost, key, q, nil, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return expect(resp, http.StatusOK, http.StatusCreated)
	}
	var init initiateMultipartUploadResult
	err = xml.NewDecoder(resp.Body).Decode(&init)
	_ = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("decode S3 CreateMultipartUpload: %w", err)
	}
	if strings.TrimSpace(init.UploadID) == "" {
		return errors.New("S3 CreateMultipartUpload returned empty upload id")
	}

	abort := true
	defer func() {
		if abort {
			_ = c.AbortMultipart(context.Background(), key, init.UploadID)
		}
	}()
	parts := make([]completeMultipartPart, 0, (int64(len(b))+partSize-1)/partSize)
	for off, partNumber := int64(0), 1; off < int64(len(b)); off, partNumber = off+partSize, partNumber+1 {
		end := off + partSize
		if end > int64(len(b)) {
			end = int64(len(b))
		}
		q := url.Values{"partNumber": {strconv.Itoa(partNumber)}, "uploadId": {init.UploadID}}
		resp, err := c.do(ctx, http.MethodPut, key, q, b[off:end], nil)
		if err != nil {
			return fmt.Errorf("upload S3 multipart part %d: %w", partNumber, err)
		}
		if resp.StatusCode != http.StatusOK {
			return expect(resp, http.StatusOK)
		}
		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		_ = resp.Body.Close()
		if etag == "" {
			return fmt.Errorf("S3 multipart part %d returned empty ETag", partNumber)
		}
		parts = append(parts, completeMultipartPart{PartNumber: partNumber, ETag: etag})
	}
	body, err := xml.Marshal(completeMultipartUpload{Parts: parts})
	if err != nil {
		return err
	}
	q = url.Values{"uploadId": {init.UploadID}}
	resp, err = c.do(ctx, http.MethodPost, key, q, body, http.Header{"content-type": {"application/xml"}})
	if err != nil {
		return fmt.Errorf("complete S3 multipart upload: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return expect(resp, http.StatusOK, http.StatusCreated)
	}
	_ = resp.Body.Close()
	abort = false
	return nil
}

func (c *s3Client) AbortMultipart(ctx context.Context, key, uploadID string) error {
	if strings.TrimSpace(uploadID) == "" {
		return nil
	}
	q := url.Values{"uploadId": {uploadID}}
	resp, err := c.do(ctx, http.MethodDelete, key, q, nil, nil)
	if err != nil {
		return err
	}
	return expect(resp, http.StatusOK, http.StatusNoContent)
}

func (c *s3Client) Put(ctx context.Context, key string, b []byte) error {
	resp, err := c.do(ctx, http.MethodPut, key, nil, b, nil)
	if err != nil {
		return err
	}
	return expect(resp, 200, 201, 204)
}
func (c *s3Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, key, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("S3 GET %s failed: status=%d body=%s", key, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return io.ReadAll(resp.Body)
}
func (c *s3Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, key, nil, nil, nil)
	if err != nil {
		return err
	}
	return expect(resp, 200, 204)
}
func (c *s3Client) Copy(ctx context.Context, src, dst string) error {
	h := make(http.Header)
	source := "/" + c.bucket + "/" + strings.TrimPrefix(src, "/")
	h.Set("x-amz-copy-source", uriEncodePath(source))
	resp, err := c.do(ctx, http.MethodPut, dst, nil, nil, h)
	if err != nil {
		return err
	}
	return expect(resp, 200)
}

type listedObject struct {
	Key          string
	LastModified time.Time
	Size         int64
}
type listBucketResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string    `xml:"Key"`
		LastModified time.Time `xml:"LastModified"`
		Size         int64     `xml:"Size"`
	} `xml:"Contents"`
}

func (c *s3Client) ListPrefix(ctx context.Context, prefix string) ([]listedObject, error) {
	var out []listedObject
	token := ""
	for pages := 0; pages < 10000; pages++ {
		q := url.Values{"list-type": {"2"}, "max-keys": {"1000"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := c.do(ctx, http.MethodGet, "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			e := expect(resp, http.StatusOK)
			return nil, e
		}
		var x listBucketResult
		err = xml.NewDecoder(resp.Body).Decode(&x)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("decode S3 ListObjectsV2: %w", err)
		}
		for _, v := range x.Contents {
			out = append(out, listedObject{Key: v.Key, LastModified: v.LastModified, Size: v.Size})
		}
		if !x.IsTruncated {
			return out, nil
		}
		if x.NextContinuationToken == "" {
			return nil, errors.New("S3 ListObjectsV2 truncated without continuation token")
		}
		token = x.NextContinuationToken
	}
	return nil, errors.New("S3 ListObjectsV2 exceeded page safety limit")
}
