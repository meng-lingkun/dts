package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Protocol verified against the installed SDK /usr/local/rds/openapi/sdk/polarx/main.dara.
// Even GET can mutate DBStack; this client deliberately exposes only the patrol read API.
type APIConfig struct {
	Endpoint                  string   `json:"endpoint"`
	Region                    string   `json:"region"`
	AccessKeyEnv              string   `json:"access_key_env"`
	SecretKeyEnv              string   `json:"secret_key_env"`
	InsecureTLS               bool     `json:"insecure_tls,omitempty"`
	MaxPages                  int      `json:"max_pages"`
	HealthyStatusDescriptions []string `json:"healthy_status_descriptions"`
}

func validateAPI(c APIConfig) error {
	if c.Endpoint == "" {
		return nil
	}
	u, e := url.Parse(c.Endpoint)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("OpenAPI endpoint 必须是无参数根 URL")
	}
	if c.Region == "" || c.MaxPages < 1 || c.MaxPages > 100 {
		return errors.New("OpenAPI region/max_pages 无效")
	}
	return nil
}
func percent(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
func rpcSignature(q url.Values, secret string) string {
	canonical := strings.ReplaceAll(q.Encode(), "+", "%20")
	h := hmac.New(sha1.New, []byte(secret+"&"))
	h.Write([]byte("GET&%2F&" + percent(canonical)))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
func (r *Inspector) openAPI() {
	c := r.cfg.OpenAPI
	if c.Endpoint == "" {
		r.add("UNKNOWN", "OpenAPI", "DescribeInstancePatrolInfo", "未配置业务 OpenAPI，API 实例清单未验证")
		return
	}
	ak, sk := os.Getenv(c.AccessKeyEnv), os.Getenv(c.SecretKeyEnv)
	if ak == "" || sk == "" {
		r.add("UNKNOWN", "OpenAPI", "DescribeInstancePatrolInfo", "缺少 AK 环境变量，未调用需鉴权的业务 API")
		return
	}
	if c.InsecureTLS || strings.HasPrefix(c.Endpoint, "http://") {
		r.add("WARN", "OpenAPI", "传输", "配置使用 HTTP 或跳过证书校验；推荐 HTTPS 加受信任 CA")
	}
	cli := httpClient(r.timeout(), c.InsecureTLS)
	defer cli.CloseIdleConnections()
	seen := map[string]bool{}
	inventory := []Obj{}
	for page := 1; page <= c.MaxPages; page++ {
		nonce := make([]byte, 16)
		if _, e := rand.Read(nonce); e != nil {
			r.add("UNKNOWN", "OpenAPI", "nonce", "随机数生成失败")
			return
		}
		q := url.Values{"Action": {"DescribeInstancePatrolInfo"}, "Format": {"json"}, "OpenApiVersion": {"v1"}, "ServiceCode": {"polardbx"}, "RegionId": {c.Region}, "PageNum": {strconv.Itoa(page)}, "PageSize": {"100"}, "Timestamp": {time.Now().UTC().Format("2006-01-02T15:04:05Z")}, "SignatureNonce": {hex.EncodeToString(nonce)}, "SignatureMethod": {"HMAC-SHA1"}, "SignatureVersion": {"1.0"}, "AccessKeyId": {ak}}
		q.Set("Signature", rpcSignature(q, sk))
		ctx, cancel := context.WithTimeout(r.ctx, r.timeout())
		req, e := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(c.Endpoint, "/")+"/?"+q.Encode(), nil)
		if e != nil {
			cancel()
			r.add("UNKNOWN", "OpenAPI", "request", "无法构造请求")
			return
		}
		req.Header.Set("x-acs-version", "v1")
		req.Header.Set("x-acs-action", "DescribeInstancePatrolInfo")
		resp, e := cli.Do(req)
		if e != nil {
			cancel()
			r.add("UNKNOWN", "OpenAPI", "DescribeInstancePatrolInfo", "连接/TLS/超时错误（签名 URL 已隐藏）")
			return
		}
		b, e := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		resp.Body.Close()
		cancel()
		if e != nil || len(b) > 8<<20 {
			r.add("UNKNOWN", "OpenAPI", "response", "响应读取失败或超过 8 MiB")
			return
		}
		if resp.StatusCode != http.StatusOK {
			r.add("UNKNOWN", "OpenAPI", "DescribeInstancePatrolInfo", fmt.Sprintf("HTTP %d；业务 API 未验证", resp.StatusCode))
			return
		}
		var d Obj
		if json.Unmarshal(b, &d) != nil {
			r.add("UNKNOWN", "OpenAPI", "response", "非 JSON 响应")
			return
		}
		ok, known := businessOK(d)
		if !ok || !known {
			r.add("UNKNOWN", "OpenAPI", "response", "API 业务失败或成功字段不明确")
			return
		}
		values, exists := d["dbInstances"].([]any)
		if !exists {
			r.add("UNKNOWN", "OpenAPI", "schema", "缺少 dbInstances 数组，需核对本版本响应结构")
			return
		}
		for _, v := range values {
			n := str(at(v, "dbInstanceName"))
			if n == "" {
				r.add("UNKNOWN", "OpenAPI", "schema", "实例名称缺失")
				continue
			}
			if seen[n] {
				r.add("UNKNOWN", "OpenAPI", n, "分页出现重复实例，清单完整性无法保证")
				return
			}
			seen[n] = true
			s := "UNKNOWN"
			desc := str(at(v, "statusDesc"))
			for _, healthy := range c.HealthyStatusDescriptions {
				if desc == healthy {
					s = "OK"
				}
			}
			for _, bad := range []string{"异常", "失败", "error", "failed", "unavailable"} {
				if strings.Contains(strings.ToLower(desc), bad) {
					s = "CRITICAL"
				}
			}
			r.add(s, "API实例", n, fmt.Sprintf("status=%s statusDesc=%s CN=%s DN=%s GMS=%s CDC=%s", str(at(v, "status")), desc, str(at(v, "cnNodeCount")), str(at(v, "dnNodeCount")), str(at(v, "gmsNodeCount")), str(at(v, "cdcNodeCount"))))
			inventory = append(inventory, Obj{"name": n, "status": at(v, "status"), "status_description": desc, "cn": at(v, "cnNodeCount"), "dn": at(v, "dnNodeCount"), "gms": at(v, "gmsNodeCount"), "cdc": at(v, "cdcNodeCount")})
		}
		r.report.Inventory["api_instances"] = inventory
		totalPages := int(num(d["totalPage"]))
		if (totalPages > 0 && page >= totalPages) || (totalPages == 0 && len(values) < 100) {
			if len(seen) == 0 {
				r.add("WARN", "OpenAPI", "清单", "业务 API 返回零实例，请核对 Region 和账号权限")
			}
			return
		}
	}
	r.add("UNKNOWN", "OpenAPI", "分页", "达到 max_pages，清单可能不完整")
}
