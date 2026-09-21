package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type HTTPCheck struct {
	Name             string `json:"name"`
	URL              string `json:"url"`
	Host             string `json:"host,omitempty"`
	InsecureTLS      bool   `json:"insecure_tls,omitempty"`
	JSON             bool   `json:"json,omitempty"`
	Contains         string `json:"contains,omitempty"`
	ExpectedStatus   int    `json:"expected_status,omitempty"`
	ExpectedLocation string `json:"expected_location,omitempty"`
}

func resolveServiceHTTP(h HTTPCheck, services []any) (HTTPCheck, error) {
	u, e := url.Parse(h.URL)
	if e != nil {
		return h, e
	}
	if !strings.HasSuffix(u.Hostname(), ".svc.cluster.local") {
		return h, nil
	}
	for _, s := range services {
		if u.Hostname() != name(s)+"."+ns(s)+".svc.cluster.local" {
			continue
		}
		ip := str(at(s, "spec", "clusterIP"))
		if net.ParseIP(ip) == nil {
			return h, errors.New("Service 没有 ClusterIP")
		}
		port := u.Port()
		if port == "" {
			port = "80"
			if u.Scheme == "https" {
				port = "443"
			}
		}
		u.Host = net.JoinHostPort(ip, port)
		h.URL = u.String()
		return h, nil
	}
	return h, errors.New("未发现配置的 Service，无法解析健康检查地址")
}

func validateHTTP(h HTTPCheck) error {
	u, e := url.Parse(h.URL)
	if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("HTTP endpoint 无效")
	}
	if !safeHealthPath(u.Path) || u.RawQuery != "" {
		return errors.New("HTTP 仅支持明确的健康检查路径，不接受业务 Action 或查询参数")
	}
	if h.ExpectedStatus != 0 && (h.ExpectedStatus < 200 || h.ExpectedStatus >= 400) {
		return errors.New("expected_status 必须为 2xx/3xx")
	}
	if h.ExpectedStatus >= 300 && h.ExpectedLocation == "" {
		return errors.New("重定向检查必须指定 expected_location")
	}
	return nil
}
func safeHealthPath(p string) bool {
	switch p {
	case "/health", "/healthz", "/ready", "/readyz", "/-/ready", "/-/healthy", "/livez", "/healthCheck/check", "/common/healthCheck", "/actuator/health":
		return true
	}
	return false
}
func httpClient(timeout time.Duration, insecure bool) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure}, DialContext: (&net.Dialer{Timeout: timeout}).DialContext, MaxIdleConns: 4, IdleConnTimeout: 10 * time.Second}}
}
func businessOK(d Obj) (bool, bool) {
	known := false
	for _, k := range []string{"Code", "code"} {
		if v, ok := d[k]; ok {
			known = true
			s := strings.ToLower(str(v))
			if s != "200" && s != "success" && s != "ok" && s != "0" {
				return false, true
			}
		}
	}
	for _, k := range []string{"success", "Success", "successResponse", "SuccessResponse"} {
		if v, ok := d[k]; ok {
			known = true
			if v != true && str(v) != "true" {
				return false, true
			}
		}
	}
	if v, ok := d["withFailedRequest"]; ok && v == true {
		return false, true
	}
	if s := strings.ToUpper(str(d["status"])); s == "UP" {
		known = true
	} else if s == "DOWN" || s == "OUT_OF_SERVICE" {
		return false, true
	}
	return true, known
}
func (r *Inspector) httpCheck(h HTTPCheck) {
	target := h.Name
	if target == "" {
		target = h.URL
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout())
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, "GET", h.URL, nil)
	if e != nil {
		r.add("UNKNOWN", "HTTP", target, "URL 无效")
		return
	}
	req.Host = h.Host
	client := httpClient(r.timeout(), h.InsecureTLS)
	defer client.CloseIdleConnections()
	start := time.Now()
	resp, e := client.Do(req)
	if e != nil {
		var unknown x509.UnknownAuthorityError
		var invalid x509.CertificateInvalidError
		var hostname x509.HostnameError
		if errors.As(e, &unknown) || errors.As(e, &invalid) || errors.As(e, &hostname) {
			r.add("UNKNOWN", "HTTP", target, "TLS 证书信任/名称校验失败；不能据此判断服务故障")
		} else {
			r.add("CRITICAL", "HTTP", target, "请求失败（连接/超时/TLS）；未输出响应或凭据")
		}
		return
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if e != nil || len(b) > 1<<20 {
		r.add("UNKNOWN", "HTTP", target, "响应读取失败或超过 1 MiB")
		return
	}
	s := "OK"
	d := fmt.Sprintf("HTTP %d，耗时 %dms", resp.StatusCode, time.Since(start).Milliseconds())
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s = "CRITICAL"
	}
	if h.ExpectedStatus != 0 {
		if resp.StatusCode == h.ExpectedStatus {
			s = "OK"
		} else {
			s = "CRITICAL"
		}
	}
	if h.ExpectedLocation != "" {
		if resp.Header.Get("Location") != h.ExpectedLocation {
			s = "CRITICAL"
			d += "；重定向目标不符"
		} else {
			d += "；仅确认预期重定向"
		}
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		s = "UNKNOWN"
		d += "；需要应用鉴权"
	}
	if s == "OK" && h.JSON {
		var v Obj
		if json.Unmarshal(b, &v) != nil {
			s = "CRITICAL"
			d += "；未返回 JSON"
		} else if ok, known := businessOK(v); !ok {
			s = "CRITICAL"
			d += "；业务状态失败"
		} else if !known {
			s = "UNKNOWN"
			d += "；未识别业务成功字段"
		}
	}
	if s == "OK" && h.Contains != "" && !strings.Contains(string(b), h.Contains) {
		s = "CRITICAL"
		d += "；响应不含预期内容"
	}
	if h.InsecureTLS {
		if s == "OK" {
			s = "WARN"
		}
		d += "；已显式跳过 TLS 证书校验"
	}
	r.add(s, "HTTP", target, d)
}
func (r *Inspector) networkTasks(pods []any) []func() {
	tasks := []func(){}
	seen := map[string]bool{}
	for _, p := range pods {
		if !r.scoped(p) || str(at(p, "status", "phase")) != "Running" {
			continue
		}
		ip := str(at(p, "status", "podIP"))
		if net.ParseIP(ip) == nil {
			continue
		}
		if dbGroup(p) != "" {
			port := str(at(p, "metadata", "labels", "apsara.ins.port"))
			n, e := strconv.Atoi(port)
			if e == nil && n > 0 && n <= 65535 {
				addr := net.JoinHostPort(ip, port)
				target := id(p)
				tasks = append(tasks, func() { r.mysqlGreeting(target, addr) })
			}
		}
		for _, c := range arr(at(p, "spec", "containers")) {
			h := at(c, "readinessProbe", "httpGet")
			path := str(at(h, "path"))
			if !safeHealthPath(path) || len(arr(at(h, "httpHeaders"))) != 0 {
				continue
			}
			port := str(at(h, "port"))
			if _, e := strconv.Atoi(port); e != nil {
				for _, cp := range arr(at(c, "ports")) {
					if str(at(cp, "name")) == port {
						port = str(at(cp, "containerPort"))
						break
					}
				}
			}
			if _, e := strconv.Atoi(port); e != nil {
				continue
			}
			scheme := strings.ToLower(str(at(h, "scheme")))
			if scheme == "" {
				scheme = "http"
			}
			host := str(at(h, "host"))
			if host == "" {
				host = ip
			}
			u := scheme + "://" + net.JoinHostPort(host, port) + path
			if seen[u] {
				continue
			}
			seen[u] = true
			check := HTTPCheck{Name: id(p) + "/" + str(at(c, "name")), URL: u, JSON: path == "/common/healthCheck"}
			tasks = append(tasks, func() { r.httpCheck(check) })
		}
	}
	return tasks
}

// Reads only a bounded server greeting; performs no login and never sends SQL.
func (r *Inspector) mysqlGreeting(target, addr string) {
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout())
	defer cancel()
	c, e := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if e != nil {
		r.add("CRITICAL", "数据库协议", target, "TCP 连接失败 "+addr)
		return
	}
	defer c.Close()
	deadline := time.Now().Add(r.timeout())
	if t, ok := ctx.Deadline(); ok && t.Before(deadline) {
		deadline = t
	}
	_ = c.SetDeadline(deadline)
	version, e := readGreeting(c)
	if e != nil {
		state := "CRITICAL"
		var serverErr *greetingError
		if errors.As(e, &serverErr) && (serverErr.code == 1130 || serverErr.code == 1129 || serverErr.code == 1045) {
			state = "UNKNOWN"
		}
		r.add(state, "数据库协议", target, addr+" "+e.Error())
		return
	}
	r.add("OK", "数据库协议", target, addr+" MySQL greeting="+version+"（未登录）")
}
func readGreeting(rd io.Reader) (string, error) {
	hdr := make([]byte, 4)
	if _, e := io.ReadFull(rd, hdr); e != nil {
		return "", errors.New("握手包读取失败")
	}
	n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	if hdr[3] != 0 || n < 2 || n > 64*1024 {
		return "", errors.New("非法 MySQL 握手包")
	}
	b := make([]byte, n)
	if _, e := io.ReadFull(rd, b); e != nil {
		return "", errors.New("握手包不完整")
	}
	if b[0] != 10 {
		if b[0] == 255 && len(b) >= 3 {
			return "", &greetingError{int(b[1]) | int(b[2])<<8}
		}
		return "", errors.New("服务器未返回 MySQL protocol 10 greeting")
	}
	end := -1
	for i := 1; i < len(b); i++ {
		if b[i] == 0 {
			end = i
			break
		}
	}
	if end < 2 || end > 200 {
		return "", errors.New("服务器版本字段非法")
	}
	s := string(b[1:end])
	for _, ch := range s {
		if ch < 32 || ch > 126 {
			return "", errors.New("服务器版本含控制字符")
		}
	}
	return s, nil
}

type greetingError struct{ code int }

func (e *greetingError) Error() string {
	return fmt.Sprintf("MySQL ERR %d（1130/1129/1045 表示访问限制，未验证数据库健康）", e.code)
}

func (r *Inspector) serviceTasks(services, slices []any) []func() {
	tasks := []func(){}
	seen := map[string]bool{}
	present := map[string]bool{}
	add := func(target, addr string) {
		if seen[addr] {
			return
		}
		seen[addr] = true
		tasks = append(tasks, func() { r.mysqlGreeting(target, addr) })
	}
	for _, s := range services {
		if !r.scoped(s) {
			continue
		}
		present[id(s)] = true
		if !strings.HasPrefix(name(s), "pxc-") {
			continue
		}
		for _, p := range arr(at(s, "spec", "ports")) {
			port := int(num(at(p, "port")))
			if port <= 0 {
				continue
			}
			ip := str(at(s, "spec", "clusterIP"))
			if net.ParseIP(ip) != nil {
				add(id(s)+"/service", net.JoinHostPort(ip, strconv.Itoa(port)))
			}
			for _, v := range arr(at(s, "spec", "externalIPs")) {
				if net.ParseIP(str(v)) != nil {
					add(id(s)+"/external", net.JoinHostPort(str(v), strconv.Itoa(port)))
				}
			}
		}
	}
	for _, s := range slices {
		if !r.scoped(s) || !strings.HasPrefix(str(at(s, "metadata", "labels", "kubernetes.io/service-name")), "pxc-") {
			continue
		}
		for _, e := range arr(at(s, "endpoints")) {
			if at(e, "conditions", "ready") == false || at(e, "conditions", "terminating") == true {
				continue
			}
			for _, p := range arr(at(s, "ports")) {
				if proto := str(at(p, "protocol")); proto != "" && proto != "TCP" {
					continue
				}
				port := int(num(at(p, "port")))
				if port < 1 || port > 65535 {
					continue
				}
				for _, ip := range arr(at(e, "addresses")) {
					if net.ParseIP(str(ip)) != nil {
						add(id(s)+"/backend", net.JoinHostPort(str(ip), strconv.Itoa(port)))
					}
				}
			}
		}
	}
	for _, expected := range r.cfg.ExpectedServices {
		if !present[expected] {
			r.add("CRITICAL", "实例入口", expected, "配置基线中的 Service 已缺失")
		}
	}
	return tasks
}
