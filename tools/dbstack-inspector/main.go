package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Obj = map[string]any

func at(v any, keys ...string) any {
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}
func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
func num(v any) float64 { n, _ := strconv.ParseFloat(str(v), 64); return n }
func arr(v any) []any   { a, _ := v.([]any); return a }
func obj(v any) Obj     { m, _ := v.(map[string]any); return m }
func name(v any) string { return str(at(v, "metadata", "name")) }
func ns(v any) string   { return str(at(v, "metadata", "namespace")) }
func id(v any) string {
	if ns(v) == "" {
		return name(v)
	}
	return ns(v) + "/" + name(v)
}
func stamp(v any) time.Time { t, _ := time.Parse(time.RFC3339, str(v)); return t }
func condition(v any, kind string) string {
	for _, c := range arr(at(v, "status", "conditions")) {
		if str(at(c, "type")) == kind {
			return str(at(c, "status"))
		}
	}
	return ""
}

type Config struct {
	Namespaces       []string       `json:"namespaces"`
	TimeoutSeconds   int            `json:"timeout_seconds"`
	Workers          int            `json:"workers"`
	RecentMinutes    int            `json:"recent_minutes"`
	BackupMaxHours   int            `json:"backup_max_hours"`
	CPUWarn          float64        `json:"cpu_warn_percent"`
	MemoryWarn       float64        `json:"memory_warn_percent"`
	DiskWarn         float64        `json:"disk_warn_percent"`
	DiskCritical     float64        `json:"disk_critical_percent"`
	NodeProxy        bool           `json:"node_proxy"`
	ExpectedGroups   map[string]int `json:"expected_groups"`
	ExpectedServices []string       `json:"expected_services"`
	HTTP             []HTTPCheck    `json:"http"`
	SQL              []SQLCheck     `json:"sql"`
	OpenAPI          APIConfig      `json:"openapi"`
}
type Check struct {
	Status   string `json:"status"`
	Category string `json:"category"`
	Target   string `json:"target"`
	Detail   string `json:"detail"`
}
type Report struct {
	Started   time.Time      `json:"started"`
	Finished  time.Time      `json:"finished"`
	Host      string         `json:"host"`
	Summary   map[string]int `json:"summary"`
	Checks    []Check        `json:"checks"`
	Inventory map[string]any `json:"inventory"`
}
type Inspector struct {
	cfg                              Config
	ctx                              context.Context
	kubectl, kubeconfig, kubecontext string
	report                           Report
	mu                               sync.Mutex
}

func (r *Inspector) add(s, c, t, d string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.report.Checks = append(r.report.Checks, Check{s, c, t, d})
}
func (r *Inspector) scoped(v any) bool {
	for _, n := range r.cfg.Namespaces {
		if n == ns(v) {
			return true
		}
	}
	return false
}
func (r *Inspector) timeout() time.Duration { return time.Duration(r.cfg.TimeoutSeconds) * time.Second }

// Bound subprocess output as well as duration. Never include raw API/CLI output in errors.
type limitedBuffer struct {
	bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.max {
		return 0, errors.New("output limit exceeded")
	}
	return b.Buffer.Write(p)
}
func (r *Inspector) kube(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout())
	defer cancel()
	base := []string{"--request-timeout=" + strconv.Itoa(r.cfg.TimeoutSeconds) + "s"}
	if r.kubeconfig != "" {
		base = append(base, "--kubeconfig", r.kubeconfig)
	}
	if r.kubecontext != "" {
		base = append(base, "--context", r.kubecontext)
	}
	cmd := exec.CommandContext(ctx, r.kubectl, append(base, args...)...)
	out := &limitedBuffer{max: 64 << 20}
	cmd.Stdout = out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("kubectl failed: %w (检查连接与 RBAC)", err)
	}
	return out.Bytes(), nil
}
func (r *Inspector) list(resource string) ([]any, bool) {
	b, err := r.kube("get", resource, "-A", "-o", "json")
	if err != nil {
		r.add("UNKNOWN", "采集", resource, err.Error())
		return nil, false
	}
	var d Obj
	if json.Unmarshal(b, &d) != nil {
		r.add("UNKNOWN", "采集", resource, "返回数据不是合法 JSON")
		return nil, false
	}
	a, ok := d["items"].([]any)
	if !ok {
		r.add("UNKNOWN", "采集", resource, "缺少 items")
		return nil, false
	}
	return a, true
}
func (r *Inspector) raw(path string) (Obj, error) {
	b, e := r.kube("get", "--raw", path)
	if e != nil {
		return nil, e
	}
	var d Obj
	e = json.Unmarshal(b, &d)
	return d, e
}
func (r *Inspector) parallel(tasks []func()) {
	jobs := make(chan func())
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				f()
			}
		}()
	}
	for _, f := range tasks {
		jobs <- f
	}
	close(jobs)
	wg.Wait()
}
func (r *Inspector) run() {
	if b, e := r.kube("get", "--raw", "/readyz"); e != nil {
		r.add("UNKNOWN", "Kubernetes", "readyz", e.Error())
	} else if strings.TrimSpace(string(b)) != "ok" {
		r.add("CRITICAL", "Kubernetes", "readyz", "API Server 未就绪")
	} else {
		r.add("OK", "Kubernetes", "readyz", "API Server 就绪")
	}
	nodes, nok := r.list("nodes")
	if nok {
		r.nodes(nodes)
	}
	pods, pok := r.list("pods")
	if pok {
		r.pods(pods)
	}
	for _, kind := range []string{"deployments", "statefulsets", "daemonsets"} {
		if a, ok := r.list(kind); ok {
			r.workloads(kind, a)
		}
	}
	if a, ok := r.list("persistentvolumeclaims"); ok {
		count := 0
		for _, v := range a {
			if !r.scoped(v) {
				continue
			}
			count++
			s := "OK"
			if str(at(v, "status", "phase")) != "Bound" {
				s = "CRITICAL"
			}
			r.add(s, "PVC", id(v), "phase="+str(at(v, "status", "phase")))
		}
		r.report.Inventory["pvc_count"] = count
	}
	services, sok := r.list("services")
	slices, eok := r.list("endpointslices.discovery.k8s.io")
	if sok && eok {
		r.endpoints(services, slices)
	}
	if a, ok := r.list("xstores.polardbx.aliyun.com"); ok {
		r.xstores(a, pods, pok)
	}
	if a, ok := r.list("polardbxclusters.polardbx.aliyun.com"); ok {
		r.clusters(a)
	}
	if a, ok := r.list("cronjobs"); ok {
		r.backups(a)
	}
	if a, ok := r.list("events"); ok {
		r.events(a)
	}
	tasks := []func(){}
	if sok && eok {
		tasks = append(tasks, r.serviceTasks(services, slices)...)
	}
	if pok {
		tasks = append(tasks, r.networkTasks(pods)...)
	}
	for _, h := range r.cfg.HTTP {
		h := h
		if resolved, err := resolveServiceHTTP(h, services); err != nil {
			r.add("UNKNOWN", "HTTP", h.Name, err.Error())
			continue
		} else {
			h = resolved
		}
		tasks = append(tasks, func() { r.httpCheck(h) })
	}
	if nok && r.cfg.NodeProxy {
		for _, n := range nodes {
			n := n
			tasks = append(tasks, func() { r.nodeStats(n) })
		}
	}
	r.parallel(tasks)
	r.openAPI()
	if len(r.cfg.SQL) == 0 {
		r.add("UNKNOWN", "SQL", "业务数据库", "未配置数据库巡检账号；未验证 SQL、存储复制延迟和连接使用率。SSH root 不是数据库凭据")
	}
	for _, s := range r.cfg.SQL {
		r.sqlCheck(s)
	}
}
func defaults() Config {
	return Config{Namespaces: []string{"apsarabase", "dbstack-dashboard", "dbstack-dbaas", "dbstack-mybase", "dbstack-polardbx", "dbstack-redline", "monitoring", "mybase", "kube-system", "kube-flannel", "local-path-storage"}, TimeoutSeconds: 10, Workers: 6, RecentMinutes: 60, BackupMaxHours: 26, CPUWarn: 85, MemoryWarn: 85, DiskWarn: 80, DiskCritical: 90, NodeProxy: true}
}
func validate(c Config) error {
	if c.TimeoutSeconds < 1 || c.TimeoutSeconds > 120 || c.Workers < 1 || c.Workers > 32 || c.RecentMinutes < 1 || c.BackupMaxHours < 1 || len(c.Namespaces) == 0 {
		return errors.New("timeout/workers/时间窗口/namespace 配置无效")
	}
	if c.CPUWarn <= 0 || c.CPUWarn > 100 || c.MemoryWarn <= 0 || c.MemoryWarn > 100 || c.DiskWarn <= 0 || c.DiskCritical <= c.DiskWarn || c.DiskCritical > 100 {
		return errors.New("资源阈值无效")
	}
	for k, v := range c.ExpectedGroups {
		if k == "" || v < 1 {
			return errors.New("expected_groups 无效")
		}
	}
	for _, h := range c.HTTP {
		if e := validateHTTP(h); e != nil {
			return e
		}
	}
	return validateAPI(c.OpenAPI)
}
func main() { os.Exit(entry()) }
func entry() int {
	path := flag.String("config", "", "JSON 配置文件")
	out := flag.String("out", "reports", "报告目录")
	kubectl := flag.String("kubectl", "kubectl", "kubectl 路径")
	kc := flag.String("kubeconfig", "", "默认使用当前 kubeconfig")
	kctx := flag.String("context", "", "Kubernetes context")
	deadline := flag.Duration("deadline", 10*time.Minute, "整轮巡检最长时间")
	flag.Parse()
	c := defaults()
	if *path != "" {
		b, e := os.ReadFile(*path)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			return 3
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		if e = dec.Decode(&c); e != nil {
			fmt.Fprintln(os.Stderr, e)
			return 3
		}
	}
	if e := validate(c); e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 3
	}
	if *deadline <= 0 {
		fmt.Fprintln(os.Stderr, "deadline 必须大于零")
		return 3
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *deadline)
	defer cancel()
	host, _ := os.Hostname()
	r := &Inspector{cfg: c, ctx: ctx, kubectl: *kubectl, kubeconfig: *kc, kubecontext: *kctx, report: Report{Started: time.Now().UTC(), Host: host, Inventory: map[string]any{}}}
	r.run()
	if ctx.Err() != nil {
		r.add("UNKNOWN", "采集", "deadline", "巡检被中断或超时，报告不完整")
	}
	r.report.Finished = time.Now().UTC()
	r.report.Summary = map[string]int{}
	sort.Slice(r.report.Checks, func(i, j int) bool {
		a, b := r.report.Checks[i], r.report.Checks[j]
		return a.Category+"/"+a.Target+"/"+a.Detail < b.Category+"/"+b.Target+"/"+b.Detail
	})
	for _, c := range r.report.Checks {
		r.report.Summary[c.Status]++
	}
	if e := os.MkdirAll(*out, 0700); e != nil {
		fmt.Fprintln(os.Stderr, e)
		return 3
	}
	prefix := filepath.Join(*out, "inspection-"+r.report.Started.Format("20060102T150405.000000000Z"))
	b, _ := json.MarshalIndent(r.report, "", "  ")
	for _, f := range []struct {
		path string
		data []byte
	}{{prefix + ".json", b}, {prefix + ".md", []byte(markdown(r.report))}} {
		if e := writeExclusive(f.path, f.data); e != nil {
			fmt.Fprintln(os.Stderr, e)
			return 3
		}
	}
	fmt.Printf("OK=%d WARN=%d CRITICAL=%d UNKNOWN=%d\n%s.md\n%s.json\n", r.report.Summary["OK"], r.report.Summary["WARN"], r.report.Summary["CRITICAL"], r.report.Summary["UNKNOWN"], prefix, prefix)
	return reportExit(r.report.Summary)
}
func reportExit(s map[string]int) int {
	if s["CRITICAL"] > 0 {
		return 2
	}
	if s["UNKNOWN"] > 0 {
		return 3
	}
	if s["WARN"] > 0 {
		return 1
	}
	return 0
}
func writeExclusive(p string, b []byte) error {
	f, e := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	ce := f.Close()
	if e != nil {
		return e
	}
	return ce
}
func markdown(r Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# DBStack + PolarDB-X 巡检\n\n时间：%s 至 %s（UTC）；执行节点：%s。\n\nOK=%d，WARN=%d，CRITICAL=%d，UNKNOWN=%d。UNKNOWN 表示未验证，不能按正常处理。\n\n", r.Started.Format(time.RFC3339), r.Finished.Format(time.RFC3339), r.Host, r.Summary["OK"], r.Summary["WARN"], r.Summary["CRITICAL"], r.Summary["UNKNOWN"])
	for _, s := range []string{"CRITICAL", "UNKNOWN", "WARN", "OK"} {
		fmt.Fprintf(&b, "## %s\n\n| 分类 | 对象 | 结果 |\n|---|---|---|\n", s)
		for _, c := range r.Checks {
			if c.Status == s {
				fmt.Fprintf(&b, "| %s | %s | %s |\n", cell(c.Category), cell(c.Target), cell(c.Detail))
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}
func cell(s string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ", "\r", " ", "<", "&lt;", ">", "&gt;").Replace(s)
}
