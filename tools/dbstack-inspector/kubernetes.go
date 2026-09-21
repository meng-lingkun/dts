package main

import (
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (r *Inspector) nodes(nodes []any) {
	inventory := []Obj{}
	if len(nodes) == 0 {
		r.add("UNKNOWN", "节点", "all", "未发现节点")
	}
	for _, n := range nodes {
		inventory = append(inventory, Obj{"name": name(n), "addresses": at(n, "status", "addresses"), "version": at(n, "status", "nodeInfo", "kubeletVersion"), "os": at(n, "status", "nodeInfo", "osImage"), "allocatable": at(n, "status", "allocatable")})
		s := "OK"
		issues := []string{}
		if condition(n, "Ready") != "True" {
			s = "CRITICAL"
			issues = append(issues, "NodeReady="+condition(n, "Ready"))
		}
		for _, k := range []string{"MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"} {
			if condition(n, k) == "True" {
				s = "CRITICAL"
				issues = append(issues, k)
			}
		}
		if at(n, "spec", "unschedulable") == true {
			if s == "OK" {
				s = "WARN"
			}
			issues = append(issues, "已禁止调度")
		}
		if len(issues) == 0 {
			issues = append(issues, "Ready，无资源压力条件")
		}
		r.add(s, "节点", name(n), strings.Join(issues, "; "))
	}
	r.report.Inventory["nodes"] = inventory
}
func podReady(p any) bool {
	return at(p, "metadata", "deletionTimestamp") == nil && str(at(p, "status", "phase")) == "Running" && condition(p, "Ready") == "True"
}
func dbGroup(p any) string { return str(at(p, "metadata", "labels", "apsara.metric.replicaset_name")) }
func (r *Inspector) pods(pods []any) {
	groups := map[string][]any{}
	inventory := []Obj{}
	counts := map[string]int{}
	now := time.Now()
	cut := now.Add(-time.Duration(r.cfg.RecentMinutes) * time.Minute)
	for _, p := range pods {
		if !r.scoped(p) {
			continue
		}
		counts[ns(p)]++
		phase := str(at(p, "status", "phase"))
		if phase == "Succeeded" {
			continue
		}
		if g := dbGroup(p); g != "" {
			groups[g] = append(groups[g], p)
			inventory = append(inventory, Obj{"pod": id(p), "group": g, "instance": at(p, "metadata", "labels", "apsara.cluster_ins.name"), "db_type": at(p, "metadata", "labels", "apsara.metric.db_type"), "node": at(p, "spec", "nodeName"), "ip": at(p, "status", "podIP"), "port": at(p, "metadata", "labels", "apsara.ins.port"), "ready": podReady(p)})
		}
		s := "OK"
		notes := []string{"phase=" + phase}
		if !podReady(p) {
			s = "CRITICAL"
			notes = append(notes, "Pod 未就绪")
		}
		if at(p, "metadata", "deletionTimestamp") != nil {
			s = "WARN"
			notes = append(notes, "Terminating")
		}
		statuses := append(append([]any{}, arr(at(p, "status", "initContainerStatuses"))...), arr(at(p, "status", "containerStatuses"))...)
		for _, c := range statuses {
			cn := str(at(c, "name"))
			if reason := str(at(c, "state", "waiting", "reason")); reason != "" {
				notes = append(notes, cn+":"+reason)
			}
			t := stamp(at(c, "lastState", "terminated", "finishedAt"))
			restarts := int(num(at(c, "restartCount")))
			if restarts > 0 {
				notes = append(notes, fmt.Sprintf("%s 累计重启=%d", cn, restarts))
				if t.After(cut) {
					if s == "OK" {
						s = "WARN"
					}
					notes = append(notes, "近期重启:"+str(at(c, "lastState", "terminated", "reason")))
				}
			}
		}
		// Newly created periodic Job pods routinely spend a short time Pending.
		created := stamp(at(p, "metadata", "creationTimestamp"))
		if phase == "Pending" && !created.IsZero() && now.Sub(created) >= 0 && now.Sub(created) < 5*time.Minute {
			startup := true
			for _, c := range statuses {
				reason := str(at(c, "state", "waiting", "reason"))
				if reason != "" && reason != "ContainerCreating" && reason != "PodInitializing" {
					startup = false
				}
			}
			if startup {
				s = "WARN"
				notes = append(notes, "创建不足 5 分钟，处于启动宽限期")
			}
		}
		r.add(s, "Pod", id(p), strings.Join(notes, "; "))
	}
	for _, n := range r.cfg.Namespaces {
		if counts[n] == 0 {
			r.add("WARN", "覆盖", n, "命名空间没有 Pod 或已不存在")
		}
	}
	for g, want := range r.cfg.ExpectedGroups {
		if _, ok := groups[g]; !ok {
			r.add("CRITICAL", "实例副本", g, fmt.Sprintf("预期 %d 个 Pod，实际 0", want))
		}
	}
	for g, ps := range groups {
		ready := 0
		hosts := map[string]bool{}
		for _, p := range ps {
			if podReady(p) {
				ready++
			}
			if n := str(at(p, "spec", "nodeName")); n != "" {
				hosts[n] = true
			}
		}
		s := "OK"
		want, known := r.cfg.ExpectedGroups[g]
		note := fmt.Sprintf("Pod=%d，Ready=%d，分布节点=%d", len(ps), ready, len(hosts))
		if !known {
			s = "UNKNOWN"
			note += "；缺少期望副本基线"
		} else {
			note += fmt.Sprintf("，副本基线=%d", want)
			if ready < want || len(ps) < want {
				s = "CRITICAL"
			} else if len(ps) > want {
				s = "WARN"
			}
		}
		if len(ps) > 1 && len(hosts) < len(ps) {
			if s == "OK" {
				s = "WARN"
			}
			note += "；存在副本共节点"
		}
		if strings.HasPrefix(g, "pxc-xdb-") && len(ps) < 3 {
			if s == "OK" {
				s = "WARN"
			}
			note += "；存储组少于 3 个可见 Pod，需核对设计副本数和 Paxos 成员"
		}
		r.add(s, "实例副本", g, note)
	}
	r.report.Inventory["database_pods"] = inventory
	r.report.Inventory["namespace_pod_counts"] = counts
}
func (r *Inspector) workloads(kind string, items []any) {
	for _, v := range items {
		if !r.scoped(v) {
			continue
		}
		desired := num(at(v, "spec", "replicas"))
		if at(v, "spec", "replicas") == nil {
			desired = 1
		}
		ready := num(at(v, "status", "readyReplicas"))
		updated := num(at(v, "status", "updatedReplicas"))
		if kind == "daemonsets" {
			desired = num(at(v, "status", "desiredNumberScheduled"))
			ready = num(at(v, "status", "numberReady"))
			updated = num(at(v, "status", "updatedNumberScheduled"))
		}
		s := "OK"
		if ready < desired {
			s = "CRITICAL"
		} else if updated < desired || num(at(v, "status", "observedGeneration")) < num(at(v, "metadata", "generation")) {
			s = "WARN"
		}
		r.add(s, "工作负载", kind+"/"+id(v), fmt.Sprintf("desired=%.0f ready=%.0f updated=%.0f", desired, ready, updated))
	}
}
func (r *Inspector) endpoints(services, slices []any) {
	ready := map[string]int{}
	for _, v := range slices {
		key := ns(v) + "/" + str(at(v, "metadata", "labels", "kubernetes.io/service-name"))
		for _, e := range arr(at(v, "endpoints")) { // nil ready means unknown/ready per EndpointSlice semantics; terminating is not usable.
			if at(e, "conditions", "ready") != false && at(e, "conditions", "terminating") != true {
				ready[key] += len(arr(at(e, "addresses")))
			}
		}
	}
	inventory := []Obj{}
	for _, v := range services {
		if !r.scoped(v) {
			continue
		}
		inventory = append(inventory, Obj{"service": id(v), "type": at(v, "spec", "type"), "ip": at(v, "spec", "clusterIP"), "external_ips": at(v, "spec", "externalIPs"), "ports": at(v, "spec", "ports"), "ready_addresses": ready[id(v)]})
		if str(at(v, "spec", "type")) == "ExternalName" {
			continue
		}
		if ready[id(v)] == 0 {
			if len(obj(at(v, "spec", "selector"))) == 0 && str(at(v, "spec", "clusterIP")) == "None" {
				r.add("UNKNOWN", "Service", id(v), "无 EndpointSlice；无 selector 的外部入口需结合数据库连接探测确认")
			} else {
				r.add("CRITICAL", "Service", id(v), "无可用 EndpointSlice 地址")
			}
		} else {
			r.add("OK", "Service", id(v), fmt.Sprintf("可用后端地址=%d", ready[id(v)]))
		}
	}
	r.report.Inventory["services"] = inventory
}
func (r *Inspector) xstores(items, pods []any, podsOK bool) {
	count := 0
	for _, v := range items {
		if !r.scoped(v) {
			continue
		}
		count++
		s := "OK"
		phase := str(at(v, "status", "phase"))
		leader := str(at(v, "status", "leaderPod"))
		ready, total := num(at(v, "status", "readyPods")), num(at(v, "status", "totalPods"))
		if phase != "Running" || leader == "" || total == 0 || ready < total {
			s = "CRITICAL"
		}
		if s == "OK" && num(at(v, "status", "observedGeneration")) < num(at(v, "metadata", "generation")) {
			s = "WARN"
		}
		if podsOK {
			found := false
			for _, p := range pods {
				if name(p) == leader && ns(p) == ns(v) && podReady(p) {
					found = true
				}
			}
			if !found {
				s = "CRITICAL"
			}
		}
		r.add(s, "XStore", id(v), fmt.Sprintf("phase=%s ready=%.0f/%.0f leader=%s engine=%s", phase, ready, total, leader, str(at(v, "status", "engineVersion"))))
	}
	r.report.Inventory["xstore_count"] = count
}
func (r *Inspector) clusters(items []any) {
	count := 0
	for _, v := range items {
		if !r.scoped(v) {
			continue
		}
		count++
		s := "OK"
		phase := str(at(v, "status", "phase"))
		if phase != "Running" {
			s = "CRITICAL"
		}
		r.add(s, "PolarDBXCluster", id(v), "phase="+phase)
	}
	r.report.Inventory["polardbxcluster_count"] = count
	if count == 0 {
		r.add("OK", "架构", "PolarDBXCluster", "未发现 CR；业务实例另按 DBStack Pod 标签和副本基线检查")
	}
}
func (r *Inspector) backups(items []any) {
	count := 0
	for _, v := range items {
		if !r.scoped(v) || !strings.Contains(strings.ToLower(name(v)), "backup") {
			continue
		}
		count++
		s := "OK"
		last := stamp(at(v, "status", "lastSuccessfulTime"))
		d := "lastSuccess=" + str(at(v, "status", "lastSuccessfulTime")) + " schedule=" + str(at(v, "spec", "schedule"))
		if last.IsZero() || time.Since(last) > time.Duration(r.cfg.BackupMaxHours)*time.Hour {
			s = "WARN"
			d += "；备份调度最近成功时间缺失或超过阈值"
		}
		if at(v, "spec", "suspend") == true {
			s = "WARN"
			d += "；已暂停"
		}
		r.add(s, "备份调度", id(v), d)
	}
	if count == 0 {
		r.add("UNKNOWN", "备份调度", "CronJob", "未发现备份 CronJob；不能据此判断 DBStack 业务备份状态")
	}
	r.add("UNKNOWN", "业务备份", "恢复有效性", "CronJob 成功不代表业务备份可恢复；需备份集状态、时间及独立恢复演练证据")
}
func (r *Inspector) events(items []any) {
	cut := time.Now().Add(-time.Duration(r.cfg.RecentMinutes) * time.Minute)
	count := 0
	for _, v := range items {
		if !r.scoped(v) || str(at(v, "type")) != "Warning" {
			continue
		}
		t := stamp(at(v, "lastTimestamp"))
		for _, x := range []any{at(v, "eventTime"), at(v, "series", "lastObservedTime")} {
			if p := stamp(x); p.After(t) {
				t = p
			}
		}
		if t.IsZero() {
			t = stamp(at(v, "metadata", "creationTimestamp"))
		}
		if t.Before(cut) {
			continue
		}
		count++
		r.add("WARN", "近期事件", ns(v)+"/"+str(at(v, "involvedObject", "name")), str(at(v, "reason"))+" count="+str(at(v, "count"))+" last="+t.Format(time.RFC3339))
	}
	if count == 0 {
		r.add("OK", "近期事件", "Warning", fmt.Sprintf("最近 %d 分钟无 Warning 事件", r.cfg.RecentMinutes))
	}
}

func (r *Inspector) nodeStats(node any) {
	d, e := r.raw("/api/v1/nodes/" + url.PathEscape(name(node)) + "/proxy/stats/summary")
	if e != nil {
		r.add("UNKNOWN", "节点资源", name(node), e.Error())
		return
	}
	// Kubelet summary covers host and mounted volumes without SSH to additional nodes.
	n := at(d, "node")
	for _, metric := range []struct {
		key             string
		value, capacity any
		warn            float64
	}{{"CPU", at(n, "cpu", "usageNanoCores"), at(node, "status", "allocatable", "cpu"), r.cfg.CPUWarn}, {"内存", at(n, "memory", "workingSetBytes"), at(node, "status", "allocatable", "memory"), r.cfg.MemoryWarn}} {
		cap, ok := quantity(str(metric.capacity))
		val := num(metric.value)
		if metric.key == "CPU" {
			val /= 1e9
		}
		if !ok || cap <= 0 || metric.value == nil {
			r.add("UNKNOWN", "节点资源", name(node)+"/"+metric.key, "采样缺失")
			continue
		}
		pct := 100 * val / cap
		s := "OK"
		if pct >= metric.warn {
			s = "WARN"
		}
		r.add(s, "节点资源", name(node)+"/"+metric.key, fmt.Sprintf("瞬时占 allocatable %.1f%%", pct))
	}
	r.disk(name(node)+"/rootfs", at(n, "fs"))
	r.disk(name(node)+"/imagefs", at(n, "runtime", "imageFs"))
	disks := map[string]any{}
	for _, p := range arr(at(d, "pods")) {
		if !r.scoped(Obj{"metadata": Obj{"namespace": at(p, "podRef", "namespace")}}) {
			continue
		}
		for _, v := range arr(at(p, "volume")) {
			key := str(at(p, "podRef", "namespace")) + "/" + str(at(p, "podRef", "name")) + "/" + str(at(v, "name"))
			if at(v, "capacityBytes") != nil {
				disks[key] = v
			}
		}
	}
	keys := []string{}
	for k := range disks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r.disk(name(node)+"/"+k, disks[k])
	}
	r.add("UNKNOWN", "磁盘覆盖", name(node), "kubelet summary 不保证暴露所有 hostPath 数据盘；未返回的挂载点无法判定容量")
}
func (r *Inspector) disk(target string, v any) {
	cap, avail := num(at(v, "capacityBytes")), at(v, "availableBytes")
	if cap <= 0 || avail == nil {
		r.add("UNKNOWN", "磁盘", target, "容量指标缺失")
		return
	}
	pct := 100 * (cap - num(avail)) / cap
	s := "OK"
	if pct >= r.cfg.DiskCritical {
		s = "CRITICAL"
	} else if pct >= r.cfg.DiskWarn {
		s = "WARN"
	}
	d := fmt.Sprintf("空间使用=%.1f%%，可用=%.2f GiB", pct, num(avail)/(1<<30))
	if ino := num(at(v, "inodes")); ino > 0 && at(v, "inodesFree") != nil {
		p := 100 * (ino - num(at(v, "inodesFree"))) / ino
		d += fmt.Sprintf("，inode使用=%.1f%%", p)
		if p >= r.cfg.DiskCritical {
			s = "CRITICAL"
		} else if p >= r.cfg.DiskWarn && s == "OK" {
			s = "WARN"
		}
	}
	r.add(s, "磁盘", target, d)
}
func quantity(s string) (float64, bool) {
	for _, u := range []struct {
		s string
		v float64
	}{{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"Pi", 1 << 50}, {"n", 1e-9}, {"u", 1e-6}, {"m", 1e-3}, {"k", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12}} {
		if strings.HasSuffix(s, u.s) {
			n, e := strconv.ParseFloat(strings.TrimSuffix(s, u.s), 64)
			return n * u.v, e == nil && !math.IsNaN(n) && !math.IsInf(n, 0)
		}
	}
	n, e := strconv.ParseFloat(s, 64)
	return n, e == nil && !math.IsNaN(n) && !math.IsInf(n, 0)
}
