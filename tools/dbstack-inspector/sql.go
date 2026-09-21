package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type SQLCheck struct {
	Name                  string  `json:"name"`
	Address               string  `json:"address"`
	UsernameEnv           string  `json:"username_env"`
	PasswordEnv           string  `json:"password_env"`
	TLS                   string  `json:"tls"`
	Role                  string  `json:"role"`
	ConnectionWarnPercent float64 `json:"connection_warn_percent"`
	LagWarnSeconds        float64 `json:"lag_warn_seconds"`
}

func (r *Inspector) sqlCheck(s SQLCheck) {
	user, pw := os.Getenv(s.UsernameEnv), os.Getenv(s.PasswordEnv)
	if user == "" || pw == "" {
		r.add("UNKNOWN", "SQL", s.Name, "未设置数据库账号/密码环境变量")
		return
	}
	if s.Role != "cn" && s.Role != "dn" {
		r.add("UNKNOWN", "SQL", s.Name, "role 必须为 cn 或 dn")
		return
	}
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = pw
	cfg.Net = "tcp"
	cfg.Addr = s.Address
	cfg.Timeout = r.timeout()
	cfg.ReadTimeout = r.timeout()
	cfg.WriteTimeout = r.timeout()
	cfg.TLSConfig = s.TLS
	cfg.MultiStatements = false
	cfg.AllowAllFiles = false
	connector, e := mysql.NewConnector(cfg)
	if e != nil {
		r.add("UNKNOWN", "SQL", s.Name, "连接配置无效")
		return
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Minute)
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout())
	var one int
	e = db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	cancel()
	if e != nil || one != 1 {
		r.add("UNKNOWN", "SQL", s.Name, "SELECT 1 失败，检查连接、数据库账号和 TLS（未输出凭据）")
		return
	}
	r.add("OK", "SQL", s.Name, "SELECT 1 成功")
	if s.TLS == "" || s.TLS == "false" || s.TLS == "skip-verify" || s.TLS == "preferred" {
		r.add("WARN", "SQL", s.Name+"/TLS", "未要求严格验证的 TLS")
	}
	rows, e := r.query(db, "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Uptime','Slow_queries','Innodb_buffer_pool_wait_free')")
	if e != nil {
		r.add("UNKNOWN", "SQL", s.Name+"/status", "无法读取全局状态")
	} else {
		m := map[string]string{}
		for _, row := range rows {
			m[strings.ToLower(row["variable_name"])] = row["value"]
		}
		r.add("OK", "SQL", s.Name+"/status", fmt.Sprintf("Threads_connected=%s Threads_running=%s Uptime=%s Slow_queries累计=%s", m["threads_connected"], m["threads_running"], m["uptime"], m["slow_queries"]))
		vars, e := r.query(db, "SHOW GLOBAL VARIABLES LIKE 'max_connections'")
		if e != nil || len(vars) == 0 {
			r.add("UNKNOWN", "SQL", s.Name+"/connections", "无法读取 max_connections")
		} else {
			max, _ := strconv.ParseFloat(vars[0]["value"], 64)
			cur, pe := strconv.ParseFloat(m["threads_connected"], 64)
			if max > 0 && pe == nil {
				pct := 100 * cur / max
				warn := s.ConnectionWarnPercent
				if warn <= 0 {
					warn = 80
				}
				state := "OK"
				if pct >= warn {
					state = "WARN"
				}
				r.add(state, "SQL", s.Name+"/connections", fmt.Sprintf("连接占用 %.1f%%", pct))
			} else {
				r.add("UNKNOWN", "SQL", s.Name+"/connections", "连接指标缺失或非法")
			}
		}
	}
	if s.Role == "cn" {
		rows, e := r.query(db, "SHOW STORAGE")
		if e != nil {
			r.add("UNKNOWN", "SQL存储", s.Name, "SHOW STORAGE 不支持或权限不足")
			return
		}
		if len(rows) == 0 {
			r.add("UNKNOWN", "SQL存储", s.Name, "SHOW STORAGE 返回零行")
		}
		for _, row := range rows {
			target := s.Name + "/" + row["storage_inst_id"]
			healthy, exists := row["is_healthy"]
			status := "UNKNOWN"
			if exists {
				if strings.EqualFold(healthy, "true") || healthy == "1" {
					status = "OK"
				} else {
					status = "CRITICAL"
				}
			}
			detail := "IS_HEALTHY=" + healthy
			if lag, ok := row["delay"]; ok {
				v, e := strconv.ParseFloat(lag, 64)
				warn := s.LagWarnSeconds
				if warn <= 0 {
					warn = 30
				}
				if e != nil {
					if status == "OK" {
						status = "UNKNOWN"
					}
				} else if v > warn && status == "OK" {
					status = "WARN"
				}
				detail += " DELAY=" + lag
			}
			r.add(status, "SQL存储", target, detail)
		}
	} else {
		r.add("UNKNOWN", "SQL复制", s.Name, "DN Paxos 延迟需要本版系统表/DBStack DescribeDnInstanceHA 验证；未用传统主从状态替代")
	}
}
func (r *Inspector) query(db *sql.DB, q string) ([]map[string]string, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout())
	defer cancel()
	rows, e := db.QueryContext(ctx, q)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	cols, e := rows.Columns()
	if e != nil {
		return nil, e
	}
	out := []map[string]string{}
	for rows.Next() {
		if len(out) >= 1000 {
			return nil, fmt.Errorf("row limit exceeded")
		}
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if e := rows.Scan(ptrs...); e != nil {
			return nil, e
		}
		m := map[string]string{}
		for i, k := range cols {
			if vals[i].Valid {
				m[strings.ToLower(k)] = vals[i].String
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
