package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"qmigration/backend/internal/netchaos"
)

func networkChaosCheck() check {
	return runCheck("tcp-commit-response-drop", func() (map[string]any, error) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		defer ln.Close()
		var commits atomic.Int64
		done := make(chan struct{})
		go func() {
			defer close(done)
			c, e := ln.Accept()
			if e != nil {
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				line, e := r.ReadString('\n')
				if e != nil {
					return
				}
				if strings.TrimSpace(line) == "COMMIT" {
					commits.Add(1)
					_, _ = io.WriteString(c, "COMMIT_OK\n")
					return
				}
				_, _ = io.WriteString(c, "OK\n")
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p, err := netchaos.Start(ctx, netchaos.Config{Upstream: ln.Addr().String(), Trigger: []byte("COMMIT"), Action: netchaos.DropServerResponseOnce})
		if err != nil {
			return nil, err
		}
		defer p.Close()
		c, err := net.DialTimeout("tcp", p.Addr(), time.Second)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		r := bufio.NewReader(c)
		_, _ = io.WriteString(c, "WRITE\n")
		if got, e := r.ReadString('\n'); e != nil || got != "OK\n" {
			return nil, fmt.Errorf("pre-commit proxy response got=%q err=%v", got, e)
		}
		_, _ = io.WriteString(c, "COMMIT\n")
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if got, e := r.ReadString('\n'); e == nil {
			return nil, fmt.Errorf("commit response was not dropped: %q", got)
		}
		<-done
		st := p.Stats()
		if commits.Load() != 1 || st.Triggered != 1 || st.DroppedBytes == 0 {
			return nil, fmt.Errorf("network chaos mismatch commits=%d stats=%+v", commits.Load(), st)
		}
		return map[string]any{"upstream_commits": commits.Load(), "triggered": st.Triggered, "dropped_response_bytes": st.DroppedBytes}, nil
	})
}

func networkBlackholeCheck() check {
	return runCheck("tcp-network-blackhole", func() (map[string]any, error) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		defer ln.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			c, e := ln.Accept()
			if e != nil {
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				line, e := r.ReadString('\n')
				if e != nil {
					return
				}
				_, _ = io.WriteString(c, "ACK:"+line)
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p, err := netchaos.Start(ctx, netchaos.Config{Upstream: ln.Addr().String(), Trigger: []byte("PARTITION"), Action: netchaos.BlackholeAfterTrigger})
		if err != nil {
			return nil, err
		}
		defer p.Close()
		c, err := net.DialTimeout("tcp", p.Addr(), time.Second)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		r := bufio.NewReader(c)
		_, _ = io.WriteString(c, "HELLO\n")
		if got, e := r.ReadString('\n'); e != nil || got != "ACK:HELLO\n" {
			return nil, fmt.Errorf("pre-blackhole response got=%q err=%v", got, e)
		}
		_, _ = io.WriteString(c, "PARTITION\n")
		_ = c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if got, e := r.ReadString('\n'); e == nil {
			return nil, fmt.Errorf("blackholed response leaked: %q", got)
		}
		st := p.Stats()
		if st.Triggered != 1 || st.DroppedBytes == 0 {
			return nil, fmt.Errorf("blackhole stats mismatch: %+v", st)
		}
		return map[string]any{"triggered": st.Triggered, "dropped_bytes": st.DroppedBytes, "client_deadline_observed": true}, nil
	})
}
