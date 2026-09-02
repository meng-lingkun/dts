// Package netchaos provides an opt-in TCP fault proxy for QMigration
// qualification. It is deliberately protocol-neutral: rules are triggered by
// byte sequences observed on the client->server stream, then act on the TCP
// connection itself. This makes it useful for MySQL COM_QUERY, PostgreSQL
// Query, and synthetic qualification traffic without embedding database
// protocol semantics in the proxy.
package netchaos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Action string

const (
	// DropServerResponseOnce forwards the triggering client request to the
	// upstream server, discards the first subsequent server response bytes,
	// then closes both sides. This models the classic COMMIT-result-unknown
	// window: the database may have committed while the client never received
	// the acknowledgement.
	DropServerResponseOnce Action = "DROP_SERVER_RESPONSE_ONCE"
	// ResetAfterTrigger forwards the triggering client bytes, then closes both
	// sockets immediately. It is useful for generic network-partition tests.
	ResetAfterTrigger Action = "RESET_AFTER_TRIGGER"
	// BlackholeAfterTrigger forwards the triggering request, then silently
	// discards traffic in both directions until the peer or context closes.
	BlackholeAfterTrigger Action = "BLACKHOLE_AFTER_TRIGGER"
)

type Config struct {
	Listen      string
	Upstream    string
	Trigger     []byte
	Action      Action
	DialTimeout time.Duration
}

type Stats struct {
	Connections  int64 `json:"connections"`
	Triggered    int64 `json:"triggered"`
	DroppedBytes int64 `json:"dropped_bytes"`
	Resets       int64 `json:"resets"`
}

type Proxy struct {
	cfg Config
	ln  net.Listener

	connections  atomic.Int64
	triggered    atomic.Int64
	droppedBytes atomic.Int64
	resets       atomic.Int64

	closeOnce sync.Once
}

func normalize(c Config) (Config, error) {
	c.Listen = strings.TrimSpace(c.Listen)
	c.Upstream = strings.TrimSpace(c.Upstream)
	if c.Listen == "" {
		c.Listen = "127.0.0.1:0"
	}
	if c.Upstream == "" {
		return c, errors.New("netchaos upstream is required")
	}
	if len(c.Trigger) == 0 {
		return c, errors.New("netchaos trigger is required")
	}
	if c.Action == "" {
		c.Action = DropServerResponseOnce
	}
	switch c.Action {
	case DropServerResponseOnce, ResetAfterTrigger, BlackholeAfterTrigger:
	default:
		return c, fmt.Errorf("unsupported netchaos action %q", c.Action)
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	return c, nil
}

// Start opens the listening socket before returning, so callers can safely use
// Addr immediately (including listen=:0 qualification scenarios).
func Start(ctx context.Context, cfg Config) (*Proxy, error) {
	cfg, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, ln: ln}
	go p.serve(ctx)
	return p, nil
}

func (p *Proxy) Addr() string {
	if p == nil || p.ln == nil {
		return ""
	}
	return p.ln.Addr().String()
}

func (p *Proxy) Stats() Stats {
	if p == nil {
		return Stats{}
	}
	return Stats{
		Connections:  p.connections.Load(),
		Triggered:    p.triggered.Load(),
		DroppedBytes: p.droppedBytes.Load(),
		Resets:       p.resets.Load(),
	}
}

func (p *Proxy) Close() error {
	if p == nil || p.ln == nil {
		return nil
	}
	var err error
	p.closeOnce.Do(func() { err = p.ln.Close() })
	return err
}

func (p *Proxy) serve(ctx context.Context) {
	go func() {
		<-ctx.Done()
		_ = p.Close()
	}()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		p.connections.Add(1)
		go p.handle(ctx, c)
	}
}

func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	d := net.Dialer{Timeout: p.cfg.DialTimeout}
	upstream, err := d.DialContext(ctx, "tcp", p.cfg.Upstream)
	if err != nil {
		_ = client.Close()
		return
	}

	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			_ = client.Close()
			_ = upstream.Close()
		})
	}
	defer closeBoth()

	var armed atomic.Bool
	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		tail := make([]byte, 0, len(p.cfg.Trigger)-1)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				if armed.Load() && p.cfg.Action == BlackholeAfterTrigger {
					p.droppedBytes.Add(int64(n))
					if err != nil {
						return
					}
					continue
				}

				// Detect and arm before forwarding the bytes that complete the
				// trigger. Otherwise a fast upstream can reply between Write and
				// armed.Store, leaking the very response the fault proxy is meant
				// to drop. This ordering also works when the trigger spans reads:
				// earlier prefix bytes may already be upstream, but the server cannot
				// observe the complete trigger until this final chunk is forwarded.
				triggeredNow := false
				if !armed.Load() {
					probe := append(append([]byte(nil), tail...), chunk...)
					if bytes.Contains(probe, p.cfg.Trigger) {
						armed.Store(true)
						p.triggered.Add(1)
						triggeredNow = true
					}
					if !triggeredNow {
						keep := len(p.cfg.Trigger) - 1
						if keep > 0 {
							if len(probe) > keep {
								probe = probe[len(probe)-keep:]
							}
							tail = append(tail[:0], probe...)
						}
					}
				}

				if _, werr := upstream.Write(chunk); werr != nil {
					return
				}
				if triggeredNow && p.cfg.Action == ResetAfterTrigger {
					p.resets.Add(1)
					closeBoth()
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		for {
			n, err := upstream.Read(buf)
			if n > 0 {
				if armed.Load() && p.cfg.Action == BlackholeAfterTrigger {
					p.droppedBytes.Add(int64(n))
					if err != nil {
						return
					}
					continue
				}
				if armed.Load() && p.cfg.Action == DropServerResponseOnce {
					p.droppedBytes.Add(int64(n))
					p.resets.Add(1)
					closeBoth()
					return
				}
				if _, werr := client.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					// Connection errors are expected in a fault proxy. The
					// client observes the TCP failure; no separate side channel
					// is needed here.
				}
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		closeBoth()
		<-done
	case <-done:
		closeBoth()
	}
}
