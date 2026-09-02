package netchaos

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDropServerResponseAfterCommitTrigger(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var commits atomic.Int64
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
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
	p, err := Start(ctx, Config{Listen: "127.0.0.1:0", Upstream: ln.Addr().String(), Trigger: []byte("COMMIT"), Action: DropServerResponseOnce})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c, err := net.DialTimeout("tcp", p.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	if _, err := io.WriteString(c, "WRITE\n"); err != nil {
		t.Fatal(err)
	}
	if got, err := r.ReadString('\n'); err != nil || got != "OK\n" {
		t.Fatalf("pre-trigger response got=%q err=%v", got, err)
	}
	if _, err := io.WriteString(c, "COMMIT\n"); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if got, err := r.ReadString('\n'); err == nil {
		t.Fatalf("commit acknowledgement leaked through proxy: %q", got)
	}

	<-serverDone
	if commits.Load() != 1 {
		t.Fatalf("upstream commits=%d want 1", commits.Load())
	}
	deadline := time.Now().Add(time.Second)
	for p.Stats().DroppedBytes == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	st := p.Stats()
	if st.Triggered != 1 || st.DroppedBytes == 0 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestTriggerAcrossClientWrites(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// TCP does not preserve application write boundaries. Read until the
		// complete trigger payload has arrived so this test deterministically
		// verifies the proxy's cross-read trigger matching rather than relying
		// on the kernel coalescing the two client writes into one upstream read.
		buf := make([]byte, len("COMMIT"))
		if _, err := io.ReadFull(c, buf); err != nil {
			return
		}
		_, _ = io.WriteString(c, "ACK")
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := Start(ctx, Config{Upstream: ln.Addr().String(), Trigger: []byte("COMMIT"), Action: DropServerResponseOnce})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("COM"))
	_, _ = c.Write([]byte("MIT"))
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = c.Read(make([]byte, 8))
	deadline := time.Now().Add(time.Second)
	for p.Stats().Triggered == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.Stats().Triggered != 1 {
		t.Fatalf("trigger did not match across writes: %+v", p.Stats())
	}
}

func TestBlackholeAfterTriggerForcesClientDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
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
	p, err := Start(ctx, Config{Upstream: ln.Addr().String(), Trigger: []byte("PARTITION"), Action: BlackholeAfterTrigger})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	_, _ = io.WriteString(c, "HELLO\n")
	if got, e := r.ReadString('\n'); e != nil || got != "ACK:HELLO\n" {
		t.Fatalf("pre-partition got=%q err=%v", got, e)
	}
	_, _ = io.WriteString(c, "PARTITION\n")
	_ = c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if got, e := r.ReadString('\n'); e == nil {
		t.Fatalf("blackholed response leaked: %q", got)
	}
	if p.Stats().Triggered != 1 || p.Stats().DroppedBytes == 0 {
		t.Fatalf("stats=%+v", p.Stats())
	}
}
