package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"qmigration/backend/internal/netchaos"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "listen address")
	upstream := flag.String("upstream", "", "upstream host:port")
	trigger := flag.String("trigger", "COMMIT", "client->server byte substring that arms the fault")
	triggerHex := flag.String("trigger-hex", "", "hex encoded trigger bytes (overrides --trigger)")
	action := flag.String("action", string(netchaos.DropServerResponseOnce), "DROP_SERVER_RESPONSE_ONCE, RESET_AFTER_TRIGGER or BLACKHOLE_AFTER_TRIGGER")
	flag.Parse()

	trig := []byte(*trigger)
	if strings.TrimSpace(*triggerHex) != "" {
		b, err := hex.DecodeString(strings.TrimSpace(*triggerHex))
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid --trigger-hex:", err)
			os.Exit(2)
		}
		trig = b
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	p, err := netchaos.Start(ctx, netchaos.Config{Listen: *listen, Upstream: *upstream, Trigger: trig, Action: netchaos.Action(strings.ToUpper(strings.TrimSpace(*action)))})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("QMigration netchaos listening=%s upstream=%s action=%s trigger_bytes=%d\n", p.Addr(), *upstream, strings.ToUpper(strings.TrimSpace(*action)), len(trig))
	<-ctx.Done()
	_ = p.Close()
	st := p.Stats()
	fmt.Printf("QMigration netchaos stopped connections=%d triggered=%d dropped_bytes=%d resets=%d\n", st.Connections, st.Triggered, st.DroppedBytes, st.Resets)
}
