package postgresconnector

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMetadataTransactionRollbackAndUnknownCommit(t *testing.T) {
	for _, scenario := range []string{"success", "callback-error", "commit-response-lost"} {
		t.Run(scenario, func(t *testing.T) {
			var input []byte
			commands := 3
			if scenario == "commit-response-lost" {
				commands = 2
			}
			for i := 0; i < commands; i++ {
				input = append(input, pgTestFrame('C', []byte("OK\x00"))...)
				input = append(input, pgTestFrame('Z', []byte{'I'})...)
			}
			conn := &queryScriptConn{input: bytes.NewReader(input)}
			client := &pgClient{conn: conn, r: bufio.NewReader(conn)}
			c := &Connector{client: client}
			sentinel := errors.New("abort callback")
			err := c.InTransaction(context.Background(), func(tx *SQLTransaction) error {
				if err := tx.ExecSQL(context.Background(), "UPDATE fixture"); err != nil {
					return err
				}
				if scenario == "callback-error" {
					return sentinel
				}
				return nil
			})
			sent := conn.output.String()
			switch scenario {
			case "success":
				if err != nil || !strings.Contains(sent, "COMMIT") {
					t.Fatalf("commit failed: %v", err)
				}
			case "callback-error":
				if !errors.Is(err, sentinel) || !strings.Contains(sent, "ROLLBACK") {
					t.Fatalf("rollback failed: %v", err)
				}
			case "commit-response-lost":
				if err == nil || !strings.Contains(err.Error(), "outcome unknown") || strings.Contains(sent, "ROLLBACK") || !conn.closed {
					t.Fatalf("ambiguous COMMIT replayed or connection retained: %v", err)
				}
			}
		})
	}
}
