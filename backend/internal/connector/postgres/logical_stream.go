package postgresconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"regexp"
	"strings"
	"time"
)

var replicationSlotName = regexp.MustCompile(`^[a-z0-9_]+$`)

type logicalStream struct {
	client *pgClient
	ackLSN uint64
}

func (s *logicalStream) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.close()
}

func parseReplicationLSN(s string) (uint64, error) {
	var hi, lo uint64
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%X/%X", &hi, &lo); err != nil {
		return 0, err
	}
	if hi > 0xffffffff || lo > 0xffffffff {
		return 0, errors.New("PostgreSQL LSN overflow")
	}
	return hi<<32 | lo, nil
}

func pgEpochMicros(t time.Time) int64 {
	// PostgreSQL replication timestamps are microseconds since 2000-01-01 UTC.
	return t.UTC().Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Microseconds()
}

func (s *logicalStream) sendStatus(ctx context.Context, lsn uint64) error {
	p := make([]byte, 1+8+8+8+8+1)
	p[0] = 'r'
	binary.BigEndian.PutUint64(p[1:9], lsn)
	binary.BigEndian.PutUint64(p[9:17], lsn)
	binary.BigEndian.PutUint64(p[17:25], lsn)
	binary.BigEndian.PutUint64(p[25:33], uint64(pgEpochMicros(time.Now())))
	p[33] = 0
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.client.conn.SetWriteDeadline(deadline)
	}
	defer s.client.conn.SetWriteDeadline(time.Time{})
	return s.client.writeMessage('d', p)
}

func (s *logicalStream) Next(ctx context.Context) ([]byte, error) {
	if s == nil || s.client == nil {
		return nil, io.EOF
	}
	for {
		typ, p, err := s.client.readMessage(ctx)
		if err != nil {
			return nil, err
		}
		switch typ {
		case 'd':
			if len(p) == 0 {
				continue
			}
			// Primary keepalive: 'k' + walEnd(8) + serverTime(8) + reply(1).
			if p[0] == 'k' && len(p) >= 18 && p[17] != 0 {
				_ = s.sendStatus(ctx, s.ackLSN)
			}
			return append([]byte(nil), p...), nil
		case 'c': // CopyDone
			return nil, io.EOF
		case 'E':
			return nil, parsePGError(p)
		case 'N', 'S':
			continue
		case 'Z':
			return nil, io.EOF
		}
	}
}

func (s *logicalStream) Acknowledge(ctx context.Context, lsn string) error {
	v, err := parseReplicationLSN(lsn)
	if err != nil {
		return err
	}
	if v < s.ackLSN {
		return nil
	}
	s.ackLSN = v
	return s.sendStatus(ctx, v)
}

func (c *Connector) OpenLogicalReplicationStream(ctx context.Context, slot, startLSN, publication string) (connector.PostgreSQLLogicalStream, error) {
	slot = strings.TrimSpace(slot)
	if !replicationSlotName.MatchString(slot) {
		return nil, errors.New("invalid PostgreSQL replication slot name")
	}
	startLSN = strings.TrimSpace(startLSN)
	if startLSN == "" {
		return nil, errors.New("start LSN is required")
	}
	publication = strings.TrimSpace(publication)
	if publication == "" {
		return nil, errors.New("publication name is required for pgoutput")
	}
	p, err := dialPGWithParams(ctx, c.ds, map[string]string{"replication": "database"})
	if err != nil {
		return nil, err
	}
	cmd := "START_REPLICATION SLOT " + slot + " LOGICAL " + startLSN + " (proto_version '1', publication_names '" + strings.ReplaceAll(publication, "'", "''") + "')"
	if err := p.writeMessage('Q', cstr(cmd)); err != nil {
		_ = p.close()
		return nil, err
	}
	for {
		typ, payload, err := p.readMessage(ctx)
		if err != nil {
			_ = p.close()
			return nil, err
		}
		switch typ {
		case 'W': // CopyBothResponse
			return &logicalStream{client: p}, nil
		case 'E':
			_ = p.close()
			return nil, parsePGError(payload)
		case 'N', 'S':
			continue
		default:
			_ = p.close()
			return nil, fmt.Errorf("unexpected PostgreSQL START_REPLICATION response %q", typ)
		}
	}
}

var _ connector.PostgreSQLLogicalSource = (*Connector)(nil)

func (c *Connector) EnsurePublication(ctx context.Context, name string, tables []string) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if !replicationSlotName.MatchString(name) {
		return errors.New("invalid PostgreSQL publication name")
	}
	if len(tables) == 0 {
		return errors.New("publication requires at least one table")
	}
	qualified := make([]string, 0, len(tables))
	for _, item := range tables {
		parts := strings.Split(strings.TrimSpace(item), ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("invalid publication table %q", item)
		}
		qualified = append(qualified, pgIdent(parts[0])+"."+pgIdent(parts[1]))
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	publicationCatalog := "pg_publication"
	if c.ds.Type == domain.DataSourceKingbase {
		publicationCatalog = "sys_publication"
	}
	r, err := p.query(ctx, "SELECT 1 FROM "+publicationCatalog+" WHERE pubname="+pgLiteral(name))
	if err != nil {
		return err
	}
	if len(r.rows) == 0 {
		return p.exec(ctx, "CREATE PUBLICATION "+pgIdent(name)+" FOR TABLE "+strings.Join(qualified, ","))
	}
	return p.exec(ctx, "ALTER PUBLICATION "+pgIdent(name)+" SET TABLE "+strings.Join(qualified, ","))
}

// DropPublication removes a QMigration-owned logical publication. It is used
// by qualification cleanup; production managed CDC publications are retained
// until task cleanup so reconnects keep a stable object identity.
func (c *Connector) DropPublication(ctx context.Context, name string) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || len(name) > 63 || !replicationSlotName.MatchString(name) {
		return errors.New("invalid publication name")
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	return p.exec(ctx, "DROP PUBLICATION IF EXISTS "+pgIdent(name))
}

// ValidateLogicalSlotPlugin proves that a durable logical slot uses the output
// plugin expected by the selected product. This prevents a PostgreSQL pgoutput
// slot or a Kingbase kboutput slot from being silently consumed by the wrong
// decoder after task edits/restarts.
func (c *Connector) ValidateLogicalSlotPlugin(ctx context.Context, slot, expectedPlugin string) error {
	slot = strings.ToLower(strings.TrimSpace(slot))
	expectedPlugin = strings.ToLower(strings.TrimSpace(expectedPlugin))
	if slot == "" || len(slot) > 63 || !replicationSlotName.MatchString(slot) {
		return errors.New("invalid logical replication slot name")
	}
	if expectedPlugin == "" || !replicationSlotName.MatchString(expectedPlugin) {
		return errors.New("invalid expected logical output plugin")
	}
	catalog := "pg_replication_slots"
	if c.ds.Type == domain.DataSourceKingbase {
		catalog = "sys_replication_slots"
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	r, err := p.query(ctx, "SELECT plugin FROM "+catalog+" WHERE slot_name="+pgLiteral(slot))
	if err != nil {
		return err
	}
	if len(r.rows) == 0 || len(r.rows[0]) == 0 {
		return fmt.Errorf("logical slot %s was not found in %s", slot, catalog)
	}
	actual := strings.ToLower(strings.TrimSpace(string(r.rows[0][0])))
	if actual != expectedPlugin {
		return fmt.Errorf("logical slot %s uses output plugin %s; expected %s", slot, actual, expectedPlugin)
	}
	return nil
}
