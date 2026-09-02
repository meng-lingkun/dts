package mysqlconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"qmigration/backend/internal/cdc/mysqlbinlog"
	"qmigration/backend/internal/connector"
)

const (
	comBinlogDump     byte   = 0x12
	comBinlogDumpGTID byte   = 0x1e
	binlogThroughGTID uint16 = 0x04
)

type binlogStream struct {
	client *protocolClient
}

func (s *binlogStream) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.close()
}

func (s *binlogStream) Next(ctx context.Context) ([]byte, error) {
	if s == nil || s.client == nil {
		return nil, io.EOF
	}
	if err := s.client.setDeadline(ctx); err != nil {
		return nil, err
	}
	defer s.client.clearDeadline()
	pkt, err := s.client.readPacket()
	if err != nil {
		return nil, err
	}
	if len(pkt) == 0 {
		return nil, errors.New("empty mysql binlog stream packet")
	}
	switch pkt[0] {
	case 0x00:
		if len(pkt) == 1 {
			return nil, errors.New("empty mysql binlog event")
		}
		return append([]byte(nil), pkt[1:]...), nil
	case 0xff:
		return nil, parseErrorPacket(pkt)
	case 0xfe:
		if len(pkt) < 9 {
			return nil, io.EOF
		}
	}
	return nil, fmt.Errorf("unexpected mysql binlog stream packet header 0x%x", pkt[0])
}

func (c *Connector) OpenBinlogStream(ctx context.Context, file string, position uint32, serverID uint32) (connector.RawCDCStream, error) {
	if file == "" {
		return nil, errors.New("binlog file is required")
	}
	if position < 4 {
		position = 4
	}
	if serverID == 0 {
		serverID = 100000 + uint32(len(file))
	}
	// A dedicated protocol session is mandatory: COM_BINLOG_DUMP turns the
	// connection into a replication stream and it cannot serve normal queries.
	p, err := dialProtocol(ctx, c.ds)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (connector.RawCDCStream, error) { _ = p.close(); return nil, e }
	// Ask the server to omit trailing CRC32 bytes. This keeps the native parser
	// deterministic; production deployments can later enable checksum verify mode.
	if _, err := p.exec(ctx, "SET @master_binlog_checksum='NONE'"); err != nil {
		// Some compatible databases do not expose this session variable. Continue:
		// the parser can be configured with a 4-byte checksum when needed.
	}
	payload := make([]byte, 1+4+2+4+len(file))
	payload[0] = comBinlogDump
	binary.LittleEndian.PutUint32(payload[1:5], position)
	binary.LittleEndian.PutUint16(payload[5:7], 0) // BINLOG_DUMP_NEVER_STOP
	binary.LittleEndian.PutUint32(payload[7:11], serverID)
	copy(payload[11:], file)
	if err := p.setDeadline(ctx); err != nil {
		return fail(err)
	}
	p.seq = 0
	if err := p.writePacket(payload); err != nil {
		p.clearDeadline()
		return fail(err)
	}
	p.clearDeadline()
	return &binlogStream{client: p}, nil
}

// OpenBinlogGTIDStream starts a native MySQL replication stream from a durable
// executed-GTID set. The server sends only transactions not covered by the SID
// block, making process/worker restart recovery independent of binlog rotation.
func (c *Connector) OpenBinlogGTIDStream(ctx context.Context, gtidSet string, serverID uint32) (connector.RawCDCStream, error) {
	set, err := mysqlbinlog.ParseGTIDSet(gtidSet)
	if err != nil {
		return nil, fmt.Errorf("parse MySQL GTID set: %w", err)
	}
	sidBlock, err := set.EncodeSIDBlock()
	if err != nil {
		return nil, fmt.Errorf("encode MySQL GTID SID block: %w", err)
	}
	if serverID == 0 {
		serverID = 100001
	}
	p, err := dialProtocol(ctx, c.ds)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (connector.RawCDCStream, error) { _ = p.close(); return nil, e }
	if _, err := p.exec(ctx, "SET @master_binlog_checksum='NONE'"); err != nil {
		// Compatible servers may not expose the variable; streaming can still work.
	}
	// COM_BINLOG_DUMP_GTID payload:
	// command, flags, server-id, filename-len, filename, position(uint64),
	// sid-block-len(uint32), sid-block. An empty filename + position 4 is valid
	// when BINLOG_THROUGH_GTID is set.
	payload := make([]byte, 1+2+4+4+8+4+len(sidBlock))
	payload[0] = comBinlogDumpGTID
	binary.LittleEndian.PutUint16(payload[1:3], binlogThroughGTID)
	binary.LittleEndian.PutUint32(payload[3:7], serverID)
	binary.LittleEndian.PutUint32(payload[7:11], 0) // filename length
	binary.LittleEndian.PutUint64(payload[11:19], 4)
	binary.LittleEndian.PutUint32(payload[19:23], uint32(len(sidBlock)))
	copy(payload[23:], sidBlock)
	if err := p.setDeadline(ctx); err != nil {
		return fail(err)
	}
	p.seq = 0
	if err := p.writePacket(payload); err != nil {
		p.clearDeadline()
		return fail(err)
	}
	p.clearDeadline()
	return &binlogStream{client: p}, nil
}

var _ connector.MySQLBinlogSource = (*Connector)(nil)
