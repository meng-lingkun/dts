package mysqlbinlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
)

// StreamParser decodes a single COM_BINLOG_DUMP or COM_BINLOG_DUMP_GTID
// connection. The connector negotiates NONE for the initial synthetic rotate.
// The format description then determines the mode for the actual file.
type StreamParser struct {
	haveFormat bool
	crcEnabled bool
}

func (p *StreamParser) Parse(raw []byte) (*Event, error) {
	ev, err := (Parser{}).Parse(raw)
	if err != nil {
		return nil, err
	}
	if ev.Header.Type == FormatDescriptionEvent {
		crcEnabled, footerSize, err := formatChecksum(ev.Payload)
		if err != nil {
			return nil, err
		}
		if crcEnabled && !validEventCRC(raw) {
			return nil, errors.New("invalid binlog format description CRC32")
		}
		ev.Payload = ev.Payload[:len(ev.Payload)-footerSize]
		p.haveFormat, p.crcEnabled = true, crcEnabled
		return ev, nil
	}
	if !p.haveFormat && ev.Header.Type != RotateEvent {
		return nil, errors.New("binlog data arrived before its format description")
	}
	if p.crcEnabled {
		if len(ev.Payload) < 4 || !validEventCRC(raw) {
			return nil, fmt.Errorf("invalid binlog CRC32 for event type %d at %d", ev.Header.Type, ev.Header.LogPos)
		}
		ev.Payload = ev.Payload[:len(ev.Payload)-4]
	}
	if ev.Header.Type == RotateEvent {
		// Another synthetic rotate may precede the next format description.
		// MySQL still uses the previous mode for that event.
		p.haveFormat = false
	}
	return ev, nil
}

func formatChecksum(body []byte) (crcEnabled bool, footerSize int, err error) {
	// Format v4 starts with a version (2), server version (50), timestamp
	// (4) and common header length (1), followed by an event-length array.
	const fixed = 57
	if len(body) < fixed || binary.LittleEndian.Uint16(body[:2]) != 4 || body[fixed-1] != HeaderSize {
		return false, 0, errors.New("unsupported binlog format description")
	}
	version := strings.TrimRight(string(body[2:52]), "\x00")
	var major, minor, patch uint8
	if _, err := fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch); err != nil {
		return false, 0, fmt.Errorf("unknown binlog server version %q", version)
	}
	minVersion := 5<<16 | 6<<8 | 1
	if strings.Contains(strings.ToLower(version), "mariadb") {
		minVersion = 5<<16 | 3<<8
	}
	if int(major)<<16|int(minor)<<8|int(patch) < minVersion {
		return false, 0, nil // these versions predate the checksum descriptor
	}
	const footer = 5 // algorithm byte followed by four reserved checksum bytes
	if len(body) < fixed+footer {
		return false, 0, errors.New("missing binlog checksum descriptor")
	}
	switch alg := body[len(body)-footer]; alg {
	case 0:
		return false, footer, nil
	case 1:
		return true, footer, nil
	default:
		return false, 0, fmt.Errorf("unsupported binlog checksum algorithm %d", alg)
	}
}

func validEventCRC(raw []byte) bool {
	end := len(raw) - 4
	want := binary.LittleEndian.Uint32(raw[end:])
	if raw[4] == FormatDescriptionEvent && raw[17]&1 != 0 {
		// BINLOG_IN_USE can change after the file's checksum is written.
		sum := crc32.Update(0, crc32.IEEETable, raw[:17])
		sum = crc32.Update(sum, crc32.IEEETable, []byte{raw[17] &^ 1})
		return crc32.Update(sum, crc32.IEEETable, raw[18:end]) == want
	}
	return crc32.ChecksumIEEE(raw[:end]) == want
}
