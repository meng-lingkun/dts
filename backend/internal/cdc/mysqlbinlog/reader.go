package mysqlbinlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var FileMagic = []byte{0xfe, 'b', 'i', 'n'}

type Reader struct {
	r      *bufio.Reader
	parser EventParser
}

func NewReader(r io.Reader, checksumBytes int) *Reader {
	return &Reader{r: bufio.NewReader(r), parser: NewEventParser(checksumBytes)}
}

// EventParser accepts one complete binlog event at a time.
type EventParser interface {
	Parse([]byte) (*Event, error)
}

// NewEventParser selects per-file negotiation with -1, or fixed trailer
// lengths for inspecting event fragments that lack a format description.
func NewEventParser(checksumBytes int) EventParser {
	if checksumBytes == -1 {
		return &StreamParser{}
	}
	return Parser{ChecksumBytes: checksumBytes}
}

// ReadMagic validates the 4-byte header present in raw mysql-bin.* files.
// Replication network streams do not contain this prefix and can call Next directly.
func (r *Reader) ReadMagic() error {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return err
	}
	for i := range FileMagic {
		if buf[i] != FileMagic[i] {
			return fmt.Errorf("invalid binlog file magic %x", buf)
		}
	}
	return nil
}

func (r *Reader) Next() (*Event, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r.r, header); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(header[9:13])
	if size < HeaderSize {
		return nil, errors.New("invalid event size")
	}
	raw := make([]byte, int(size))
	copy(raw, header)
	if _, err := io.ReadFull(r.r, raw[HeaderSize:]); err != nil {
		return nil, err
	}
	return r.parser.Parse(raw)
}
