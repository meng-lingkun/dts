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
	parser Parser
	magic  bool
}

func NewReader(r io.Reader, checksumBytes int) *Reader {
	return &Reader{r: bufio.NewReader(r), parser: Parser{ChecksumBytes: checksumBytes}}
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
	r.magic = true
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
