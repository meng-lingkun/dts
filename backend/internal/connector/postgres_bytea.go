package connector

import (
	"bytes"
	"encoding/hex"
	"errors"
)

// DecodePostgreSQLBytea decodes the text emitted by byteaout for either
// bytea_output setting. SQL result rows and pgoutput text tuples use it alike.
func DecodePostgreSQLBytea(raw []byte) ([]byte, error) {
	if len(raw) >= 2 && raw[0] == '\\' && raw[1] == 'x' {
		data := make([]byte, hex.DecodedLen(len(raw)-2))
		_, err := hex.Decode(data, raw[2:])
		return data, err
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw, nil
	}
	data := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			data = append(data, raw[i])
			continue
		}
		if i+1 < len(raw) && raw[i+1] == '\\' {
			data = append(data, '\\')
			i++
			continue
		}
		if i+3 >= len(raw) || raw[i+1] < '0' || raw[i+1] > '3' ||
			raw[i+2] < '0' || raw[i+2] > '7' || raw[i+3] < '0' || raw[i+3] > '7' {
			return nil, errors.New("invalid PostgreSQL bytea escape")
		}
		data = append(data, (raw[i+1]-'0')<<6|(raw[i+2]-'0')<<3|(raw[i+3]-'0'))
		i += 3
	}
	return data, nil
}
