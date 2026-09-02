package oracleconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

type ttcDataTypeInfo struct {
	CompileTimeCaps []byte
	RuntimeCaps     []byte
	TTCVersion      byte
	ServerTimeZone  []byte
}

type oracleTypeRep struct {
	DataType       uint16
	NativeDataType uint16
	Representation uint16
}

// qMigrationOracleTypeReps is intentionally limited to the Oracle scalar/LOB
// families QMigration's migration engine can normalize today.  TTC allows the
// client to advertise representation support; unsupported vendor-internal
// types are not claimed merely to imitate a general-purpose driver.
var qMigrationOracleTypeReps = []oracleTypeRep{
	{1, 1, 1},  // VARCHAR2
	{2, 2, 10}, // NUMBER
	{3, 2, 10}, // INTEGER/NUMBER alias
	{4, 2, 10},
	{5, 1, 1},
	{6, 2, 10},
	{7, 2, 10},
	{8, 8, 1}, // LONG
	{9, 1, 1},
	{11, 11, 1},
	{12, 12, 10}, // DATE
	{15, 23, 1},
	{23, 23, 1}, // RAW
	{24, 24, 1}, // LONG RAW
	{39, 120, 1},
	{40, 40, 1},
	{41, 41, 1},
	{68, 2, 10},
	{91, 2, 10},
	{94, 1, 1},
	{95, 23, 1},
	{96, 96, 1}, // CHAR
	{97, 96, 1},
	{100, 100, 1}, // BINARY_FLOAT
	{101, 101, 1}, // BINARY_DOUBLE
	{102, 102, 1}, // cursor
	{104, 11, 1},
	{106, 106, 1},
	{108, 109, 1},
	{109, 109, 1},
	{110, 111, 1},
	{111, 111, 1},
	{112, 112, 1}, // CLOB
	{113, 113, 1}, // BLOB
	{114, 114, 1},
	{115, 115, 1},
	{116, 102, 1},
	{117, 117, 1},
	{120, 120, 1},
	{146, 146, 1},
	{152, 2, 10},
	{153, 2, 10},
	{154, 2, 10},
	{155, 1, 1},
	{156, 12, 10},
	{172, 2, 10},
	{178, 178, 1},
	{179, 179, 1},
	{180, 180, 1}, // TIMESTAMP
	{181, 181, 1}, // TIMESTAMP WITH TZ
	{182, 182, 1},
	{183, 183, 1},
	{184, 12, 10},
	{185, 185, 1},
	{186, 186, 1},
	{187, 187, 1},
	{188, 188, 1},
	{189, 189, 1},
	{190, 190, 1},
	{195, 112, 1},
	{196, 113, 1},
	{197, 114, 1},
	{208, 208, 1},
	{231, 231, 1}, // TIMESTAMP WITH LOCAL TZ
	{232, 231, 1},
	{233, 233, 1},
	{241, 109, 1},
	{252, 252, 0},
}

func oracleClientCompileCaps(server ttcProtocolInfo) []byte {
	caps := []byte{
		6, 1, 0, 0, 106, 1, 1, 12,
		1, 1, 1, 1, 1, 1, 0, 41,
		144, 3, 7, 3, 0, 1, 0, 79,
		1, 55, 4, 1, 0, 0, 0, 28,
		0, 0, 10, 160, 3, 179, 0,
	}
	if len(server.CompileTimeCaps) <= 27 || server.CompileTimeCaps[27] == 0 {
		caps[27] = 0
	}
	if len(server.CompileTimeCaps) <= 4 || server.CompileTimeCaps[4]&8 == 0 {
		caps[4] = 0
	}
	if len(server.CompileTimeCaps) <= 4 || server.CompileTimeCaps[4]&32 == 0 {
		caps[4] &= 0xdf
	}
	if len(server.CompileTimeCaps) > 7 && server.CompileTimeCaps[7] < 7 {
		caps[36] = 0
	}
	if len(server.CompileTimeCaps) <= 37 || server.CompileTimeCaps[37]&2 == 0 {
		caps[37] &= 0xfd
	}
	return caps
}

func oracleClientRuntimeCaps(server ttcProtocolInfo, compile []byte) []byte {
	r := []byte{2, 1, 0, 0, 0, 0, 0}
	if len(server.RuntimeCaps) <= 1 || server.RuntimeCaps[1]&1 == 0 {
		r[1] = 0
	}
	if len(server.RuntimeCaps) > 6 {
		if server.RuntimeCaps[6]&4 != 0 {
			r[6] |= 4
		}
		if server.RuntimeCaps[6]&2 != 0 {
			r[6] |= 2
		}
	}
	if len(compile) <= 37 || compile[37]&2 == 0 {
		r[1] &= 0xfe
	}
	return r
}

func oracleTZBytes() []byte {
	_, off := time.Now().Zone()
	h := int8(off / 3600)
	m := int8((off / 60) % 60)
	s := int8(off % 60)
	return []byte{128, 0, 0, 0, byte(int(h) + 60), byte(int(m) + 60), byte(int(s) + 60), 128, 0, 0, 0}
}

func buildTTCDataTypeRequest(server ttcProtocolInfo) (ttcDataTypeInfo, []byte, error) {
	var info ttcDataTypeInfo
	compile := oracleClientCompileCaps(server)
	runtime := oracleClientRuntimeCaps(server, compile)
	w := &ttcEncoder{}
	w.byte(2)
	// Oracle TTC data-type negotiation uses little-endian fixed two-byte
	// charset ids for these two fields.
	w.fixedUint(uint64(server.ServerCharset), 2, false)
	w.fixedUint(uint64(server.ServerCharset), 2, false)
	w.byte(server.ServerFlags | 2)
	w.byte(byte(len(compile)))
	_, _ = w.Write(compile)
	w.byte(byte(len(runtime)))
	_, _ = w.Write(runtime)
	if runtime[1]&1 != 0 {
		_, _ = w.Write(oracleTZBytes())
		if compile[37]&2 != 0 {
			_, _ = w.Write([]byte{0, 0, 0, 21})
		}
	}
	w.fixedUint(uint64(server.ServerNCharset), 2, false)

	wide := compile[27] != 0
	for _, rep := range qMigrationOracleTypeReps {
		vals := []uint16{rep.DataType, rep.NativeDataType}
		if rep.NativeDataType != 0 {
			vals = append(vals, rep.Representation, 0)
		}
		for _, v := range vals {
			if !wide {
				if v > 255 {
					continue
				}
				w.byte(byte(v))
			} else {
				var b [2]byte
				binary.BigEndian.PutUint16(b[:], v)
				_, _ = w.Write(b[:])
			}
		}
	}
	if wide {
		_, _ = w.Write([]byte{0, 0})
	} else {
		w.byte(0)
	}
	info.CompileTimeCaps = append([]byte(nil), compile...)
	info.RuntimeCaps = append([]byte(nil), runtime...)
	if len(compile) > 7 {
		info.TTCVersion = compile[7]
	}
	if len(server.CompileTimeCaps) > 7 && server.CompileTimeCaps[7] < info.TTCVersion {
		info.TTCVersion = server.CompileTimeCaps[7]
	}
	return info, w.Bytes(), nil
}

func parseTTCDataTypeResponse(payload []byte, info *ttcDataTypeInfo) error {
	if info == nil {
		return errors.New("nil Oracle TTC datatype info")
	}
	r := newTTCDecoder(payload)
	code, err := r.byte()
	if err != nil {
		return err
	}
	if code != 2 {
		return fmt.Errorf("Oracle TTC datatype response code %d, expected 2", code)
	}
	if len(info.RuntimeCaps) > 1 && info.RuntimeCaps[1] == 1 {
		z, err := r.take(11)
		if err != nil {
			return err
		}
		info.ServerTimeZone = append([]byte(nil), z...)
		if len(info.CompileTimeCaps) > 37 && info.CompileTimeCaps[37]&2 != 0 {
			if _, err = r.take(4); err != nil {
				return err
			}
		}
	}
	// The remainder is a server representation list.  The client does not need
	// to materialize it for migration correctness because it already limits
	// itself to the advertised QMigration scalar/LOB set; still require a legal
	// terminating zero when bytes are present so truncated replies fail closed.
	if r.remaining() == 0 {
		return nil
	}
	wide := len(info.CompileTimeCaps) > 27 && info.CompileTimeCaps[27] != 0
	term := false
	for r.remaining() > 0 {
		if wide {
			v, err := r.fixedUint(2, true)
			if err != nil {
				return err
			}
			if v == 0 {
				term = true
				break
			}
		} else {
			v, err := r.byte()
			if err != nil {
				return err
			}
			if v == 0 {
				term = true
				break
			}
		}
	}
	if !term {
		return errors.New("Oracle TTC datatype response has no terminator")
	}
	return nil
}

func (c *Connector) negotiateTTCDataTypes(ctx context.Context, accepted *acceptedSession, proto ttcProtocolInfo) (ttcDataTypeInfo, error) {
	var out ttcDataTypeInfo
	info, request, err := buildTTCDataTypeRequest(proto)
	if err != nil {
		return out, err
	}
	if err = accepted.Session.WriteData(ctx, 0, request); err != nil {
		return out, fmt.Errorf("Oracle TTC datatype request: %w", err)
	}
	flags, payload, err := accepted.Session.ReadData(ctx)
	if err != nil {
		return out, fmt.Errorf("Oracle TTC datatype response: %w", err)
	}
	if flags != 0 {
		return out, fmt.Errorf("Oracle TTC datatype DATA flags 0x%x unsupported", flags)
	}
	if err = parseTTCDataTypeResponse(payload, &info); err != nil {
		return out, err
	}
	return info, nil
}
