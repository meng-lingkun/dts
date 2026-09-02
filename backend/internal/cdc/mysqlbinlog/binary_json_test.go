package mysqlbinlog

import (
	"encoding/binary"
	"testing"
)

func TestDecodeBinaryJSONScalarsAndArray(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"string", []byte{jsonString, 3, 'a', 'b', 'c'}, `"abc"`},
		{"int16", []byte{jsonInt16, 0xfe, 0xff}, `-2`},
		{"literal", []byte{jsonLiteral, 1}, `true`},
		{"array", []byte{
			jsonSmallArray,
			0x03, 0x00, // count
			0x0f, 0x00, // size
			jsonLiteral, 0x01, 0x00,
			jsonInt16, 0x01, 0x00,
			jsonString, 0x0d, 0x00,
			0x01, 'x',
		}, `[true,1,"x"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeBinaryJSON(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestDecodeBinaryJSONSmallObject(t *testing.T) {
	// Small object payload layout:
	// header(4), two key entries(8), two value entries(6), keys(2), string(2).
	in := []byte{
		jsonSmallObject,
		0x02, 0x00, // count
		0x16, 0x00, // size=22
		0x12, 0x00, 0x01, 0x00, // key "a"
		0x13, 0x00, 0x01, 0x00, // key "b"
		jsonInt16, 0x01, 0x00,
		jsonString, 0x14, 0x00,
		'a', 'b',
		0x01, 'x',
	}
	got, err := DecodeBinaryJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if got != `{"a":1,"b":"x"}` {
		t.Fatalf("unexpected object: %s", got)
	}
}

func TestDecodeBinaryJSONUnknownOpaqueFailsSafe(t *testing.T) {
	if _, err := DecodeBinaryJSON(opaqueJSON(0xee, []byte{1, 2, 3})); err == nil {
		t.Fatal("expected unknown opaque subtype to fail safe")
	}
}

func opaqueJSON(mysqlType byte, data []byte) []byte {
	if len(data) >= 0x80 {
		panic("test helper only supports one-byte varint lengths")
	}
	out := []byte{jsonOpaque, mysqlType, byte(len(data))}
	return append(out, data...)
}

func packOpaqueDateTime(year, month, day, hour, minute, second, micro int) []byte {
	ym := int64(year*13 + month)
	ymd := (ym << 5) | int64(day)
	hms := (int64(hour) << 12) | (int64(minute) << 6) | int64(second)
	intPart := (ymd << 17) | hms
	v := (intPart << 24) | int64(micro)
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(v))
	return out
}

func packOpaqueTime(hour, minute, second, micro int, negative bool) []byte {
	intPart := (int64(hour) << 12) | (int64(minute) << 6) | int64(second)
	v := (intPart << 24) | int64(micro)
	if negative {
		v = -v
	}
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(v))
	return out
}

func TestDecodeBinaryJSONOpaqueKnownTypes(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{
			name: "decimal",
			// OPAQUE NEWDECIMAL data begins with precision, scale, then MySQL's
			// packed decimal bytes. DECIMAL(5,2) 123.45 = 80 7b 2d.
			in:   opaqueJSON(TypeNewDecimal, []byte{5, 2, 0x80, 0x7b, 0x2d}),
			want: `123.45`,
		},
		{
			name: "date",
			in:   opaqueJSON(TypeDate, packOpaqueDateTime(2024, 1, 2, 0, 0, 0, 0)),
			want: `"2024-01-02"`,
		},
		{
			name: "datetime",
			in:   opaqueJSON(TypeDateTime, packOpaqueDateTime(2024, 1, 2, 3, 4, 5, 123456)),
			want: `"2024-01-02 03:04:05.123456"`,
		},
		{
			name: "timestamp",
			in:   opaqueJSON(TypeTimestamp, packOpaqueDateTime(2024, 1, 2, 3, 4, 5, 123456)),
			want: `"2024-01-02 03:04:05.123456"`,
		},
		{
			name: "time",
			in:   opaqueJSON(TypeTime, packOpaqueTime(27, 4, 5, 654321, false)),
			want: `"27:04:05.654321"`,
		},
		{
			name: "negative-time",
			in:   opaqueJSON(TypeTime, packOpaqueTime(27, 4, 5, 654321, true)),
			want: `"-27:04:05.654321"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeBinaryJSON(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
