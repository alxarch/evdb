package blob

import (
	"encoding/binary"
)

type Shifter interface {
	ShiftBlob(blob []byte) (tail []byte, err error)
}
type Appender interface {
	AppendBlob(blob []byte) ([]byte, error)
}

func WriteU32BE(b []byte, n uint32) []byte {
	return append(b,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func ReadU32BE(b []byte) (uint32, []byte) {
	if len(b) >= 4 {
		return binary.BigEndian.Uint32(b), b[4:]
	}
	return 0, b
}

func WriteU64BE(b []byte, n uint64) []byte {
	return append(b,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func ReadU64BE(b []byte) (uint64, []byte) {
	if len(b) >= 8 {
		return binary.BigEndian.Uint64(b), b[8:]
	}
	return 0, b
}

func WriteStrings(b []byte, values []string) []byte {
	b = WriteU32BE(b, uint32(len(values)))
	for _, v := range values {
		b = WriteString(b, v)
	}
	return b
}

func ReadStrings(b []byte) ([]string, []byte) {
	var n uint32
	n, b = ReadU32BE(b)
	values := make([]string, 0, n)
	for ; len(b) > 0 && n > 0; n-- {
		var v string
		v, b = ReadString(b)
		values = append(values, v)
	}
	return values, b
}

func WriteString(b []byte, v string) []byte {
	b = WriteU32BE(b, uint32(len(v)))
	return append(b, v...)
}

func ReadString(b []byte) (string, []byte) {
	if len(b) > 4 {
		var size uint32
		size, b = binary.BigEndian.Uint32(b[:4]), b[4:]
		if size <= uint32(len(b)) {
			return string(b[:size]), b[size:]
		}
	}
	return "", b
}
