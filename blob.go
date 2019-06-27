package meter

import "encoding/binary"

type Blob []byte

type FromBlober interface {
	FromBlob(Blob) (Blob, error)
}
type ToBlober interface {
	ToBlob(Blob) (Blob, error)
}

func (b Blob) WriteU32BE(n uint32) Blob {
	return append(b,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}
func (b Blob) ReadU32BE() (uint32, Blob) {
	if len(b) >= 4 {
		return binary.BigEndian.Uint32(b), b[4:]
	}
	return 0, b
}

func (b Blob) WriteU64BE(n uint64) Blob {
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

func (b Blob) ReadU64BE() (uint64, Blob) {
	if len(b) >= 8 {
		return binary.BigEndian.Uint64(b), b[8:]
	}
	return 0, b
}

func (b Blob) WriteStrings(values []string) Blob {
	b = b.WriteU32BE(uint32(len(values)))
	for _, v := range values {
		b = b.WriteString(v)
	}
	return b
}

func (b Blob) ReadStrings() ([]string, Blob) {
	var n uint32
	n, b = b.ReadU32BE()
	values := make([]string, 0, n)
	for ; len(b) > 0 && n > 0; n-- {
		var v string
		v, b = b.ReadString()
		values = append(values, v)
	}
	return values, b
}

func (b Blob) WriteString(v string) Blob {
	b = b.WriteU32BE(uint32(len(v)))
	return append(b, v...)
}

func (b Blob) ReadString() (string, []byte) {
	if len(b) > 4 {
		var size uint32
		size, b = binary.BigEndian.Uint32(b[:4]), b[4:]
		if size <= uint32(len(b)) {
			return string(b[:size]), b[size:]
		}
	}
	return "", b
}
