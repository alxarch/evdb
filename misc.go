package meter

import (
	"encoding/binary"
	"strings"
	"sync"
	"time"
)

func stringsEqual(a, b []string) bool {
	if len(a) == len(b) {
		b = b[:len(a)]
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	return false
}
func distinctSorted(ss []string) []string {
	var (
		i    int
		last string
	)
	for _, s := range ss {
		if i == 0 || s != last {
			last = s
			ss[i] = s
			i++
		}
	}
	return ss[:i]
}
func appendDistinct(dst []string, src ...string) []string {
	for i, s := range src {
		if indexOf(dst, s[:i]) == -1 {
			dst = append(dst, s)
		}
	}
	return dst
}

// func readTimestampSuffix(key []byte) int64 {
// 	if n := len(key) - 8; 0 <= n && n < len(key) {
// 		return int64(binary.BigEndian.Uint64(key[n:]))
// 	}
// 	return 0
// }

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

// Inline and byte-free variant of hash/fnv's fnv64a.

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// hashNew initializies a new fnv64a hash value.
func hashNew() uint64 {
	return offset64
}

// hashAddByte adds a byte to a fnv64a hash value, returning the updated hash.
func hashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}

func vhash(values []string) (h uint64) {
	h = hashNew()
	for _, v := range values {
		for i := 0; 0 <= i && i < len(v); i++ {
			h = hashAddByte(h, v[i])
		}

	}
	return
}

const (
	offset32 = 2166136261
	prime32  = 16777619
)

func newFNVa32() uint32 {
	return offset32

}
func addFNVa32(h, b uint32) uint32 {
	h ^= b
	h *= prime32
	return h
}

func hashFNVa32(data []byte) uint32 {
	h := newFNVa32()
	for _, b := range data {
		h = addFNVa32(h, uint32(b))
	}
	return h
}

var buffers sync.Pool

const kiB = 1024

func getBuffer() []byte {
	if x, ok := buffers.Get().([]byte); ok {
		return x
	}
	return make([]byte, 4*kiB)
}

func putBuffer(buf []byte) {
	buffers.Put(buf)
}

func stepTS(ts, step int64) int64 {
	if step > 0 {
		return ts - ts%step
	}
	if step == 0 {
		return ts
	}
	return 0
}

func normalizeStep(step time.Duration) time.Duration {
	switch {
	case step <= 0:
		return step
	case step < time.Second:
		return time.Second
	default:
		return step.Truncate(time.Second)
	}
}

func vdeepcopy(values []string) []string {
	n := 0
	b := strings.Builder{}
	for _, v := range values {
		n += len(v)
	}
	b.Grow(n)
	for _, v := range values {
		b.WriteString(v)
	}
	tmp := b.String()
	cp := make([]string, len(values))
	if len(cp) == len(values) {
		cp = cp[:len(values)]
		for i := range values {
			n = len(values[i])
			cp[i] = tmp[:n]
			tmp = tmp[n:]
		}
	}
	return cp

}

func appendUint32(dst []byte, n uint32) []byte {
	return append(dst,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}
func appendUint64(dst []byte, n uint64) []byte {
	return append(dst,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func shiftUint64(data []byte) (uint64, []byte) {
	if len(data) >= 8 {
		return binary.BigEndian.Uint64(data), data[8:]
	}
	return 0, data
}

func u32BE(data string) (uint32, string) {
	if len(data) >= 4 {
		return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24, data[4:]
	}
	return 0, data
}
func shiftUint32(data []byte) (uint32, []byte) {
	if len(data) >= 4 {
		return binary.BigEndian.Uint32(data), data[4:]
	}
	return 0, data
}

func appendStringSlice(dst []byte, ss []string) []byte {
	dst = appendUint32(dst, uint32(len(ss)))
	for _, s := range ss {
		dst = appendString(dst, s)
	}
	return dst
}

func shiftStringSlice(data []byte) ([]string, []byte) {
	var n uint32
	n, data = shiftUint32(data)
	ss := make([]string, 0, n)
	for len(data) > 0 {
		var s string
		s, data = shiftString(data)
		ss = append(ss, s)
	}
	return ss, data
}

func shiftString(data []byte) (string, []byte) {
	if len(data) > 4 {
		var size uint32
		size, data = binary.BigEndian.Uint32(data[:4]), data[4:]
		if size <= uint32(len(data)) {
			return string(data[:size]), data[size:]
		}
	}
	return "", data
}

func appendString(dst []byte, s string) []byte {
	dst = appendUint32(dst, uint32(len(s)))
	dst = append(dst, s...)
	return dst
}
