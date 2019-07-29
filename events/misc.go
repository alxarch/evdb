package events

import "strings"

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
