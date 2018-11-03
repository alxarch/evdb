package meter

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
