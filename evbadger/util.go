package evbadger

import (
	"sync"
)

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
