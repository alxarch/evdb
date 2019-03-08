package meter

import (
	"compress/flate"
	"compress/gzip"
	"net/http"
	"sync"
	"time"
)

// OnceNoError is like sync.Once but retries until no error occured.
// type OnceNoError struct {
// 	mu   sync.Mutex
// 	done uint32
// }

// // Do calls fn if no previous call resulted in non nil error
// func (o *OnceNoError) Do(fn func() error) (err error) {
// 	if atomic.LoadUint32(&o.done) == 1 {
// 		return
// 	}
// 	o.mu.Lock()
// 	defer o.mu.Unlock()
// 	if o.done == 0 {
// 		err = fn()
// 		if err == nil {
// 			atomic.StoreUint32(&o.done, 1)
// 		}
// 	}
// 	return
// }

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

func InflateRequest(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		defer body.Close()
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			// err is returned on first read
			zr, _ := gzip.NewReader(body)
			r.Header.Del("Content-Encoding")
			r.Body = zr
		case "deflate":
			zr := flate.NewReader(body)
			r.Header.Del("Content-Encoding")
			r.Body = zr
		}
		next.ServeHTTP(w, r)
	}
}
