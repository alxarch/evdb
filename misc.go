package meter

import (
	"sync"
	"sync/atomic"
)

// OnceNoError is like sync.Once but retries until no error occured.
type OnceNoError struct {
	mu   sync.Mutex
	done uint32
}

// Do calls fn if no previous call resulted in non nil error
func (once *OnceNoError) Do(fn func() error) (err error) {
	if atomic.LoadUint32(&once.done) == 1 {
		return
	}
	once.mu.Lock()
	defer once.mu.Unlock()
	if once.done == 0 {
		defer func() {
			if err == nil {
				atomic.StoreUint32(&once.done, 1)
			}
		}()
		err = fn()
	}
	return
}

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}
