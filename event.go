package meter

import (
	"strings"
	"sync"
	"sync/atomic"
)

type Event struct {
	Name   string   `json:"name"`
	Labels []string `json:"labels"`
	// sorted   labelIndex
	mu       sync.RWMutex
	counters Snapshot
	index    map[uint64][]int
}

const (
	defaultEventSize = 64
)

func NewEvent(name string, labels ...string) *Event {
	e := Event{
		Name:   name,
		Labels: labels,
		// sorted:   newLabeLIndex(labels...),
		counters: make([]Counter, 0, defaultEventSize),
		index:    make(map[uint64][]int, defaultEventSize),
	}

	return &e
}

func (e *Event) Len() (n int) {
	e.mu.RLock()
	n = len(e.counters)
	e.mu.RUnlock()
	return
}

// Pack packs the event index dropping zero counters
func (e *Event) Pack() {
	e.mu.Lock()
	e.pack()
	e.mu.Unlock()
}

func (e *Event) add(n int64, h uint64, values []string) int64 {
	if e.index == nil {
		e.index = make(map[uint64][]int, 64)
	} else {
		if c := e.find(h, values); c != nil {
			return atomic.AddInt64(&c.Count, n)
		}
	}
	i := len(e.counters)
	e.counters = append(e.counters, Counter{
		Count:  n,
		Values: values,
	})
	e.index[h] = append(e.index[h], i)
	return n
}

func (e *Event) find(h uint64, values []string) *Counter {
	for _, i := range e.index[h] {
		if 0 <= i && i < len(e.counters) {
			c := &e.counters[i]
			if c.Match(values) {
				return c
			}
		}
	}
	return nil
}

func (e *Event) AddVolatile(n int64, values ...string) int64 {
	h := vhash(values)
	e.mu.RLock()
	c := e.find(h, values)
	e.mu.RUnlock()
	if c != nil {
		return atomic.AddInt64(&c.Count, n)
	}
	e.mu.Lock()
	// Copy avoids allocation for variadic values.
	n = e.add(n, h, vdeepcopy(values))
	e.mu.Unlock()
	return n
}

func (e *Event) Add(n int64, values ...string) int64 {
	h := vhash(values)
	e.mu.RLock()
	c := e.find(h, values)
	e.mu.RUnlock()
	if c != nil {
		return atomic.AddInt64(&c.Count, n)
	}
	e.mu.Lock()
	// Copy avoids allocation for variadic values.
	n = e.add(n, h, vcopy(values))
	e.mu.Unlock()
	return n
}

func (e *Event) Flush(s Snapshot) Snapshot {
	s = s[:cap(s)]
	e.mu.RLock()
	src := e.counters
	if len(s) < len(src) {
		s = make([]Counter, len(src))
	}
	if len(s) >= len(src) {
		s = s[:len(src)]
		for i := range src {
			c := &src[i]
			s[i] = Counter{
				Count:  atomic.SwapInt64(&c.Count, 0),
				Values: c.Values,
			}
		}
	}
	e.mu.RUnlock()
	return s
}

func (e *Event) Merge(s Snapshot) {
	for i := range s {
		c := &s[i]
		e.Add(c.Count, c.Values...)
	}
}

func (e *Event) get(i int) *Counter {
	if 0 <= i && i < len(e.counters) {
		return &e.counters[i]
	}
	return nil
}

func (e *Event) pack() {
	if len(e.counters) == 0 {
		return
	}
	counters := make([]Counter, 0, len(e.counters))
	for h, idx := range e.index {
		packed := idx[:0]
		for _, i := range idx {
			c := e.get(i)
			if c.Count != 0 {
				packed = append(packed, len(counters))
				counters = append(counters, Counter{
					Count:  c.Count,
					Values: c.Values,
				})
			}
		}
		if len(packed) == 0 {
			delete(e.index, h)
		} else {
			e.index[h] = packed
		}
	}
	e.counters = counters
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
func vcopy(values []string) []string {
	cp := make([]string, len(values))
	if len(cp) == len(values) {
		cp = cp[:len(values)]
		for i := range values {
			cp[i] = values[i]
		}
	}
	return cp
}
