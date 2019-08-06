package events

import (
	"strings"
	"sync"
	"sync/atomic"
)

// Counter counts events labeled by values
type Counter struct {
	Count  int64    `json:"n"`
	Values []string `json:"v,omitempty"`
}

// UnsafeCounterIndex is an index of counters not safe for concurrent use
type UnsafeCounterIndex struct {
	counters Counters
	index    map[uint64][]int
}

// Len returns the number of counters in an Event
func (cs *UnsafeCounterIndex) Len() int {
	return len(cs.counters)
}

// CounterIndex is an index of counters safe for councurrent use
type CounterIndex struct {
	mu    sync.RWMutex
	index UnsafeCounterIndex
}

// Len returns the number of counters in an Event
func (cs *CounterIndex) Len() (n int) {
	cs.mu.RLock()
	n = cs.index.Len()
	cs.mu.RUnlock()
	return
}

// Pack packs the counter index dropping zero counters
func (cs *CounterIndex) Pack() {
	cs.mu.Lock()
	cs.index.Pack()
	cs.mu.Unlock()
}

// Match checks if values match counter's own values
func (c *Counter) Match(values []string) bool {
	a, b := c.Values, values
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

// Add increments a counter matching values by n
func (cs *UnsafeCounterIndex) Add(n int64, values ...string) int64 {
	h := vhash(values)
	c := cs.findOrCreate(h, values)
	c.Count += n
	return c.Count
}

// Flush appends all counters to a snapshot and resets them to zero
func (cs *UnsafeCounterIndex) Flush(s Counters) Counters {
	s = append(s, cs.counters...)
	cs.counters.Zero()
	return s
}

func (cs *UnsafeCounterIndex) findOrCreate(h uint64, values []string) *Counter {
	if cs.index == nil {
		cs.index = make(map[uint64][]int, 64)
	} else if c := cs.find(h, values); c != nil {
		return c
	}
	i := len(cs.counters)
	cs.counters = append(cs.counters, Counter{
		Values: vdeepcopy(values),
	})
	cs.index[h] = append(cs.index[h], i)
	return cs.Get(i)

}

func (cs *UnsafeCounterIndex) find(h uint64, values []string) *Counter {
	for _, i := range cs.index[h] {
		if 0 <= i && i < len(cs.counters) {
			c := &cs.counters[i]
			if c.Match(values) {
				return c
			}
		}
	}
	return nil
}

// Get returns the counter at index i
func (cs *UnsafeCounterIndex) Get(i int) *Counter {
	if 0 <= i && i < len(cs.counters) {
		return &cs.counters[i]
	}
	return nil
}

// Add adds n to a specific counter
func (cs *CounterIndex) Add(n int64, values ...string) int64 {
	h := vhash(values)
	cs.mu.RLock()
	c := cs.index.find(h, values)
	cs.mu.RUnlock()
	if c == nil {
		cs.mu.Lock()
		c = cs.index.findOrCreate(h, values)
		cs.mu.Unlock()
	}
	return atomic.AddInt64(&c.Count, n)
}

// Merge adds all counters from a CounterSlice
func (cs *CounterIndex) Merge(s Counters) {
	for i := range s {
		c := &s[i]
		cs.Add(c.Count, c.Values...)
	}
}

// Pack packs the counter index dropping zero counters
func (cs *UnsafeCounterIndex) Pack() {
	if len(cs.counters) == 0 {
		return
	}
	counters := make([]Counter, 0, len(cs.counters))
	for h, idx := range cs.index {
		packed := idx[:0]
		for _, i := range idx {
			c := cs.Get(i)
			if c.Count != 0 {
				packed = append(packed, len(counters))
				counters = append(counters, Counter{
					Count:  c.Count,
					Values: c.Values,
				})
			}
		}
		if len(packed) == 0 {
			delete(cs.index, h)
		} else {
			cs.index[h] = packed
		}
	}
	cs.counters = counters
}

// Counters is a slice of counters
type Counters []Counter

// FilterZero filters out empty counters in-place
func (s Counters) FilterZero() Counters {
	j := 0
	for i := range s {
		c := &s[i]
		if c.Count == 0 {
			continue
		}
		s[j] = *c
		j++
	}
	return s[:j]
}

// Reset resets a snapshot
func (s Counters) Reset() Counters {
	for i := range s {
		s[i] = Counter{}
	}
	return s[:0]
}

// Zero resets all counters to zero count
func (s Counters) Zero() {
	for i := range s {
		c := &s[i]
		c.Count = 0
	}
}

// Flush resets non zero counters and appends them to s
func (cs *CounterIndex) Flush(s []Counter) []Counter {
	cs.mu.RLock()
	src := cs.index.counters
	for i := range src {
		c := &src[i]
		n := atomic.SwapInt64(&c.Count, 0)
		if n != 0 {
			s = append(s, Counter{n, c.Values})
		}
	}
	cs.mu.RUnlock()
	return s
}

// NewCounterIndex creates a new counter index of size capacity
func NewCounterIndex(size int) *CounterIndex {
	cs := CounterIndex{
		index: UnsafeCounterIndex{
			counters: make([]Counter, 0, size),
			index:    make(map[uint64][]int, size),
		},
	}
	return &cs
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
