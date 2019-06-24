package meter

import (
	"sync"
	"sync/atomic"
)

// Event stores counters for an event
type Event struct {
	Name     string   `json:"name"`
	Labels   []string `json:"labels"`
	mu       sync.RWMutex
	counters Snapshot
	index    map[uint64][]int
}

const (
	defaultEventSize = 64
)

// NewEvent creates a new Event using the specified labels
func NewEvent(name string, labels ...string) *Event {
	e := Event{
		Name:     name,
		Labels:   labels,
		counters: make([]Counter, 0, defaultEventSize),
		index:    make(map[uint64][]int, defaultEventSize),
	}

	return &e
}

// Len returns the number of counters in an Event
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

// Add ads n to a specific counter
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
	n = e.add(n, h, vdeepcopy(values))
	e.mu.Unlock()
	return n
}

// Flush appends counters to a Snapshot and resets all counters to zero
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

// Merge adds all counters from a Snapshot
func (e *Event) Merge(s Snapshot) {
	for i := range s {
		c := &s[i]
		e.Add(c.Count, c.Values...)
	}
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

// Counter counts events labeled by values
type Counter struct {
	Count  int64    `json:"n"`
	Values []string `json:"v,omitempty"`
}

// Match checks if values match counter's own values
func (c *Counter) Match(values []string) bool {
	if len(c.Values) == len(values) {
		values = values[:len(c.Values)]
		for i := range c.Values {
			if c.Values[i] == values[i] {
				continue
			}
			return false
		}
		return true
	}
	return false
}

// Snapshot is a collection of counters
type Snapshot []Counter

// FilterZero filters out empty counters in-place
func (s Snapshot) FilterZero() Snapshot {
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
func (s Snapshot) Reset() Snapshot {
	for i := range s {
		s[i] = Counter{}
	}
	return s[:0]
}

var snapshotPool sync.Pool

func getSnapshot() Snapshot {
	if x := snapshotPool.Get(); x != nil {
		return x.(Snapshot)
	}
	const minSnapshotSize = 64
	return make([]Counter, 0, minSnapshotSize)
}

func putSnapshot(s Snapshot) {
	snapshotPool.Put(s.Reset())
}
