package meter

import (
	"sync"
	"sync/atomic"
)

// Counter counts events labeled by values
type Counter struct {
	Count  int64    `json:"n"`
	Values []string `json:"v,omitempty"`
}

// Snapshot is a slice of counters
type Snapshot []Counter

// UnsafeCounters is an index of counters not safe for concurrent use
type UnsafeCounters struct {
	counters Snapshot
	index    map[uint64][]int
}

// Len returns the number of counters in an Event
func (cs *UnsafeCounters) Len() int {
	return len(cs.counters)
}

// Counters is an index of counters safe for councurrent use
type Counters struct {
	mu       sync.RWMutex
	counters UnsafeCounters
}

// Len returns the number of counters in an Event
func (cs *Counters) Len() (n int) {
	cs.mu.RLock()
	n = cs.counters.Len()
	cs.mu.RUnlock()
	return
}

// Pack packs the counter index dropping zero counters
func (cs *Counters) Pack() {
	cs.mu.Lock()
	cs.counters.Pack()
	cs.mu.Unlock()
}

// Match checks if values match counter's own values
func (c *Counter) Match(values []string) bool {
	return stringsEqual(c.Values, values)
}

// Add increments a counter matching values by n
func (cs *UnsafeCounters) Add(n int64, values ...string) int64 {
	h := vhash(values)
	c := cs.findOrCreate(h, values)
	c.Count += n
	return c.Count
}

// Flush appends all counters to a snapshot and resets them to zero
func (cs *UnsafeCounters) Flush(s Snapshot) Snapshot {
	s = append(s, cs.counters...)
	cs.counters.Zero()
	return s
}

func (cs *UnsafeCounters) findOrCreate(h uint64, values []string) *Counter {
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

func (cs *UnsafeCounters) find(h uint64, values []string) *Counter {
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
func (cs *UnsafeCounters) Get(i int) *Counter {
	if 0 <= i && i < len(cs.counters) {
		return &cs.counters[i]
	}
	return nil
}

// Add adds n to a specific counter
func (cs *Counters) Add(n int64, values ...string) int64 {
	h := vhash(values)
	cs.mu.RLock()
	c := cs.counters.find(h, values)
	cs.mu.RUnlock()
	if c == nil {
		cs.mu.Lock()
		c = cs.counters.findOrCreate(h, values)
		cs.mu.Unlock()
	}
	return atomic.AddInt64(&c.Count, n)
}

// Merge adds all counters from a Snapshot
func (cs *Counters) Merge(s Snapshot) {
	for i := range s {
		c := &s[i]
		cs.Add(c.Count, c.Values...)
	}
}

// Pack packs the counter index dropping zero counters
func (cs *UnsafeCounters) Pack() {
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

// Zero resets all counters to zero count
func (s Snapshot) Zero() {
	for i := range s {
		c := &s[i]
		c.Count = 0
	}
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

// Flush appends counters to a Snapshot and resets all counters to zero
func (cs *Counters) Flush(s Snapshot) Snapshot {
	src := cs.counters.counters
	cs.mu.RLock()
	for i := range src {
		c := &src[i]
		s = append(s, Counter{
			Count:  atomic.SwapInt64(&c.Count, 0),
			Values: c.Values,
		})
	}
	cs.mu.RUnlock()
	return s
}

// NewCounters creates a new counter index of size capacity
func NewCounters(size int) *Counters {
	cs := Counters{
		counters: UnsafeCounters{
			counters: make([]Counter, 0, size),
			index:    make(map[uint64][]int, size),
		},
	}
	return &cs
}
