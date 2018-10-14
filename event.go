package meter

import (
	"strings"
	"sync"
	"sync/atomic"
)

type Event struct {
	desc     *Desc
	mu       sync.RWMutex
	counters []Counter
	index    map[uint64][]int
}

var nilDesc = &Desc{err: ErrNilDesc}

const (
	defaultEventSize = 64
)

func NewEvent(desc *Desc) *Event {
	if desc == nil {
		desc = nilDesc
	}
	e := Event{
		desc:     desc,
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
			return atomic.AddInt64(&c.n, n)
		}
	}
	i := len(e.counters)
	e.counters = append(e.counters, Counter{
		n:      n,
		values: values,
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
		return atomic.AddInt64(&c.n, n)
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
		return atomic.AddInt64(&c.n, n)
	}
	e.mu.Lock()
	// Copy avoids allocation for variadic values.
	n = e.add(n, h, vcopy(values))
	e.mu.Unlock()
	return n
}

func (e *Event) Describe() *Desc {
	if e != nil {
		return e.desc
	}
	return nil
}

func (e *Event) Flush(s Snapshot) Snapshot {
	e.mu.RLock()
	for i := range e.counters {
		c := &e.counters[i]
		s = append(s, Counter{
			n:      atomic.SwapInt64(&c.n, 0),
			values: c.values,
		})
	}
	e.mu.RUnlock()
	return s
}

func (e *Event) Merge(s Snapshot) {
	for i := range s {
		c := &s[i]
		e.Add(c.n, c.values...)
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
			if c.n != 0 {
				packed = append(packed, len(counters))
				counters = append(counters, Counter{
					n:      c.n,
					values: c.values,
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
	s := strings.Builder{}
	n := 0
	for _, v := range values {
		n += len(v)
	}
	s.Grow(n)
	cp := make([]string, len(values))
	for i, v := range values {
		n = s.Len()
		s.WriteString(v)
		v = s.String()
		cp[i] = v[n:]
	}
	return cp

}
func vcopy(values []string) []string {
	cp := make([]string, len(values))
	for i, v := range values {
		cp[i] = v
	}
	return cp
}

// func (e *Event) with(tag string) (c *CounterAtomic) {
// 	h := rawValuesHash(tag)
// 	e.mu.RLock()
// 	for _, c = range e.counters[h] {
// 		if c.values == tag {
// 			e.mu.RUnlock()
// 			return
// 		}
// 	}
// 	e.mu.RUnlock()
// 	e.mu.Lock()
// 	if e.counters == nil {
// 		e.counters = make(map[uint64][]*CounterAtomic, 64)
// 	} else {
// 		for _, cc := range e.counters[h] {
// 			if cc.values == tag {
// 				e.mu.Unlock()
// 				return c
// 			}
// 		}
// 	}
// 	c = new(CounterAtomic)
// 	c.values = tag
// 	e.counters[h] = append(e.counters[h], c)
// 	e.mu.Unlock()
// 	return c
// }

// func (e *Event) WithLabels(m map[string]string) Counter {
// 	if e == nil {
// 		return nil
// 	}
// 	desc := e.Describe()
// 	labels := desc.Labels()
// 	values := make([]string, len(labels))
// 	for i, label := range labels {
// 		values[i] = m[label]
// 	}
// 	return e.findOrCreate(values)
// }

// func (e *Event) WithLabelValues(values ...string) Counter {
// 	return e.findOrCreate(values)
// }
