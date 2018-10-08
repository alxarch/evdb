package meter

import (
	"errors"
	"sync"
)

var (
	ErrInvalidEventLabel = errors.New("Invalid event label")
	ErrInvalidGroupLabel = errors.New("Invalid group label")
	ErrInvalidResolution = errors.New("Invalid event resolution")
)

var nilDesc = &Desc{err: ErrNilDesc}

func NewEvent(desc *Desc) *Event {
	if desc == nil {
		desc = nilDesc
	}

	return &Event{
		desc: desc,
	}
}

type Event struct {
	mu       sync.RWMutex
	index    map[uint64][]int
	counters []CounterAtomic
	desc     *Desc
}

func (e *Event) Len() (n int) {
	e.mu.RLock()
	n = len(e.counters)
	e.mu.RUnlock()
	return
}

// Reset clears all stored counters
// WARNING: If a counter is referenced elsewere it will not be collected by Collect()
func (e *Event) Reset() {
	e.mu.Lock()
	tmp := e.counters
	for i := range tmp {
		tmp[i].values = ""
	}
	e.counters = tmp[:0]
	for h := range e.index {
		delete(e.index, h)
	}
	e.mu.Unlock()
}

var poolValues sync.Pool

func (e *Event) WithLabels(m LabelValues) Counter {
	x := poolValues.Get()
	var values []string
	if x == nil {
		values = make([]string, len(e.desc.labels))
		x = values
	} else {
		values = x.([]string)
	}
	values = m.AppendValues(values[:0], e.desc.labels)
	c := e.findOrCreate(values)
	poolValues.Put(x)
	return c
}

func (e *Event) WithLabelValues(values ...string) Counter {
	return e.findOrCreate(values)
}

func (e *Event) findOrCreate(values []string) *CounterAtomic {
	var (
		h   = hashNew()
		ids []int
		id  int
		c   *CounterAtomic
	)
	for i, v := range values {
		if i > 0 {
			h = hashAddByte(h, LabelSeparator)
		}
		for j := 0; 0 <= j && j < len(v); j++ {
			h = hashAddByte(h, v[j])
		}
	}
	e.mu.RLock()
	ids = e.index[h]
	for _, id = range ids {
		if 0 <= id && id < len(e.counters) {
			c = &e.counters[id]
			if c.Match(values) {
				e.mu.RUnlock()
				return c
			}
		}
	}
	e.mu.RUnlock()
	e.mu.Lock()
	if e.index == nil {
		e.index = make(map[uint64][]int)
	}
	ids = e.index[h]
	for _, id = range ids {
		if 0 <= id && id < len(e.counters) {
			c = &e.counters[id]
			if c.Match(values) {
				e.mu.Unlock()
				return c
			}
		}
	}
	id = len(e.counters)
	e.counters = append(e.counters, CounterAtomic{values: joinValues(values)})
	e.index[h] = append(e.index[h], id)
	c = &e.counters[id]
	e.mu.Unlock()
	return c
}

func (e *Event) Describe() *Desc {
	return e.desc
}

const separatorByte byte = 255

func valuesHash(values []string) (h uint64) {
	var (
		i int
		v string
	)
	h = hashNew()
	for _, v = range values {
		for i = 0; 0 <= i && i < len(v); i++ {
			h = hashAddByte(h, v[i])
		}
		h = hashAddByte(h, separatorByte)
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

func distinct(values ...string) []string {
	if values == nil {
		return []string{}
	}
	j := 0
	for _, value := range values {
		if 0 <= j && j < len(values) && indexOf(values[:j], value) == -1 {
			values[j] = value
			j++
		}
	}
	return values[:j]
}

var poolSnapshot sync.Pool

func (e *Event) Flush(s Snapshot) Snapshot {
	n := e.Len()
	if cap(s)-len(s) < n {
		tmp := make([]CounterLocal, len(s), len(s)+n)
		copy(tmp, s)
		s = tmp
	}
	e.mu.RLock()
	for i := range e.counters {
		c := &e.counters[i]
		s = append(s, CounterLocal{
			n:      c.Set(0),
			values: c.values,
		})
	}
	e.mu.RUnlock()
	return s
}

func (e *Event) Merge(s Snapshot) {
	x := poolValues.Get()
	var values []string
	if x == nil {
		values = make([]string, len(e.desc.labels))
		x = values
	} else {
		values = x.([]string)
	}
	for i := range s {
		c := &s[i]
		values = appendRawValues(values[:0], c.values)
		e.findOrCreate(values).Add(c.n)
	}
	poolValues.Put(x)
}
