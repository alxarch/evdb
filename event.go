package meter

import (
	"errors"
	"sync"
)

type Event interface {
	WithLabels(labels LabelValues) Counter
	WithLabelValues(values ...string) Counter
	Descriptor
	Collector
}

type Resetable interface {
	Reset()
}

var (
	ErrInvalidEventLabel = errors.New("Invalid event label")
	ErrInvalidGroupLabel = errors.New("Invalid group label")
	ErrInvalidResolution = errors.New("Invalid event resolution")
)

var nilDesc = &Desc{err: ErrNilDesc}

func NewEvent(desc *Desc) Event {
	if desc == nil {
		desc = nilDesc
	}
	return newEvent(desc)
}

type event struct {
	counters map[uint64][]Counter
	mu       sync.RWMutex
	desc     *Desc
}

func (e *event) Len() (n int) {
	e.mu.RLock()
	for _, counters := range e.counters {
		n += len(counters)
	}
	e.mu.RUnlock()
	return
}

// Reset clears all stored counters
// WARNING: If a counter is referenced elsewere it will not be collected by Collect()
func (c *event) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counters = make(map[uint64][]Counter)
}

var _ Event = &event{}

func newEvent(d *Desc) *event {
	return &event{
		counters: make(map[uint64][]Counter),
		desc:     d,
	}
}

func (e *event) Collect(ch chan<- Metric) {
	e.mu.RLock()
	if len(e.counters) == 0 {
		e.mu.RUnlock()
		return
	}
	for _, counters := range e.counters {
		for _, c := range counters {
			ch <- counterMetric{c, e.desc}
		}
	}
	e.mu.RUnlock()
}

func (e *event) WithLabels(values LabelValues) Counter {
	lv := values.Values(e.desc.labels)
	m := e.findOrCreate(lv)
	return m
}
func (e *event) WithLabelValues(values ...string) Counter {
	return e.findOrCreate(values)
}

func (e *event) findOrCreate(values []string) Counter {
	var (
		h        = valuesHash(values)
		counters []Counter
		c        Counter
		v        []string
		i        int
	)
	e.mu.RLock()
	counters = e.counters[h]
a:
	for _, c = range counters {
		if v = c.Values(); len(v) == len(values) {
			v = v[:len(values)]
			for i = range values {
				if v[i] != values[i] {
					continue a
				}
			}
			e.mu.RUnlock()
			return c
		}
	}
	e.mu.RUnlock()
	e.mu.Lock()
	counters = e.counters[h]
	if counters != nil {
	b:
		for _, c = range counters {
			if v = c.Values(); len(v) == len(values) {
				v = v[:len(values)]
				for i = range values {
					if v[i] != values[i] {
						continue b
					}
				}
				e.mu.Unlock()
				return c
			}
		}
	}
	c = newSafeCounter(values...)
	e.counters[h] = append(e.counters[h], c)
	e.mu.Unlock()
	return c
}

func (e *event) Describe() *Desc {
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
