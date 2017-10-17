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

func (e *event) findOrCreate(values []string) (c Counter) {
	h := valuesHash(values)
	e.mu.RLock()
	counters := e.counters[h]
	for i := 0; i < len(counters); i++ {
		if c = counters[i]; matchValues(c.Values(), values) {
			e.mu.RUnlock()
			return
		}
	}
	e.mu.RUnlock()
	e.mu.Lock()
	if c = e.find(h, values); c == nil {
		c = newSafeCounter(values...)
		e.counters[h] = append(e.counters[h], c)
	}
	e.mu.Unlock()
	return
}

func (e *event) find(h uint64, values []string) Counter {
	if counters := e.counters[h]; counters != nil {
		for _, c := range counters {
			if matchValues(c.Values(), values) {
				return c
			}
		}
	}
	return nil
}

func (e *event) Describe() *Desc {
	return e.desc
}

const separatorByte byte = 255

func valuesHash(values []string) (h uint64) {
	h = hashNew()
	for i := 0; i < len(values); i++ {
		h = hashAdd(h, values[i])
		h = hashAddByte(h, separatorByte)
	}
	return h
}

func indexOf(values []string, s string) int {
	for i := 0; i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

func distinct(values ...string) []string {
	j := 0
	for _, value := range values {
		if indexOf(values[:j], value) == -1 {
			values[j] = value
			j++
		}
	}
	return values[:j]
}
