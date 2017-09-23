package meter2

import (
	"errors"
	"sync"
)

type Event interface {
	WithLabels(labels LabelValues) Metric
	WithLabelValues(values ...string) Metric
	Descriptor
	Collector
}

type Resetable interface {
	Reset()
}

var (
	ErrInvalidEventLabel = errors.New("Invalid event label.")
)

var nilDesc = &Desc{err: ErrNilDesc}

func NewCounterEvent(desc *Desc) Event {
	if desc == nil {
		desc = nilDesc
	}
	return newCounterEvent(desc, MetricTypeIncrement)
}
func NewEvent(desc *Desc, t MetricType) Event {
	if desc == nil {
		desc = nilDesc
	}
	return newCounterEvent(desc, t)

}

type counterEvent struct {
	values map[uint64][]*Counter
	t      MetricType
	mu     sync.RWMutex
	desc   *Desc
}

// Reset clears all stored counters
// WARNING: If a counter is referenced elsewere it will not be collected by Collect()
func (c *counterEvent) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values = make(map[uint64][]*Counter)
}

var _ Event = &counterEvent{}

func newCounterEvent(d *Desc, t MetricType) *counterEvent {
	return &counterEvent{
		values: make(map[uint64][]*Counter),
		t:      t,
		desc:   d,
	}
}

func (e *counterEvent) Collect(ch chan<- Metric) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.values == nil {
		return
	}
	for _, counters := range e.values {
		for _, c := range counters {
			ch <- c
		}
	}
}

func (e *counterEvent) WithLabels(values LabelValues) Metric {
	lv := values.Values(e.desc.labels)
	m, _ := e.FindOrCreate(lv)
	return m
}
func (e *counterEvent) WithLabelValues(values ...string) Metric {
	m, _ := e.FindOrCreate(values)
	return m
}

func (e *counterEvent) FindOrCreate(values []string) (m Metric, created bool) {
	h := valuesHash(values)
	var v *Counter
	if v = e.Find(h, values); v == nil {
		e.mu.Lock()
		if v = e.find(h, values); v == nil {
			v = newCounter(e.desc, e.t, values...)
			e.values[h] = append(e.values[h], v)
			created = true
		}
		e.mu.Unlock()
	}
	return v, created
}
func (e *counterEvent) find(h uint64, values []string) *Counter {
	collisions := e.values[h]
	if collisions != nil {
		for _, c := range collisions {
			if c.matches(values) {
				return c
			}
		}
	}
	return nil
}

func (e *counterEvent) Find(h uint64, values []string) *Counter {
	e.mu.RLock()
	collisions := e.values[h]
	e.mu.RUnlock()
	if collisions != nil {
		for _, c := range collisions {
			if c.matches(values) {
				return c
			}
		}
	}
	return nil
}

func (e *counterEvent) Describe() *Desc {
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
