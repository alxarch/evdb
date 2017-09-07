package meter2

import (
	"errors"
	"sync"
)

type Event interface {
	WithLabels(labels LabelValues) Metric
	WithLabelValues(values []string) Metric
	Descriptor
	Collector
}

var (
	ErrInvalidEventLabel = errors.New("Invalid event label.")
)

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
		if indexOf(values[:j], value) != -1 {
			continue
		}
		values[j] = value
		j++
	}
	return values[:j]

}

var nilDesc = &Desc{err: ErrNilDesc}

func NewEvent(desc *Desc) Event {
	if desc == nil {
		desc = nilDesc
	}
	return newCounterEvent(desc)
}

func checkDistinct(values []string) int {
	for i, value := range values {
		if indexOf(values[:i], value) != -1 {
			return i
		}
	}
	return -1
}

type counterEvent struct {
	values map[uint64][]*Counter
	mu     sync.RWMutex
	desc   *Desc
}

var _ Event = &counterEvent{}

func newCounterEvent(d *Desc) *counterEvent {
	return &counterEvent{
		values: make(map[uint64][]*Counter),
		desc:   d,
	}
}

func (e *counterEvent) Collect(ch chan<- Metric) {
	e.mu.RLock()
	for _, counters := range e.values {
		for _, c := range counters {
			ch <- c
		}
	}
}

func (e *counterEvent) WithLabels(values LabelValues) Metric {
	return e.WithLabelValues(values.Values(e.desc.labels))
}
func (e *counterEvent) WithLabelValues(values []string) Metric {
	m, _ := e.FindOrCreate(values, e.desc)
	return m
}

func (e *counterEvent) FindOrCreate(values []string, d Descriptor) (m Metric, created bool) {
	h := valuesHash(values)
	var v *Counter
	if v = e.find(h, values); v == nil {
		e.mu.Lock()
		if v = e.find(h, values); v == nil {
			v = NewCounter(values...)
			v.desc = d.Describe()
			e.values[h] = append(e.values[h], v)
			created = true
		}
		e.mu.Unlock()
	}
	return v, created
}

func (e *counterEvent) find(h uint64, values []string) *Counter {
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
	for _, v := range values {
		h = hashAdd(h, v)
		h = hashAddByte(h, separatorByte)
	}
	return h
}
