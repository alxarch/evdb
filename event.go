package meter

import (
	"strings"
	"time"
)

type Event struct {
	Type       *EventType
	Count      int64
	Time       time.Time
	Attributes Attributes
}

func NewEvent(t *EventType, attr []string) *Event {
	return &Event{
		Type:       t,
		Time:       time.Now(),
		Count:      1,
		Attributes: CopyAttributes(attr),
	}
}

func (e *Event) Set(pairs ...string) *Event {
	e.Attributes = e.Attributes.Set(pairs...)
	return e
}

func (e *Event) increment(counters TTLCounters, f *Filter, q map[string]string, n int64, attr Attributes) int64 {
	if nil == f {
		return 0
	}
	res := f.Resolution()
	if nil == res {
		return 0
	}
	t := e.Type
	name := t.EventName(q)
	key := res.Key(name, e.Time)
	ttl := f.MaxAge()
	// debug("sk %s %s", key, q)
	counters.Increment(key, "*", n, ttl)
	if len(attr) == 0 {
		return 0
	}
	s := int64(0)
dim_loop:
	for _, dim := range f.Dimensions() {
		i := 0
		for _, d := range dim {
			attr[i] = d
			if value := q[d]; value != "" {
				i++
				attr[i] = value
				i++
			} else {
				continue dim_loop
			}
		}
		field := strings.Join(attr[:i], ":")
		// debug("sk %s", field)
		s += 1
		counters.Increment(key, field, n, ttl)
	}
	return s
}

func (e *Event) Increment(counters TTLCounters) int64 {
	if e.Type == nil {
		return 0
	}
	filters := e.Type.Filters()
	if filters == nil {
		return 0
	}
	q := e.Type.FilterAttributes(e.Attributes)
	// debug("%v", q)
	n := e.Count
	if n == 0 {
		n = 1
	}
	s := int64(0)
	attr := defaultPool.Get(2 * e.Type.MaxDimSize())
	defer defaultPool.Put(attr)
	for _, f := range filters {
		s += e.increment(counters, f, q, n, attr)
	}
	return s
}
