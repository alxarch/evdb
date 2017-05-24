package meter

import (
	"strings"
	"time"
)

type EventType struct {
	Name        string
	NameParams  []string
	filters     []*Filter
	needed      map[string]bool
	initialized bool
	maxdimsize  int
}

func NewEventType(name string, filters ...*Filter) *EventType {
	t := &EventType{
		Name:    name,
		filters: filters,
	}
	t.init()
	return t

}

func (t *EventType) EventName(q map[string]string) string {
	if dim := t.NameParams; dim != nil {
		pairs := make([]string, len(dim)*2+1)
		pairs[0] = t.Name
		i := 1
		for _, d := range dim {
			v := q[d]
			if v == "" {
				v = "*"
			}
			pairs[i] = d
			i++
			pairs[i] = v
			i++
		}
		return Join(":", pairs...)

	}

	return t.Name
}

func (t *EventType) NeedsAttr(a string) bool {
	for _, f := range t.filters {
		if f.NeedsAttr(a) {
			return true
		}
	}
	return false

}

func CopyAttributes(attr []string) Attributes {
	n := len(attr)
	result := fullAttributes(n)
	copy(result, attr)
	return result[:n-n%2]
}

// FilterAttributes copies an attributes slice keeping only needed attributes
func (t *EventType) FilterAttributes(attr []string) (m map[string]string) {
	if n := len(attr); n > 0 {
		m = make(map[string]string)
		n -= n % 2
		for i := 0; i < n; i += 2 {
			if a := attr[i]; t.NeedsAttr(a) {
				// debug("needs %s", a)
				m[a] = attr[i+1]
			}
		}
	}
	return
}

func (t *EventType) init() {
	if t.initialized {
		return
	}
	maxdimsize := 0
	needed := make(map[string]bool)
	for _, f := range t.filters {
		for _, dim := range f.Dimensions() {
			size := len(dim)
			if size > maxdimsize {
				maxdimsize = size
			}
			for _, a := range dim {
				needed[a] = true
			}
		}
	}
	t.needed = needed
	t.maxdimsize = maxdimsize
	t.initialized = true
}
func (t *EventType) MaxDimSize() int {
	t.init()
	return t.maxdimsize
}

const (
	AttrSkip int = iota
	AttrOptional
	AttrRequired
)

func (t *EventType) RequiresAttr(a string) bool {
	for _, r := range t.NameParams {
		if r == a {
			return true
		}
	}
	return false
}

func (t *EventType) AttributeQuery(attr Attributes) map[string]string {
	q := make(map[string]string)
	n := len(attr)
	n -= n % 2
	for i := 0; i < n; i += 2 {
		k := attr[i]
		q[k] = attr[i+1]
	}
	return q
}

func (t *EventType) Records(filter *Filter, start, end time.Time, queries ...[]string) []*Record {
	if filter == nil {
		return nil
	}
	res := filter.Resolution()
	if res == nil {
		return nil
	}
	ts := res.TimeSequence(start, end)
	if len(ts) == 0 {
		ts = append(ts, res.Round(time.Now()))
	}
	results := make([]*Record, len(queries)*(len(ts)+1))
	i := 0
	for _, attr := range queries {
		q := t.AttributeQuery(attr)
		if labels := filter.MatchQuery(q); labels != nil {
			name := t.EventName(q)
			field := "*"
			if len(labels) > 0 {
				field = strings.Join(labels, ":")
			}
			for _, tm := range ts {
				key := res.Key(name, tm)
				r := NewRecord(name, tm, labels)
				r.Key = key
				r.Field = field
				results[i] = r
				i++
			}
		}
	}
	return results[:i]
}

func (t *EventType) Filters() []*Filter {
	return t.filters
}
