package meter

import (
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type EventType struct {
	name       string
	nameParams []string
	filters    []*Filter
	maxdimsize int
	counters   *Counters
	pool       *sync.Pool
	aliases    Aliases
	labels     map[string]int
}

func NewEventType(name string, nameParams []string, aliases Aliases, filters ...*Filter) *EventType {
	t := &EventType{
		name:       name,
		nameParams: nameParams,
		filters:    filters,
		aliases:    aliases,
		counters:   NewCounters(),
	}
	maxdimsize := 0
	n := 0
	needed := make(map[string]int)
	for _, f := range t.filters {
		for _, dim := range f.Dimensions() {
			size := len(dim)
			if size > maxdimsize {
				maxdimsize = size
			}
			for _, a := range dim {
				if _, ok := needed[a]; !ok {
					needed[a] = 2 * n
					n++
				}
			}
		}
	}
	for _, p := range nameParams {
		if _, ok := needed[p]; !ok {
			needed[p] = 2 * n
			n++
		}
	}
	t.labels = needed
	t.maxdimsize = maxdimsize
	t.pool = &sync.Pool{
		New: func() interface{} {
			labels := make([]string, 2*n)
			for label, i := range t.labels {
				labels[i] = label
			}
			return labels
		},
	}
	return t

}
func (t *EventType) Put(labels []string) {
	n := len(t.labels)
	if cap(labels) < n {
		return
	}
	t.pool.Put(labels[:n])
}

func (t *EventType) MatchDim(labels []string, dim []string) []string {
	dl := t.pool.Get().([]string)
	n := 0
	for _, d := range dim {
		if i, ok := t.labels[d]; ok {
			if i++; i < len(labels) && labels[i] != "" {
				dl[n] = d
				n++
				dl[n] = labels[i]
				n++
			}
			t.Put(labels)
			return nil
		}
	}
	return dl[:n]

}
func (t *EventType) Labels(input []string, aliases Aliases) (labels []string) {
	labels = t.pool.Get().([]string)
	for label, i := range t.labels {
		labels[i] = label
		labels[i+1] = ""
	}
	n := len(input)
	n = n - n%2
	for i := 0; i < n; i += 2 {
		a := aliases.Alias(input[i])
		if j, ok := t.labels[a]; ok {
			labels[j+1] = input[i+1]
		}
	}
	return
}

func (t *EventType) EventNameLabels(labels []string) string {
	if dim := t.nameParams; len(dim) > 0 {
		pairs := make([]string, len(dim)*2+1)
		pairs[0] = t.name
		i := 1
		for _, d := range dim {
			j := t.labels[d]
			v := labels[j+1]
			if v == "" {
				v = "*"
			}
			pairs[i] = d
			i++
			pairs[i] = v
			i++
		}
		return strings.Join(pairs, ":")

	}

	return t.name
}
func (t *EventType) EventName(q map[string]string) string {
	if dim := t.nameParams; len(dim) > 0 {
		pairs := make([]string, len(dim)*2+1)
		pairs[0] = t.name
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
		return strings.Join(pairs, ":")

	}

	return t.name
}

func (t *EventType) NeedsAttr(a string) bool {
	for _, f := range t.filters {
		if f.NeedsAttr(a) {
			return true
		}
	}
	return false

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

func (t *EventType) MaxDimSize() int {
	return t.maxdimsize
}

const (
	AttrSkip int = iota
	AttrOptional
	AttrRequired
)

func (t *EventType) RequiresAttr(a string) bool {
	for _, r := range t.nameParams {
		if r == a {
			return true
		}
	}
	return false
}

func (t *EventType) Records(res *Resolution, start, end time.Time, queries [][]string) []Record {
	if res == nil {
		return nil
	}
	ts := res.TimeSequence(start, end)
	if len(ts) == 0 {
		ts = append(ts, res.Round(time.Now()))
	}
	results := make([]Record, 0, len(queries)*(len(ts)+1))
	for _, attr := range queries {
		labels := t.Labels(attr, nil)
		name := t.EventNameLabels(labels)
		for _, f := range t.filters {
			if f.res != res {
				continue

			}
			for _, dim := range f.dims {
				dl := t.MatchDim(labels, dim)
				if dl == nil {
					continue
				}
				field := "*"
				if len(dl) > 0 {
					field = strings.Join(dl, ":")
				}
				for _, tm := range ts {
					key := res.Key(name, tm)
					r := Record{
						Name:   name,
						Time:   tm,
						Labels: dl,
						Field:  field,
						Key:    key,
					}
					results = append(results, r)
				}
			}
		}
	}
	return results
}

func (t *EventType) Filters() []*Filter {
	return t.filters
}

func (t *EventType) increment(labels []string, n float64) {
	t.counters.Increment(strings.Join(labels, labelSeparator), n)
}

// func (t *EventType) Increment(q map[string]string, n float64) {
// 	attr := defaultPool.Get(2 * t.MaxDimSize())
// 	defer defaultPool.Put(attr)
// 	name := t.EventName(q)
// 	// debug("sk %s %s", key, q)
// 	t.counters.Increment(name, "*", n)
// 	if len(q) == 0 {
// 		return
// 	}
// 	for _, f := range t.filters {
// 	dim_loop:
// 		for _, dim := range f.Dimensions() {
// 			i := 0
// 			for _, d := range dim {
// 				attr[i] = d
// 				if value := q[d]; value != "" {
// 					i++
// 					attr[i] = value
// 					i++
// 				} else {
// 					continue dim_loop
// 				}
// 			}
// 			field := strings.Join(attr[:i], ":")
// 			// debug("sk %s", field)
// 			t.counters.Increment(name, field, n)
// 		}
// 	}
// }

// func (t *EventType) batch(b Batch, now time.Time) Batch {
// 	out := make(map[CounterKey]float64)
// 	for key, val := range b {
// 		for _, f := range t.filters {
// 			res := f.Resolution()
// 			k := CounterKey{res.Key(key.name, now), key.field}
// 			out[k] = val
// 		}
// 	}
// 	return out
// }
//
// func (t *EventType) Batch(now time.Time) Batch {
// 	return t.batch(t.counters.Batch(), now)
// }
//
// func (t *EventType) Flush(now time.Time) Batch {
// 	return t.batch(t.counters.Flush(), now)
// }
//
const labelSeparator = string('0')

func (t *EventType) Persist(tm time.Time, r *redis.Client) error {
	p := r.TxPipeline()
	defer p.Close()
	b := t.counters.Flush()
	if len(b) == 0 {
		return nil
	}
	keys := make(map[string]time.Duration)
	attr := make([]string, 2*t.maxdimsize)
	defer t.Put(attr)
	for fields, val := range b {
		labels := strings.Split(fields, labelSeparator)
		q := Attributes(labels).Map()
		name := t.EventName(q)
		for _, f := range t.filters {
			res := f.Resolution()
			key := res.Key(name, tm)
			keys[key] = f.MaxAge()
			p.HIncrByFloat(key, "*", val)
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
				p.HIncrByFloat(key, field, val)
			}
		}
	}
	for k, ttl := range keys {
		p.PExpire(k, ttl)
	}
	_, err := p.Exec()
	if err != nil {
		t.counters.BatchIncrement(b)
	}
	return err
}
