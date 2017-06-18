package meter

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type Labels []string

func (labels Labels) Map() map[string]string {
	n := len(labels)
	n = n - n%2
	labels = labels[:n]
	m := make(map[string]string)
	if len(labels) > 0 {
		for i := 0; i < n; i += 2 {
			m[labels[i]] = labels[i+1]
		}
	}
	return m
}

type Event struct {
	name       string
	nameParams []string
	filters    []*Filter
	maxdimsize int
	counters   *Counters
	pool       *sync.Pool
	labels     map[string]int
}

func NewEvent(name string, nameParams []string, filters ...*Filter) *Event {
	t := &Event{
		name:       name,
		nameParams: make([]string, 0, len(nameParams)),
		filters:    make([]*Filter, 0, len(filters)),
		counters:   NewCounters(),
	}
	maxdimsize := 0
	n := 0
	needed := make(map[string]int)
	for _, f := range filters {
		if f == nil {
			continue
		}
		t.filters = append(t.filters, f)
		for _, dim := range f.Dimensions() {
			size := len(dim)
			if size == 0 {
				continue
			}
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
	if len(nameParams) > 0 {
		for _, p := range nameParams {
			if p != "" {
				t.nameParams = append(t.nameParams, p)
				if _, ok := needed[p]; !ok {
					needed[p] = 2 * n
					n++
				}
			}
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
func (t *Event) put(labels []string) {
	n := len(t.labels)
	if cap(labels) < n {
		return
	}
	t.pool.Put(labels[:n])
}

func (t *Event) MatchDim(labels []string, dim []string) []string {
	dl := t.pool.Get().([]string)
	n := 0
	for _, d := range dim {
		if i, ok := t.labels[d]; ok {
			if i++; i < len(labels) && labels[i] != "" {
				dl[n] = d
				n++
				dl[n] = labels[i]
				n++
			} else {
				t.put(dl)
				return nil
			}
		}
	}
	return dl[:n]

}
func (t *Event) Labels(input []string, aliases Aliases) (labels []string) {
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

func (t *Event) EventNameLabels(labels []string) string {
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
func (t *Event) EventName(q map[string]string) string {
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

func (t *Event) HasLabel(a string) bool {
	_, ok := t.labels[a]
	return ok
}

func (t *Event) MaxDimSize() int {
	return t.maxdimsize
}

func (t *Event) Records(res *Resolution, start, end time.Time, queries [][]string) []Record {
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

func (t *Event) Filters() []*Filter {
	return t.filters
}

func (t *Event) Log(n int64, labels ...string) {
	t.counters.Increment(strings.Join(labels, labelSeparator), n)
}

const labelSeparator = string('0')

func (t *Event) MustPersist(tm time.Time, r *redis.Client) {
	if err := t.Persist(tm, r); err != nil {
		panic(err)
	}
}

var NilEventError = errors.New("Event is nil.")

func (t *Event) Persist(tm time.Time, r *redis.Client) error {
	if t == nil {
		return NilEventError
	}
	// Use a transaction to ensure each event type is persisted entirely
	p := r.TxPipeline()
	defer p.Close()
	b := t.counters.Flush()
	if len(b) == 0 {
		return nil
	}
	keys := make(map[string]time.Duration)
	tmp := make([]string, 2*t.maxdimsize)
	for fields, val := range b {
		labels := strings.Split(fields, labelSeparator)
		q := Labels(labels).Map()
		name := t.EventName(q)
		for _, f := range t.filters {
			res := f.Resolution()
			key := res.Key(name, tm)
			keys[key] = f.MaxAge()
			p.HIncrBy(key, "*", val)
		dim_loop:
			for _, dim := range f.Dimensions() {
				i := 0
				for _, d := range dim {
					tmp[i] = d
					if value := q[d]; value != "" {
						i++
						tmp[i] = value
						i++
					} else {
						continue dim_loop
					}
				}
				field := strings.Join(tmp[:i], ":")
				// debug("sk %s", field)
				p.HIncrBy(key, field, val)
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

func (e *Event) Key(res *Resolution, tm time.Time, labels ...string) string {
	return res.Key(e.EventNameLabels(labels), tm)
}
