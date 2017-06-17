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

func (t *EventType) increment(labels []string, n int64) {
	t.counters.Increment(strings.Join(labels, labelSeparator), n)
}

const labelSeparator = string('0')

func (t *EventType) Persist(tm time.Time, r *redis.Client) error {
	p := r.TxPipeline()
	defer p.Close()
	b := t.counters.Flush()
	if len(b) == 0 {
		return nil
	}
	keys := make(map[string]time.Duration)
	tmp := make([]string, 2*t.maxdimsize)
	defer t.Put(tmp)
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
