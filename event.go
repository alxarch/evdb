package meter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type Event struct {
	name     string
	counters *Counters
	labels   []string
	// pool        *sync.Pool
	index       map[string]int
	resolutions []*Resolution
}

var isTemplateRX = regexp.MustCompile("\\{\\{[^\\}]+\\}\\}")

func IsTemplateName(name string) bool {
	return isTemplateRX.MatchString(name)
}

func NewEvent(name string, labels []string, res ...*Resolution) *Event {
	e := &Event{
		name:     name,
		counters: NewCounters(),
		labels:   NormalizeLabels(labels...),
		index:    make(map[string]int),
	}
	for i, label := range e.labels {
		e.index[label] = 2*i + 1
	}
	// e.pool = &sync.Pool{
	// 	New: func() interface{} {
	// 		labels := make([]string, 2*len(e.labels))
	// 		for label, i := range e.index {
	// 			labels[i-1] = label
	// 			labels[i] = "*"
	// 		}
	// 		return Labels(labels)
	// 	},
	// }
	for _, r := range res {
		if r != nil {
			e.resolutions = append(e.resolutions, r)
		}
	}
	return e

}
func (e *Event) blankLabels() Labels {
	// labels := e.pool.Get().(Labels)
	labels := make([]string, 2*len(e.labels))
	for label, i := range e.index {
		labels[i] = "*"
		labels[i-1] = label
	}
	return labels
}

// func (e *Event) put(labels Labels) {
// 	n := 2 * len(e.labels)
// 	if cap(labels) < n {
// 		return
// 	}
// 	e.pool.Put(labels[:n])
// }

func (e *Event) AliasedLabels(input []string, aliases Aliases) (labels Labels) {
	labels = e.blankLabels()
	n := len(input)
	n = n - (n % 2)
	for i := 0; i < n; i += 2 {
		a := aliases.Alias(input[i])
		if j, ok := e.index[a]; ok {
			labels[j] = input[i+1]
		}
	}
	return
}

func (e *Event) Labels(input ...string) (labels []string) {
	return e.AliasedLabels(input, nil)
}
func (e *Event) field(labels, input []string) string {
	n := len(input)
	n = n - (n % 2)
	j := 0
	for i := 0; i < n; i += 2 {
		k, v := input[i], input[i+1]
		switch v {
		case "", "*":
			continue
		default:
			if _, ok := e.index[k]; ok {
				labels[j] = k
				j++
				labels[j] = v
				j++
			}
		}
	}
	if j == 0 {
		return "*"
	}
	return strings.Join(labels[:j], ":")
}
func (e *Event) Field(input ...string) string {
	labels := make([]string, 2*len(e.labels))
	return e.field(labels, input)
}

func Replacer(labels ...string) *strings.Replacer {
	n := len(labels)
	n = n - (n % 2)
	r := make([]string, n)
	for i := 0; i < n; i++ {
		if (i % 2) == 0 {
			r[i] = fmt.Sprintf("{{%s}}", labels[i])
		} else {
			if v := labels[i]; v == "" {
				r[i] = "*"
			} else {
				r[i] = labels[i]
			}
		}
	}
	return strings.NewReplacer(r...)
}

func (e *Event) EventName(labels Labels) string {
	if IsTemplateName(e.name) {
		return Replacer(labels...).Replace(e.name)
	}
	return e.name
}

func (e *Event) HasLabel(a string) bool {
	_, ok := e.index[a]
	return ok
}

func (e *Event) Record(r *Resolution, t time.Time, labels []string) *Record {
	name := e.EventName(labels)
	return &Record{
		Name:   name,
		Key:    r.Key(name, t),
		Field:  e.Field(labels...),
		Time:   t,
		Labels: labels,
	}
}

func (e *Event) Records(res *Resolution, start, end time.Time, queries ...[]string) []*Record {
	if res == nil {
		return nil
	}
	ts := res.TimeSequence(start, end)
	if len(ts) == 0 {
		ts = append(ts, res.Round(time.Now()))
	}
	results := make([]*Record, 0, len(queries)*(len(ts)+1))
	for _, labels := range queries {
		labels = e.Labels(labels...)
		for _, tm := range ts {
			results = append(results, e.Record(res, tm, labels))
		}
	}
	return results
}

func (e *Event) Log(n int64, labels ...string) {
	e.counters.Increment(strings.Join(labels, labelSeparator), n)
}

const labelSeparator = "\x00"

func (e *Event) MustPersist(tm time.Time, r *redis.Client) {
	if err := e.Persist(tm, r); err != nil {
		panic(err)
	}
}

var NilEventError = errors.New("Event is nil.")

func (e *Event) DimField(dim Dimension, q map[string]string) (field string, ok bool) {
	labels := e.blankLabels()
	// defer e.put(labels)
	n := 0
	i := 0
	for _, label := range dim {
		if _, hasLabel := e.index[label]; hasLabel {
			if v := q[label]; v != "" && v != "*" {
				labels[i] = label
				i++
				labels[i] = v
				i++
				n++
			}
		}
	}
	if n == len(dim) {
		ok = true
		field = strings.Join(labels[:i], ":")
	}
	return
}

func (e *Event) AllField() string {
	return "*"
}

func (e *Event) Persist(tm time.Time, r *redis.Client) error {
	if e == nil {
		return NilEventError
	}
	keys := make(map[string]time.Duration)
	all := e.AllField()
	dims := LabelDimensions(e.labels...)
	p := r.Pipeline()
	defer p.Close()
	b := e.counters.Flush()
	if len(b) == 0 {
		return nil
	}
	for fields, val := range b {
		labels := strings.Split(fields, labelSeparator)
		q := Labels(labels).Map()
		name := e.EventName(labels)
		for _, res := range e.resolutions {
			if res == nil {
				continue
			}
			key := res.Key(name, tm)
			keys[key] = res.TTL()
			p.HIncrBy(key, all, val)
			if fields == "" {
				continue
			}
			for _, dim := range dims {
				if field, ok := e.DimField(dim, q); ok {
					p.HIncrBy(key, field, val)
				}
			}
		}
	}
	for k, ttl := range keys {
		if ttl > 0 {
			p.PExpire(k, ttl)
		}
	}
	_, err := p.Exec()
	return err
}

func (e *Event) Key(res *Resolution, tm time.Time, labels Labels) string {
	return res.Key(e.EventName(labels), tm)
}

func (e *Event) Snapshot() map[string]int64 {
	return e.counters.Snapshot()
}
