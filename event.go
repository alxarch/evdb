package meter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type Event struct {
	name     string
	counters *Counters
	labels   []string
	pool     *sync.Pool
	index    map[string]int
}

var isTemplateRX = regexp.MustCompile("\\{\\{[^\\}]+\\}\\}")

func IsTemplateName(name string) bool {
	return isTemplateRX.MatchString(name)
}

func NewEvent(name string, labels ...string) *Event {
	e := &Event{
		name:     name,
		counters: NewCounters(),
		labels:   NormalizeLabels(labels...),
		index:    make(map[string]int),
	}
	for i, label := range e.labels {
		e.index[label] = i
	}
	e.pool = &sync.Pool{
		New: func() interface{} {
			return Labels(make([]string, 2*len(e.labels)))
		},
	}
	return e

}
func (e *Event) get() Labels {
	labels := e.pool.Get().(Labels)
	for label, i := range e.index {
		labels[i] = label
		labels[i+1] = "*"
	}
	return labels
}
func (e *Event) put(labels Labels) {
	n := 2 * len(e.labels)
	if cap(labels) < n {
		return
	}
	e.pool.Put(labels[:n])
}

func (e *Event) AliasedLabels(input []string, aliases Aliases) (labels Labels) {
	labels = e.get()
	n := len(input)
	n = n - (n % 2)
	for i := 0; i < n; i += 2 {
		a := aliases.Alias(input[i])
		if j, ok := e.index[a]; ok {
			labels[j+1] = input[i+1]
		}
	}
	return
}

func (e *Event) Labels(input []string) (labels []string) {
	return e.AliasedLabels(input, nil)
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

func (e *Event) EventName(labels ...string) string {
	if IsTemplateName(e.name) {
		return Replacer(labels...).Replace(e.name)
	}
	return e.name
}

func (e *Event) HasLabel(a string) bool {
	_, ok := e.index[a]
	return ok
}

func (e *Event) Record(r *Resolution, t time.Time, labels []string) Record {
	return Record{
		Key:    e.Key(r, t, labels),
		Field:  strings.Join(labels, ":"),
		Time:   t,
		Labels: labels,
	}
}

func (e *Event) Records(res *Resolution, start, end time.Time, queries [][]string) []Record {
	if res == nil {
		return nil
	}
	ts := res.TimeSequence(start, end)
	if len(ts) == 0 {
		ts = append(ts, res.Round(time.Now()))
	}
	results := make([]Record, 0, len(queries)*(len(ts)+1))
	for _, labels := range queries {
		labels = e.Labels(labels)
		for _, tm := range ts {
			results = append(results, e.Record(res, tm, labels))
		}
	}
	return results
}

func (e *Event) Log(n int64, labels ...string) {
	e.counters.Increment(strings.Join(labels, labelSeparator), n)
}

const labelSeparator = string('0')

func (e *Event) MustPersist(tm time.Time, r *redis.Client, resolutions ...*Resolution) {
	if err := e.Persist(tm, r, resolutions...); err != nil {
		panic(err)
	}
}

var NilEventError = errors.New("Event is nil.")

func (e *Event) DimField(dim Dimension, q map[string]string) (field string, ok bool) {
	labels := e.get()
	defer e.put(labels)
	n := 0
	for _, label := range dim {
		if i, hasLabel := e.index[label]; hasLabel {
			if v := q[label]; v != "" {
				labels[i+1] = v
				n++
			}
		}
	}
	if n == len(dim) {
		ok = true
		field = strings.Join(labels, ":")
	}
	return
}

func (e *Event) AllField() string {
	labels := e.get()
	defer e.put(labels)
	return strings.Join(labels, ":")
}
func (e *Event) Persist(tm time.Time, r *redis.Client, resolutions ...*Resolution) error {
	if e == nil {
		return NilEventError
	}
	// Use a transaction to ensure each event type is persisted entirely
	b := e.counters.Flush()
	if len(b) == 0 {
		return nil
	}
	p := r.TxPipeline()
	defer p.Close()
	keys := make(map[string]time.Duration)
	all := e.AllField()
	dims := LabelDimensions(e.labels...)
	for fields, val := range b {
		labels := strings.Split(fields, labelSeparator)
		q := Labels(labels).Map()
		name := e.EventName(labels...)
		for _, res := range resolutions {
			if res == nil {
				continue
			}
			key := res.Key(name, tm)
			keys[key] = res.TTL()
			p.HIncrBy(key, all, val)
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
	if err != nil {
		e.counters.BatchIncrement(b)
	}
	return err
}

func (e *Event) Key(res *Resolution, tm time.Time, labels []string) string {
	return res.Key(e.EventName(labels...), tm)
}
