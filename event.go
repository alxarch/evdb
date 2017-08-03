package meter

import (
	"bytes"
	"errors"
	"net"
	"regexp"
	"strings"
	"text/template"
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
	nameTpl     *template.Template
	dimensions  []Dimension
}

// var isTemplateRX = regexp.MustCompile("\\{\\{[^\\}]+\\}\\}")

const LabelSeparator = '\x1f'
const FieldTerminator = '\x1e'
const sFieldTerminator = string(FieldTerminator)
const sLabelSeparator = string(LabelSeparator)

// func (labels Labels) Field(b []byte) string {
// 	n := len(labels)
// 	n -= n % 2
// 	switch n {
// 	case 0:
// 		return string(FieldTerminator)
// 	case 2:
// 		// Special case for common small values.
// 		// Remove if golang.org/issue/6714 is fixed
// 		return labels[0] + sLabelSeparator + labels[1] + sFieldTerminator
// 	}
// 	if b == nil {
// 		size := n
// 		for i := 0; i < n; i++ {
// 			size += len(labels[i])
// 		}
// 		b = make([]byte, size)
// 	}
// 	bp := copy(b, labels[0])
// 	for _, s := range labels[1:n] {
// 		b[bp] = LabelSeparator
// 		bp++
// 		bp += copy(b[bp:], s)
// 	}
// 	return string(b)
//
// }
func Field(labels []string) string {
	n := len(labels)
	n -= n % 2
	field := strings.Join(labels[:n], string(LabelSeparator))
	return field + string(FieldTerminator)
}

func parseValueField(field string) []string {
	return strings.Split(field, sLabelSeparator)
}

func ParseField(field string) Labels {
	if field == "*" {
		return Labels{}
	}

	// Strip FieldTerminator
	if n := len(field) - 1; n != -1 && field[n] == FieldTerminator {
		field = field[:n]
	}

	tmp := strings.Split(field, string(LabelSeparator))
	n := len(tmp)
	n -= n % 2
	labels := Labels(make(map[string]string, n/2))
	for i := 0; i < n; i += 2 {
		labels[tmp[i]] = tmp[i+1]
	}
	return labels
}

// func IsTemplateName(name string) bool {
// 	return isTemplateRX.MatchString(name)
// }

func unqLabels(labels ...string) []string {
	unq := make(map[string]bool, len(labels))
	n := 0
	for _, label := range labels {
		if unq[label] {
			continue
		}
		unq[label] = true
		labels[n] = label
		n++
	}
	return labels[:n]
}

func NewEvent(name string, labels []string, res ...*Resolution) *Event {
	labels = unqLabels(labels...)
	e := &Event{
		name:        name,
		counters:    NewCounters(),
		labels:      labels,
		dimensions:  LabelDimensions(labels...),
		index:       make(map[string]int),
		resolutions: make([]*Resolution, len(res)),
	}
	if tpl, err := NameTemplate(name); err == nil {
		e.nameTpl = tpl
	}

	for i, label := range labels {
		e.index[label] = i
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

func (e *Event) Labels(values []string) Labels {
	m := make(map[string]string, len(e.index))
	for label, i := range e.index {
		if i < len(values) {
			m[label] = values[i]
		}
	}
	return m
}

func (e *Event) AliasedLabels(input map[string]string, aliases Aliases) (labels Labels) {
	labels = Labels(make(map[string]string, len(e.labels)))
	for key, value := range input {
		key = aliases.Alias(key)
		if _, ok := e.index[key]; ok {
			labels[key] = value
		}
	}
	return
}

func (e *Event) MatchField(group string, input Labels) string {
	labels := make([]string, 2*len(e.labels))
	n := 0
	for k, v := range input {
		if k == group {
			labels[n] = k
			n++
			labels[n] = "*"
			n++
			continue
		}
		switch v {
		case "", "*":
			continue
		default:
			if _, ok := e.index[k]; ok {
				labels[n] = k
				n++
				labels[n] = regexp.QuoteMeta(v)
				n++
			}
		}
	}
	if n == 0 {
		return "\\*"
	}
	return Field(labels[:n])
}

func (e *Event) Pairs(input Labels) []string {
	labels := make([]string, 2*len(e.labels))
	n := 0
	for k, v := range input {
		if _, ok := e.index[k]; ok && v != "" {
			labels[n] = k
			n++
			labels[n] = v
			n++
		}
	}
	return labels[:n]
}

func (e *Event) Field(input Labels) string {
	if pairs := e.Pairs(input); len(pairs) > 0 {
		return Field(pairs)
	}
	return "*"
}

// func Replacer(labels ...string) *strings.Replacer {
// 	n := len(labels)
// 	n = n - (n % 2)
// 	r := make([]string, n)
// 	for i := 0; i < n; i++ {
// 		if (i % 2) == 0 {
// 			r[i] = fmt.Sprintf("{{%s}}", labels[i])
// 		} else {
// 			if v := labels[i]; v == "" {
// 				r[i] = "*"
// 			} else {
// 				r[i] = labels[i]
// 			}
// 		}
// 	}
// 	return strings.NewReplacer(r...)
// }

// func (e *Event) Replacer(labels map[string]string) *strings.Replacer {
// 	n := len(e.labels)
// 	tmp := make([]string, 2*n)
// 	i := 0
// 	for _, label := range e.labels {
// 		tmp[i] = label
// 		i++
// 		tmp[i] = labels[label]
// 		i++
// 	}
// 	return strings.NewReplacer(tmp...)
// }

var emptyLabels = Labels(map[string]string{})

func (e *Event) EventName(labels map[string]string) string {
	if e.nameTpl != nil {
		if labels == nil {
			labels = emptyLabels
		}
		b := bytes.NewBuffer([]byte(e.name)[:0])
		if err := e.nameTpl.Execute(b, labels); err == nil {
			return b.String()
		} else {
			return err.Error()
		}
	}
	return e.name
}

func (e *Event) HasLabel(a string) bool {
	_, ok := e.index[a]
	return ok
}

func (e *Event) Record(r *Resolution, t time.Time, labels Labels) *Record {
	name := e.EventName(labels)
	return &Record{
		Name:   name,
		Key:    r.Key(name, t),
		Field:  e.Field(labels),
		Time:   t,
		Labels: labels,
	}
}

func (e *Event) Records(res *Resolution, start, end time.Time, queries ...map[string]string) []*Record {
	if res == nil {
		return nil
	}
	ts := res.TimeSequence(start, end)
	if len(ts) == 0 {
		ts = append(ts, res.Round(time.Now()))
	}
	results := make([]*Record, 0, len(queries)*(len(ts)+1))
	for _, labels := range queries {
		labels = e.AliasedLabels(labels, defaultAliases)
		name := e.EventName(labels)
		field := e.Field(labels)
		for _, tm := range ts {
			results = append(results, &Record{
				Name:   name,
				Time:   tm,
				Key:    res.Key(name, tm),
				Field:  field,
				Labels: labels,
			})
		}
	}
	return results
}

func (e *Event) valueField(values []string) string {
	if n := len(e.labels); len(values) > n {
		values = values[:n]
	}
	return strings.Join(values, sLabelSeparator)
}

func (e *Event) LabelValues(labels Labels) []string {
	values := make([]string, len(e.labels))
	if len(labels) > 0 {
		for i, label := range e.labels {
			values[i] = labels[label]
		}
	}
	return values
}

func (e *Event) LogWithLabelValues(n int64, labels ...string) {
	e.counters.Increment(e.valueField(labels), n)
}

func (e *Event) LogWith(n int64, labels Labels) {
	e.counters.Increment(e.valueField(e.LabelValues(labels)), n)
}

func (e *Event) MustPersist(tm time.Time, r *redis.Client) {
	if err := e.Persist(tm, r); err != nil {
		panic(err)
	}
}

var (
	NilEventError          = errors.New("Event is nil.")
	NilResolutionError     = errors.New("Resolution is nil.")
	InvalidEventLabelError = errors.New("Invalid event label.")
)

func (e *Event) DimField(dim Dimension, q map[string]string) (field string, ok bool) {
	labels := make([]string, 2*len(e.labels))
	n := 0 // number of matched labels
	i := 0 // size of labels
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
		field = Field(labels[:i])
	}
	return
}

func (e *Event) AllField() string {
	return "*"
}

func (e *Event) Persist(tm time.Time, r redis.UniversalClient) (err error) {
	if e == nil {
		return NilEventError
	}
	b := e.counters.Flush()
	if len(b) == 0 {
		return nil
	}
	defer func() {
		if err != nil {
			// In case of network errors re-add events to be peristed later on
			if _, ok := err.(net.Error); ok {
				e.counters.BatchIncrement(b)
			}
		}
	}()
	keys := make(map[string]time.Duration, len(b))
	all := e.AllField()
	dims := e.dimensions
	p := r.Pipeline()
	defer p.Close()
	for fields, val := range b {
		values := parseValueField(fields)
		labels := e.Labels(values)
		name := e.EventName(labels)
		empty := len(values) == 0
		for _, res := range e.resolutions {
			if res == nil {
				continue
			}
			key := res.Key(name, tm)
			keys[key] = res.TTL()
			p.HIncrBy(key, all, val)
			if empty {
				continue
			}
			for _, dim := range dims {
				if field, ok := dim.Field(labels); ok {
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
	_, err = p.Exec()
	return
}

func (e *Event) Key(res *Resolution, tm time.Time, labels Labels) string {
	return res.Key(e.EventName(labels), tm)
}

func (e *Event) Snapshot() map[string]int64 {
	return e.counters.Snapshot()
}

func (e *Event) ValueIndex(label string) int {
	if i, ok := e.index[label]; ok {
		return i
	}
	return -1
}
