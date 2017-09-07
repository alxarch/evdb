package meter2

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	tc "github.com/alxarch/go-timecodec"
	"github.com/go-redis/redis"
)

const DefaultKeyPrefix = "meter:"

type DB struct {
	Redis     redis.UniversalClient
	registry  *Registry
	KeyPrefix string
}

func NewDB(r redis.UniversalClient) *DB {
	return &DB{r, defaultRegistry, DefaultKeyPrefix}
}

type Query struct {
	start, end time.Time
	event      string
	q          url.Values
	res        Resolution
}

type Record struct {
	name   string
	tm     time.Time
	key    string
	field  string
	values map[string]string
	err    error
	result *redis.StringCmd
}

func (r Record) Value() int64 {
	if r.result != nil {
		if n, err := r.result.Int64(); err == nil {
			return n
		}
	}
	return 0
}

func (r Record) MarshalJSON() ([]byte, error) {
	obj := make(map[string]string, (len(r.values) + 3))
	for k, v := range r.values {
		obj[k] = v
	}
	obj["count"] = strconv.FormatInt(r.Value(), 10)
	obj["time"] = r.tm.String()
	obj["name"] = r.name
	return json.Marshal(obj)
}

func (q Query) TimeSequence() []time.Time {
	return q.res.TimeSequence(q.start, q.end)
}

func (q Query) Values(d *Desc) []map[string]string {
	if d == nil {
		return nil
	}
	dims := d.Dimensions(q.res)
	if dims == nil {
		return nil
	}
	queries := d.MatchingQueries(q.q)
qloop:
	for label, _ := range queries {
		for _, dim := range dims {
			if dim.Contains(label) {
				continue qloop
			}
		}
		delete(queries, label)
	}
	return QueryPermutations(queries)
}
func (dim Dimension) MatchQuery(q map[string]string) bool {
	if q == nil {
		return false
	}
	for _, d := range dim {
		if _, ok := q[d]; !ok {
			return false
		}
	}
	return true
}
func (dims Dimensions) FirstMatch(q map[string]string) Dimension {
	if q == nil {
		return nil
	}
	for _, dim := range dims {
		if dim.MatchQuery(q) {
			return dim
		}
	}
	return nil
}

func (db *DB) Records(q Query, pipeline redis.Pipeliner, ch chan<- Record) {
	event := db.registry.Get(q.event)
	if event == nil {
		return
	}
	desc := event.Describe()
	if desc == nil {
		return
	}
	ts := q.TimeSequence()
	if len(ts) == 0 {
		return
	}
	queryValues := q.Values(desc)
	if len(queryValues) == 0 {
		return
	}
	size := len(ts) * len(queryValues)
	records := make([]Record, 0, size)
	if len(records) == 0 {
		return
	}
	b := bget()
	defer bput(b)
	for _, values := range queryValues {
		m := event.WithLabels(values)
		d := m.Describe()
		err := d.Error()
		if err != nil {
			continue
		}
		dims := d.Dimensions(q.res)
		if len(dims) == 0 {
			continue
		}
		dims.SortBy(nil)
		dim := dims.FirstMatch(values)
		if dim == nil {
			continue
		}
		r := Record{
			values: values,
			name:   d.Name(),
			err:    err,
		}
		b.Reset()
		db.AppendFieldMap(dim, values, b)
		r.field = b.String()
		for _, tm := range ts {
			r.tm = tm
			b.Reset()
			db.AppendKey(q.res, r.name, tm, b)
			r.key = b.String()
			if pipeline != nil {
				r.result = pipeline.HGet(r.key, r.field)
			}
			ch <- r
		}
	}
	return
}

func (db *DB) ReadRecords(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	p := db.Redis.Pipeline()
	defer p.Close()
	for _, r := range records {
		r.result = p.HGet(r.key, r.field)
	}
	_, err := p.Exec()
	if err == redis.Nil {
		return nil
	}
	return err
}

func (e *Desc) MatchingQueries(q url.Values) url.Values {
	if e == nil || q == nil {
		return nil
	}
	m := make(map[string][]string, len(q))
	for key, values := range q {
		if e.HasLabel(key) {
			m[key] = values
		}
	}
	return m
}

func TimeSequence(start time.Time, end time.Time, unit time.Duration) []time.Time {
	if unit == 0 {
		return []time.Time{}
	}
	start = tc.Round(start, unit).In(start.Location())
	end = tc.Round(end, unit).In(end.Location())
	n := end.Sub(start) / unit

	results := make([]time.Time, 0, n)

	for s := start; end.Sub(s) >= 0; s = s.Add(unit) {
		results = append(results, s)
	}
	return results
}

func QueryPermutations(input url.Values) []map[string]string {
	vcount := []int{}
	keys := []string{}
	combinations := [][]int{}
	for k, v := range input {
		if c := len(v); c > 0 {
			keys = append(keys, k)
			vcount = append(vcount, c)
		}
	}
	var generate func([]int)
	generate = func(comb []int) {
		if i := len(comb); i == len(vcount) {
			combinations = append(combinations, comb)
			return
		} else {
			for j := 0; j < vcount[i]; j++ {
				next := make([]int, i+1)
				if i > 0 {
					copy(next[:i], comb)
				}
				next[i] = j
				generate(next)
			}
		}
	}
	generate([]int{})
	results := make([]map[string]string, 0, len(combinations))
	for _, comb := range combinations {
		result := make(map[string]string, len(comb))
		for i, j := range comb {
			key := keys[i]
			result[key] = input[key][j]
		}
		if len(result) > 0 {
			results = append(results, result)
		}
	}
	return results
}

const LabelSeparator = '\x1f'
const FieldTerminator = '\x1e'

func (db DB) Key(r Resolution, event string, t time.Time) (k string) {
	b := bget()
	db.AppendKey(r, event, t, b)
	k = b.String()
	bput(b)
	return
}

func (db DB) AppendKey(r Resolution, event string, t time.Time, b *bytes.Buffer) {
	if db.KeyPrefix != "" {
		b.WriteString(db.KeyPrefix)
		b.WriteByte(':')
	}
	b.WriteString(r.Name())
	b.WriteByte(':')
	b.WriteString(r.MarshalTime(t))
	b.WriteByte(':')
	b.WriteString(event)
	return
}

func (db *DB) AppendFieldMap(dim Dimension, values map[string]string, b *bytes.Buffer) {
	if len(dim) == 0 || values == nil {
		return
	}
	for i, label := range dim {
		if i > 0 {
			b.WriteByte(LabelSeparator)
		}
		b.WriteString(label)
		b.WriteByte(LabelSeparator)
		b.WriteString(values[label])
	}
	b.WriteByte(FieldTerminator)
}

func (db *DB) AppendField(dim Dimension, labels []string, values []string, b *bytes.Buffer) (ok bool) {
	if len(dim) == 0 {
		return
	}
	n := len(labels)
	if n > len(values) {
		n = len(values)
	}
	if n == 0 {
		return
	}
	j := 0
	for i := 0; i < n; i++ {
		label := labels[i]
		if !dim.Contains(label) {
			continue
		}
		v := values[i]
		if len(v) == 0 {
			continue
		}
		if j > 0 {
			b.WriteByte(LabelSeparator)
		}
		b.WriteString(label)
		b.WriteByte(LabelSeparator)
		b.WriteString(v)
		j++
	}
	if ok = j == len(dim); ok {
		b.WriteByte(FieldTerminator)
	}
	return
}

// TODO: Aggregate errors per key?
type jobs map[string]*job

func newJobs() jobs {
	return jobs(make(map[string]*job))
}

type job struct {
	fields map[string]int64
	name   string
	ttl    time.Duration
}

func newJob(name string) *job {
	return &job{make(map[string]int64), name, 0}
}

func (db DB) aggregate(c Collector) jobs {
	ch := make(chan Metric)
	result := make(chan jobs)
	go func() {
		b := newJobs()
		buf := bytes.NewBuffer(nil)
		tm := time.Now()
		for m := range ch {
			if m == nil {
				continue
			}
			n := m.Set(0)
			if n == 0 {
				continue
			}
			values := m.Values()
			desc := m.Describe()
			name := desc.Name()
			labels := desc.Labels()
			for _, layout := range desc.Layouts() {
				res, dims := layout.Resolution, layout.Dimensions
				buf.Reset()
				db.AppendKey(res, name, tm, buf)
				key := buf.String()
				j := b[key]
				if j == nil {
					j = newJob(name)
					b[key] = j
				}
				if values == nil {
					j.fields["*"] += n
					j.ttl = res.TTL()
					continue
				}
				if len(values) == 0 || len(dims) == 0 {
					continue
				}
				i := 0
				for _, dim := range dims {
					buf.Reset()
					if ok := db.AppendField(dim, labels, values, buf); ok {
						field := buf.Bytes()
						j.fields[string(field)] += n
						i++
					}
				}
				if i > 0 {
					j.ttl = res.TTL()
				}
			}
		}
		result <- b
	}()
	return <-result
}

func (db *DB) Gather(col Collector) error {
	jobs := db.aggregate(col)
	if len(jobs) == 0 {
		return nil
	}
	pipeline := db.Redis.Pipeline()
	defer pipeline.Close()
	for key, job := range jobs {
		if job == nil {
			continue
		}
		if len(job.fields) != 0 {
			for f, n := range job.fields {
				pipeline.HIncrBy(key, f, n)

			}
		}
		if job.ttl > 0 {
			pipeline.Expire(key, job.ttl)
		}
	}
	_, err := pipeline.Exec()
	return err
}

type RecordSequence []Record

func (records RecordSequence) AppendResults(results ResultSequence) ResultSequence {
	if results == nil {
		results = ResultSequence{}
	}
	for _, r := range records {
		p := DataPoint{r.tm.Unix(), r.Value()}
		if i := results.Find(r.name, r.field); i != -1 {
			results[i].Data = append(results[i].Data, p)
			continue
		}
		results = append(results, Result{
			Event:  r.name,
			Labels: r.values,
			Data:   DataPointSequence{p},
		})
	}
	return results
}
