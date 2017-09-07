package meter2

import (
	"log"
	"net/url"
	"strconv"
	"sync"
	"time"

	tc "github.com/alxarch/go-timecodec"
	"github.com/go-redis/redis"
)

const DefaultKeyPrefix = "meter:"

type DB struct {
	Redis     redis.UniversalClient
	Registry  *Registry
	KeyPrefix string
}

func NewDB(r redis.UniversalClient) *DB {
	db := new(DB)
	db.Redis = r
	db.Registry = defaultRegistry
	db.KeyPrefix = DefaultKeyPrefix
	return db
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
	return string(db.AppendKey(nil, r, event, t))
}

func (db DB) AppendKey(data []byte, r Resolution, event string, t time.Time) []byte {
	if db.KeyPrefix != "" {
		data = append(data, db.KeyPrefix...)
		data = append(data, LabelSeparator)
	}
	data = append(data, r.Name()...)
	data = append(data, LabelSeparator)
	data = append(data, r.MarshalTime(t)...)
	data = append(data, LabelSeparator)
	data = append(data, event...)
	return data
}

const NilByte byte = 0

func AppendField(data []byte, labels, values []string) []byte {
	n := len(values)
	for i := 0; i < len(labels); i++ {
		label := labels[i]
		if i > 0 {
			data = append(data, LabelSeparator)
		}
		data = append(data, label...)
		data = append(data, LabelSeparator)
		if i < n {
			value := values[i]
			data = append(data, value...)
		} else {
			data = append(data, NilByte)
		}
	}
	data = append(data, FieldTerminator)
	return data
}

func (db *DB) Gather(col Collector) error {
	ch := make(chan Metric)
	result := make(chan error)
	go func() {
		pipeline := db.Redis.Pipeline()
		defer pipeline.Close()
		data := []byte{}
		tm := time.Now()
		pipelineSize := 0
		for m := range ch {
			if m == nil {
				continue
			}
			n := m.Set(0)
			if n == 0 {
				// log.Println("Empty counter")
				continue
			}
			values := m.Values()
			desc := m.Describe()
			name := desc.Name()
			labels := desc.Labels()
			for _, layout := range desc.Layouts() {

				res := layout.Resolution
				data = db.AppendKey(data[:0], res, name, tm)
				key := string(data)
				data = AppendField(data[:0], labels, values)
				// log.Println(res.Name(), key, string(data))
				pipeline.HIncrBy(key, string(data), n)
				pipelineSize++
			}
		}
		if pipelineSize == 0 {
			result <- nil
			return
		}
		if _, err := pipeline.Exec(); err != nil && err != redis.Nil {
			log.Println(err)
			result <- err
			return
		}
		result <- nil
	}()
	col.Collect(ch)
	close(ch)
	return <-result
}

type ScanResult struct {
	Name   string
	Time   time.Time
	Values LabelValues
	err    error
	count  int64
}

func AppendMatchField(data []byte, labels []string, group string, q map[string]string) []byte {
	for i := 0; i < len(labels); i++ {
		if i > 0 {
			data = append(data, LabelSeparator)
		}
		label := labels[i]
		data = append(data, quoteMeta(label)...)
		data = append(data, LabelSeparator)
		if label == group {
			data = append(data, '[', '^', NilByte, ']', '*')
			continue
		}
		if q != nil {
			if v, ok := q[label]; ok {
				data = append(data, quoteMeta(v)...)
				continue
			}
		}
		data = append(data, '*')

	}
	data = append(data, FieldTerminator)
	return data
}

type ScanQuery struct {
	Event      string
	Start, End time.Time
	Group      string
	Query      url.Values
	Resolution Resolution
}

func (q ScanQuery) QueryValues(d *Desc) []map[string]string {
	if d == nil {
		return nil
	}
	queries := d.MatchingQueries(q.Query)
	return QueryPermutations(queries)
}

func (db *DB) ScanQuery(q ScanQuery, results chan<- ScanResult) (err error) {
	event := db.Registry.Get(q.Event)
	if event == nil {
		return ErrUnregisteredEvent
	}
	queries := q.QueryValues(event.Describe())
	if queries == nil {
		return nil
	}
	ts := q.Resolution.TimeSequence(q.Start, q.End)
	if len(ts) == 0 {
		return nil
	}
	wg := &sync.WaitGroup{}
	result := ScanResult{
		Name: q.Event,
	}
	for _, values := range queries {
		result.Values = values
		m := event.WithLabels(values)
		if m == nil {
			result.err = ErrInvalidEventLabel
			results <- result
			continue
		}
		desc := m.Describe()
		if desc == nil {
			result.err = ErrNilDesc
			results <- result
			continue
		}
		result.Name = desc.Name()
		data := AppendMatchField(nil, desc.Labels(), q.Group, values)
		field := string(data)
		// Let redis client pool size determine parallel requests
		for _, tm := range ts {
			result.Time = tm
			data = db.AppendKey(data[:0], q.Resolution, desc.Name(), tm)
			key := string(data)
			wg.Add(1)
			go func(key, field string, tm time.Time, r ScanResult) {
				r.count, r.err = db.Scan(key, field)
				results <- r
				wg.Done()
			}(key, field, tm, result)
		}
	}
	wg.Wait()

	return
}
func (db *DB) Scan(key string, field string) (n int64, err error) {
	var fields []string
	scan := db.Redis.HScan(key, 0, field, -1).Iterator()
	for scan.Next() {
		fields = append(fields, scan.Val())
	}
	if err = scan.Err(); err != nil {
		return
	}
	if len(fields) == 0 {
		return
	}
	var reply []interface{}
	reply, err = db.Redis.HMGet(key, fields...).Result()
	if err != nil {
		return
	}
	for _, x := range reply {
		if a, ok := x.(string); ok {
			if count, e := strconv.ParseInt(a, 10, 64); e == nil {
				n += count
			} else {
				err = e
				return
			}
		}
	}

	return

}

func (r ScanResult) AppendResults(results Results) Results {
	p := DataPoint{r.Time.Unix(), r.count}
	if results != nil {
		if i := results.Find(r.Name, r.Values); i != -1 {
			results[i].Data = append(results[i].Data, p)
			return results
		}
	}
	return append(results, Result{
		Event:  r.Name,
		Labels: r.Values,
		Data:   DataPointSequence{p},
	})

}
