package meter

import (
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const (
	DefaultKeyPrefix = "meter"
	DefaultScanSize  = 100
)

type DB struct {
	Redis       *redis.Client
	KeyPrefix   string
	ScanSize    int64
	concurrency chan struct{}
}

func NewDB(r *redis.Client) *DB {
	db := new(DB)
	db.Redis = r
	db.KeyPrefix = DefaultKeyPrefix
	db.ScanSize = DefaultScanSize
	db.concurrency = make(chan struct{}, r.Options().PoolSize)

	return db
}

const LabelSeparator = '\x1f'
const FieldTerminator = '\x1e'

func (db DB) Key(r Resolution, event string, t time.Time) (k string) {
	b := getBuffer()
	b = db.AppendKey(b[:0], r, event, t)
	k = string(b)
	putBuffer(b)
	return
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
const sNilByte = "\x00"

func Field(labels, values []string) (f string) {
	b := getBuffer()
	b = AppendField(b[:0], labels, values)
	f = string(b)
	putBuffer(b)
	return
}

func AppendField(data []byte, labels, values []string) []byte {
	for i := 0; 0 <= i && i < len(labels); i++ {
		label := labels[i]
		if i != 0 {
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

// func (db *DB) Sync() error {
// 	return db.SyncAt(time.Now())
// }

// func (db *DB) SyncAt(tm time.Time) error {
// 	return db.Registry.Sync(db, tm)
// }

func (db *DB) Gather(tm time.Time, events ...*Event) (err error) {
	var (
		values   []string
		data     []byte
		pipeline = db.Redis.Pipeline()
		keysTTL  = make(map[string]time.Duration)
	)
	defer pipeline.Close()
	for _, e := range events {
		desc := e.Describe()
		name := desc.Name()
		labels := desc.Labels()
		t := desc.Type()
		s := e.Flush(nil)
		for _, m := range s {
			n := m.Count()
			if n == 0 {
				continue
			}
			values = m.AppendValues(values[:0])
			data = AppendField(data[:0], labels, values)
			field := string(data)
			for _, res := range desc.Resolutions() {
				data = db.AppendKey(data[:0], res, name, tm)
				key := string(data)
				keysTTL[key] = res.TTL()
				switch t {
				case MetricTypeIncrement:
					pipeline.HIncrBy(key, field, n)
				case MetricTypeUpdateOnce:
					pipeline.HSetNX(key, field, n)
				case MetricTypeUpdate:
					pipeline.HSet(key, field, n)
				default:
					continue
				}
			}
		}
		for key, ttl := range keysTTL {
			pipeline.Expire(key, ttl)
		}
	}
	_, err = pipeline.Exec()
	return
}

type ScanResult struct {
	Event  string
	Group  []string
	Time   time.Time
	Values LabelValues
	err    error
	count  int64
}

func AppendMatch(data []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch b := s[i]; b {
		case '*', '[', ']', '?', '^':
			data = append(data, '\\', b)
		default:
			data = append(data, b)
		}
	}
	return data
}

func MatchField(labels []string, group []string, q map[string]string) (f string) {
	b := getBuffer()
	b = AppendMatchField(b[:0], labels, group, q)
	f = string(b)
	putBuffer(b)
	return
}
func AppendMatchField(data []byte, labels []string, group []string, q map[string]string) []byte {
	if len(group) == 0 && len(q) == 0 {
		return append(data, '*')
	}
	for i := 0; i < len(labels); i++ {
		if i != 0 {
			data = append(data, LabelSeparator)
		}
		label := labels[i]
		data = AppendMatch(data, label)
		data = append(data, LabelSeparator)
		if indexOf(group, label) >= 0 {
			data = append(data, '[', '^', NilByte, ']', '*')
			continue
		}
		if q != nil {
			if v, ok := q[label]; ok {
				data = AppendMatch(data, v)
				continue
			}
		}
		data = append(data, '*')

	}
	data = append(data, FieldTerminator)
	return data
}

type resultCollector struct {
	mu      sync.Mutex
	results Results
}

func (c *resultCollector) Add(r ScanResult) {
	c.mu.Lock()
	c.results = r.AppendTo(c.results)
	c.mu.Unlock()
}
func (c *resultCollector) Snapshot(r Results) Results {
	c.mu.Lock()
	r = append(r, c.results...)
	c.mu.Unlock()
	return r
}

func (db *DB) Query(queries ...Query) (Results, error) {
	if len(queries) == 0 {
		return Results{}, nil
	}
	mode := queries[0].Mode
	results := new(resultCollector)
	wg := new(sync.WaitGroup)
	for _, q := range queries {
		if err := q.Error(); err != nil {
			continue
		}
		wg.Add(1)
		go func(q Query) {
			switch mode {
			case ModeExact:
				db.exactQuery(results, q)
			case ModeScan:
				db.scanQuery2(results, q)
			case ModeValues:
				db.valueQuery(results, q)
			}
			wg.Done()
		}(q)
	}
	wg.Wait()
	rr := results.results
	if rr != nil {
		for i := range rr {
			rr[i].Data.Sort()
		}
		return rr, nil
	}
	return Results{}, nil
}

func (db *DB) exactQuery(results *resultCollector, q Query) error {
	var replies []*redis.StringCmd
	if err := q.Error(); err != nil {
		results.Add(ScanResult{err: err})
	}
	// Field/Key buffer
	if len(q.Values) == 0 {
		// Exact query without values is pointless
		return nil
	}
	data := []byte{}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	desc := q.Event.Describe()
	labels := desc.Labels()
	pipeline := db.Redis.Pipeline()
	defer pipeline.Close()
	for _, values := range q.Values {
		data = AppendField(data[:0], labels, LabelValues(values).Values(labels))
		field := string(data)
		for _, tm := range ts {
			data = db.AppendKey(data[:0], res, desc.Name(), tm)
			key := string(data)
			replies = append(replies, pipeline.HGet(key, field))
		}
	}
	if len(replies) == 0 {

		return nil
	}
	db.concurrency <- struct{}{}
	_, err := pipeline.Exec()
	<-db.concurrency
	if err != nil && err != redis.Nil {
		return err
	}

	for i, values := range q.Values {
		for j, tm := range ts {
			reply := replies[i*len(ts)+j]
			n, err := reply.Int64()
			if err == redis.Nil {
				err = nil
			}
			results.Add(ScanResult{
				Event:  desc.Name(),
				Time:   tm,
				Values: values,
				count:  n,
				err:    err,
			})
		}
	}
	return nil
}

func matchFieldToValues(field []string, values map[string]string) bool {
	if values == nil {
		return true
	}
	n := 0
	for i := 0; i < len(field); i += 2 {
		key := field[i]
		if v, ok := values[key]; ok {
			if v == field[i+1] {
				n++
			} else {
				return false
			}
		}
	}
	return n == len(values)
}

func (db *DB) scan2(key string, r ScanResult, results *resultCollector, values ...map[string]string) (err error) {
	scan := db.Redis.HScan(key, 0, "", db.ScanSize).Iterator()
	i := 0
	j := 0
	var pairs []string
	// group := len(r.Group) != 0
	for scan.Next() {
		if i%2 == 0 {
			pairs = parseField(pairs[:0], scan.Val())
		} else {
			r.count, r.err = strconv.ParseInt(scan.Val(), 10, 64)
			if r.err != nil {
				results.Add(r)
				j++
				continue
			}
			for _, v := range values {
				if len(v) == 0 {
					r.Values = FieldLabels(pairs)
				} else if matchFieldToValues(pairs, v) {
					r.Values = LabelValues(v).Copy()
				} else {
					continue
				}
				results.Add(r)
				j++
			}
		}
		i++
	}
	if err = scan.Err(); err != nil {
		return
	}
	if j == 0 {
		// Report an empty result
		results.Add(r)
	}
	return

}

func (db *DB) scanQuery2(results *resultCollector, q Query) (err error) {
	desc := q.Event.Describe()
	if e := desc.Error(); e != nil {
		return e
	}
	result := ScanResult{
		Event: desc.Name(),
		Group: q.Group,
	}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	if len(ts) == 0 {
		return
	}
	wg := new(sync.WaitGroup)
	data := make([]byte, 256)
	for _, tm := range ts {
		result.Time = tm
		data = db.AppendKey(data[:0], res, desc.Name(), tm)
		key := string(data)
		// Let redis client pool size determine parallel request blocking
		wg.Add(1)
		go func(r ScanResult, key string) {
			db.scan2(key, r, results, q.Values...)
			wg.Done()
		}(result, key)
	}
	wg.Wait()
	return nil

}
func (db *DB) scanQuery(results chan<- ScanResult, q Query) (err error) {
	desc := q.Event.Describe()
	if e := desc.Error(); e != nil {
		return e
	}
	result := ScanResult{
		Event: desc.Name(),
		Group: q.Group,
	}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	if len(ts) == 0 {
		return
	}
	wg := &sync.WaitGroup{}
	for _, values := range q.Values {
		result.Values = values
		data := AppendMatchField(nil, desc.Labels(), q.Group, values)
		match := string(data)
		// Let redis client pool size determine parallel request blocking
		for _, tm := range ts {
			result.Time = tm
			data = db.AppendKey(data[:0], res, desc.Name(), tm)
			key := string(data)
			db.concurrency <- struct{}{}
			wg.Add(1)
			go func(r ScanResult, key string) {
				db.scan(key, match, r, results)
				<-db.concurrency
				wg.Done()
			}(result, key)
		}
	}
	wg.Wait()

	return nil
}

func parseField(values []string, field string) []string {
	offset := 0
	for i := 0; i < len(field); i++ {
		switch field[i] {
		case LabelSeparator:
			values = append(values, field[offset:i])
			offset = i + 1
		case FieldTerminator:
			values = append(values, field[offset:i])
			offset = i + 1
			break
		}
	}
	if offset < len(field) {
		values = append(values, field[offset:])
	}
	return values
}

func (db *DB) scan(key, match string, r ScanResult, results chan<- ScanResult) (err error) {
	scan := db.Redis.HScan(key, 0, match, db.ScanSize).Iterator()
	i := 0
	var pairs []string
	group := len(r.Group) != 0
	for scan.Next() {
		if i%2 == 0 {
			if group {
				pairs = parseField(pairs[:0], scan.Val())
				r.Values = FieldLabels(pairs)
			}
		} else {
			r.count, r.err = strconv.ParseInt(scan.Val(), 10, 64)
			results <- r
		}
		i++
	}
	if err = scan.Err(); err != nil {
		return
	}
	if i == 0 {
		// Report an empty result
		results <- r
	}
	return

}

type pair struct {
	Label, Value string
	Count        int64
}

// valueQuery return a frequency map of event label values
func (db *DB) valueQuery(results *resultCollector, q Query) error {
	desc := q.Event.Describe()
	labels := desc.Labels()
	r := ScanResult{
		Event: desc.Name(),
	}
	wg := new(sync.WaitGroup)
	data := []byte{}
	ts := q.Resolution.TimeSequence(q.Start, q.End)

	for _, t := range ts {
		db.concurrency <- struct{}{}
		wg.Add(1)
		r.Time = t
		data = db.AppendKey(data[:0], q.Resolution, desc.Name(), t)
		key := string(data)
		go func(key string) {
			var n int64
			field := make([]string, len(labels)*2)
			reply, err := db.Redis.HGetAll(key).Result()
			if err == redis.Nil {
				return
			}
			if err != nil {
				r.err = err
				results.Add(r)
				return
			}
			for key, value := range reply {
				if n, _ = strconv.ParseInt(value, 10, 64); n == 0 {
					continue
				}
				field = parseField(field[:0], key)
				if len(q.Values) == 0 {
					for i := 0; i < len(field); i += 2 {
						r.Values = LabelValues{field[i]: field[i+1]}
						r.count = n
						results.Add(r)
					}
					continue
				}
				for j := 0; j < len(q.Values); j++ {
					values := q.Values[j]
					if !matchFieldToValues(field, values) {
						continue
					}
					for i := 0; i < len(field); i += 2 {
						r.Values = LabelValues{field[i]: field[i+1]}
						r.count = n
						results.Add(r)
					}
					break
				}
			}

			<-db.concurrency
			wg.Done()
		}(key)
	}
	wg.Wait()
	return nil
}

func CollectResults(scan <-chan ScanResult) <-chan Results {
	out := make(chan Results)
	go func() {
		var results Results
		for r := range scan {
			if r.err != nil {
				continue
			}

			results = r.AppendTo(results)
		}
		out <- results
	}()
	return out

}

func (r ScanResult) AppendTo(results Results) Results {
	values := r.Values
	if r.Group != nil {
		values = LabelValues{}
		for _, g := range r.Group {
			if v, ok := r.Values[g]; ok {
				values[g] = v
			}
		}
	}
	if len(values) == 0 {
		return results
	}
	p := DataPoint{Timestamp: r.Time.Unix(), Value: r.count}
	if i := results.IndexOf(r.Event, values); i < 0 {
		return append(results, Result{
			Event:  r.Event,
			Labels: values,
			Data:   DataPoints{p},
		})
	} else if j := results[i].Data.IndexOf(r.Time); j < 0 {
		results[i].Data = append(results[i].Data, p)
	} else {
		results[i].Data[j].Value += r.count
	}
	return results
}

var bufferPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 256)
	},
}

func getBuffer() []byte {
	return bufferPool.Get().([]byte)
}
func putBuffer(b []byte) {
	bufferPool.Put(b)
}
