package meter2

import (
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const DefaultKeyPrefix = "meter:"

type DB struct {
	Redis redis.UniversalClient
	// *Registry
	KeyPrefix string
}

func NewDB(r redis.UniversalClient) *DB {
	db := new(DB)
	db.Redis = r
	// db.Registry = defaultRegistry
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
const sNilByte = "\x00"

func AppendField(data []byte, labels, values []string) []byte {
	n := len(values)
	for i := 0; i < len(labels); i++ {
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

func (db *DB) Gather(col Collector, tm time.Time) error {
	pipelineSize := 0
	pipeline := db.Redis.Pipeline()
	defer pipeline.Close()
	ch := make(chan Metric)
	go func() {
		data := []byte{}
		keysTTL := make(map[string]time.Duration)
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
			data = AppendField(data[:0], labels, values)
			field := string(data)
			t := desc.Type()
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

				pipelineSize++
			}
			for key, ttl := range keysTTL {
				pipeline.Expire(key, ttl)
				pipelineSize++
			}
		}
	}()
	col.Collect(ch)
	close(ch)
	if pipelineSize != 0 {
		if _, err := pipeline.Exec(); err != nil && err != redis.Nil {
			return err
		}
	}
	return nil
}

type ScanResult struct {
	Name   string
	Time   time.Time
	Group  []string
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

func AppendMatchField(data []byte, labels []string, group []string, q map[string]string) []byte {
	for i := 0; i < len(labels); i++ {
		if i != 0 {
			data = append(data, LabelSeparator)
		}
		label := labels[i]
		data = AppendMatch(data, label)
		data = append(data, LabelSeparator)
		if indexOf(group, label) < 0 {
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

func (db *DB) Query(queries ...Query) (Results, error) {
	results := Results{}
	if len(queries) == 0 {
		return results, nil
	}
	ch := make(chan ScanResult, len(queries))
	go func() {
		for r := range ch {
			results = results.Append(r)
		}
	}()
	wg := new(sync.WaitGroup)
	for _, q := range queries {
		wg.Add(1)
		go func(q Query) {
			switch q.Mode {
			case QueryModeExact:
				db.ExactQuery(ch, q)
			case QueryModeScan:
				db.ScanQuery(ch, q)
			}
			wg.Done()
		}(q)
	}
	wg.Wait()
	close(ch)
	return results, nil
}

func (db *DB) ExactQuery(results chan<- ScanResult, q Query) error {
	var replies []*redis.StringCmd
	if err := q.Error(); err != nil {
		r := ScanResult{err: err}
		results <- r
	}
	// Field/Key buffer
	data := []byte{}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	desc := q.Event.Describe()
	labels := desc.Labels()
	qValues := QueryPermutations(q.Values)
	pipeline := db.Redis.Pipeline()
	defer pipeline.Close()
	for _, values := range qValues {
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
	if _, err := pipeline.Exec(); err != nil && err != redis.Nil {
		return err
	}

	for i, values := range qValues {
		for j, tm := range ts {
			reply := replies[i*len(ts)+j]
			n, err := reply.Int64()
			results <- ScanResult{
				Name:   desc.Name(),
				Time:   tm,
				Values: values,
				count:  n,
				err:    err,
			}
		}
	}
	return nil
}

func (db *DB) ScanQuery(results chan<- ScanResult, q Query) (err error) {
	if e := q.Error(); e != nil {
		results <- ScanResult{err: e}
		return
	}
	desc := q.Event.Describe()

	result := ScanResult{
		Name:  desc.Name(),
		Group: q.Group,
	}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	if len(ts) == 0 {
		return
	}
	qValues := QueryPermutations(q.Values)
	if len(qValues) == 0 {
		qValues = append(qValues, map[string]string{})
	}
	wg := &sync.WaitGroup{}
	for _, values := range qValues {
		result.Values = values
		// data = AppendField(data, desc.Labels(), m.Values())
		data := AppendMatchField(nil, desc.Labels(), q.Group, values)
		match := string(data)
		// Let redis client pool size determine parallel request blocking
		for _, tm := range ts {
			result.Time = tm
			data = db.AppendKey(data[:0], res, desc.Name(), tm)
			key := string(data)
			wg.Add(1)
			go func(r ScanResult, key string) {
				db.Scan(key, match, q.Group, r, results)
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

func fieldIndexOf(field []string, v string) int {
	n := len(field)
	n -= n % 2
	for i := 0; i < n; i += 2 {
		if field[i] == v {
			return i
		}
	}
	return -1
}

func parseGroup(pairs []string, group string) LabelValues {
	pairs = parseField(pairs, group)
	n := len(pairs)
	n -= n % 2
	values := make(map[string]string, n/2)
	for i := 0; i < n; i += 2 {
		if v := pairs[i+1]; v != sNilByte {
			values[pairs[i]] = pairs[i+1]
		}
	}
	return LabelValues(values)
}

func scanField(val []byte, field []string, group []string) ([]byte, bool) {
	for i := 0; i < len(group); i++ {
		if j := fieldIndexOf(field, group[i]); j < 0 || field[j+1] == sNilByte {
			return val, false
		} else {
			if i != 0 {
				val = append(val, LabelSeparator)
			}
			val = append(val, group[i]...)
			val = append(val, LabelSeparator)
			val = append(val, field[j+1]...)
		}
	}
	val = append(val, FieldTerminator)
	return val, true
}

// const sLabelSeparator = "\x1f"

func (db *DB) Scan(key, match string, group []string, r ScanResult, results chan<- ScanResult) (err error) {
	var fields []string
	scan := db.Redis.HScan(key, 0, match, -1).Iterator()
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
		if err == redis.Nil {
			err = nil
		}
		return
	}
	var grouped map[string]int64
	var val []byte
	var field []string
	if len(group) != 0 {
		val = []byte{}
		grouped = make(map[string]int64, len(fields))
		field = make([]string, len(group)*2)
	}
	// Just in case
	r.count = 0
	for i, x := range reply {
		if a, ok := x.(string); ok {
			if n, e := strconv.ParseInt(a, 10, 64); e == nil {
				if grouped != nil {
					field = parseField(field[:0], fields[i])
					if val, ok = scanField(val[:0], field, group); ok {
						grouped[string(val)] += n
					}
				} else {
					r.count += n
				}
			}
		}
	}
	if grouped == nil {
		results <- r
		return
	}
	for g, count := range grouped {
		r.Values = parseGroup(field[:0], g)
		r.count = count
		results <- r
	}
	return

}
