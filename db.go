package meter

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type DB struct {
	Redis        *redis.Pool
	KeyPrefix    string
	KeySeparator byte
	ScanSize     int64
}

// DB defaults
const (
	DefaultKeyPrefix = "meter" // Prefix all keys
	DefaultSeparator = '^'     // Key separator
	DefaultScanSize  = 100     // Scan size for queries
)

func NewDB(r *redis.Pool) *DB {
	db := new(DB)
	db.Redis = r
	db.KeyPrefix = DefaultKeyPrefix
	db.KeySeparator = DefaultSeparator
	db.ScanSize = DefaultScanSize

	return db
}

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
		data = append(data, db.KeySeparator)
	}
	data = append(data, r.Name()...)
	data = append(data, db.KeySeparator)
	data = append(data, r.MarshalTime(t)...)
	data = append(data, db.KeySeparator)
	data = append(data, event...)
	return data
}

const maxValueSize = 255

func packField(data []byte, values, labels []string) []byte {
	for _, v := range values {
		if len(v) > maxValueSize {
			v = v[:maxValueSize]
		}
		data = append(data, byte(len(v)))
		data = append(data, v...)
	}
	for i := len(values); i < len(labels); i++ {
		data = append(data, 0)
	}
	return data
}

func (db *DB) Batch(tm time.Time, events ...*Event) (err []error) {
	wg := sync.WaitGroup{}
	errCh := make(chan error, len(events))
	for _, e := range events {
		wg.Add(1)
		go func(e *Event) {
			errCh <- db.Gather(tm, e)
			wg.Done()
		}(e)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		err = append(err, e)
	}
	return
}

func (db *DB) Gather(tm time.Time, e *Event) error {
	snapshot := e.Flush(getSnapshot())
	defer putSnapshot(snapshot)
	err := db.gather(tm, e.Describe(), snapshot)
	if err != nil {
		e.Merge(snapshot)
		return err
	}
	return nil
}

func (db *DB) gather(tm time.Time, desc *Desc, snapshot Snapshot) (err error) {
	var (
		name        = desc.Name()
		t           = desc.Type()
		labels      = desc.Labels()
		data        = make([]byte, 64*len(labels))
		pipeline    = db.Redis.Get()
		resolutions = desc.Resolutions()
		keys        = make(map[string]Resolution, len(resolutions))
		size        = 0
	)
	defer pipeline.Close()
	for _, res := range resolutions {
		key := db.Key(res, name, tm)
		keys[key] = res
	}
	for _, m := range snapshot {
		n := m.Count
		if n == 0 {
			continue
		}
		data = packField(data[:0], m.Values, labels)
		field := string(data)
		for key := range keys {
			pipeline.Send(t.RedisCmd(), key, field, n)
			size++
		}
	}
	if size == 0 {
		return
	}
	for key, res := range keys {
		pipeline.Send("PEXPIRE", key, int64(res.TTL()/time.Millisecond))
	}
	_, err = pipeline.Do("")
	return
}

func (db *DB) Query(queries ...Query) (Results, error) {
	if len(queries) == 0 {
		return Results{}, nil
	}
	mode := queries[0].Mode
	results := new(scanResults)
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
				db.scanQuery(results, q)
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

func (db *DB) exactQuery(results *scanResults, q Query) error {
	var replies []interface{}
	if err := q.Error(); err != nil {
		results.Add(scanResult{err: err})
		return err
	}
	if len(q.Values) == 0 {
		// Exact query without values is pointless
		return fmt.Errorf("Invalid query")
	}
	data := []byte{}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	desc := q.Event.Describe()
	labels := desc.Labels()
	pipeline := db.Redis.Get()
	defer pipeline.Close()
	values := make([]string, len(labels))
	for _, v := range q.Values {
		for i, label := range labels {
			values[i] = v[label]
		}
		data = packField(data[:0], values, labels)
		field := string(data)
		for _, tm := range ts {
			key := db.Key(res, desc.Name(), tm)
			replies = append(replies, pipeline.Send("HGET", key, field))
		}
	}
	if len(replies) == 0 {
		return nil
	}
	err := pipeline.Flush()
	if err != nil {
		return err
	}

	for _, values := range q.Values {
		for _, tm := range ts {
			n, err := redis.Int64(pipeline.Receive())
			results.Add(scanResult{
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

func matchField(field string, labels []string, values map[string]string) bool {
	if len(values) == 0 {
		// Match all
		return true
	}
	n := 0
	size := 0
	for _, label := range labels {
		if len(field) > 0 {
			size = int(field[0])
			field = field[1:]
			if 0 <= size && size <= len(field) {
				if v, ok := values[label]; ok {
					if field[:size] != v {
						return false
					}
					n++
				}
				field = field[size:]
				continue
			}
		}
		return false
	}
	return n == len(values)
}

type scanIterator struct {
	cursor int64
	page   map[string]int64
}

func (it *scanIterator) Scan(conn redis.Conn, key string, size int64) (bool, error) {
	reply, err := conn.Do("HSCAN", key, it.cursor, "COUNT", size)
	if err != nil {
		return false, err
	}
	err = it.RedisScan(reply)
	if err != nil {
		return false, err
	}
	return it.cursor != 0, nil

}

func (it *scanIterator) RedisScan(src interface{}) error {
	scanReply, err := redis.Values(src, nil)
	if err != nil {
		return err
	}
	if len(scanReply) != 2 {
		return errors.New("Invalid scan reply")
	}
	it.cursor, err = redis.Int64(scanReply[0], nil)
	if err != nil {
		return err
	}
	page, err := redis.Int64Map(scanReply[1], nil)
	if err != nil {
		return err
	}
	it.page = page
	return nil
}

func (db *DB) scanQuery(results *scanResults, q Query) (err error) {
	desc := q.Event.Describe()
	if e := desc.Error(); e != nil {
		return e
	}
	labels := desc.Labels()
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	if len(ts) == 0 {
		return
	}
	wg := new(sync.WaitGroup)
	for _, tm := range ts {
		wg.Add(1)
		go func(tm time.Time) {
			defer wg.Done()
			key := db.Key(res, desc.Name(), tm)
			it := scanIterator{}
			hasMore := true
			numResults := 0
			for hasMore {
				conn := db.Redis.Get()
				hasMore, err = it.Scan(conn, key, db.ScanSize)
				conn.Close()
				if err != nil {
					return
				}
				for field, count := range it.page {
					for _, v := range q.Values {
						if len(v) == 0 {
							v = fieldValues(field, labels)
						} else if matchField(field, labels, v) {
							v = copyValues(v)
						} else {
							continue
						}
						results.Add(scanResult{
							Event:  desc.Name(),
							Group:  q.Group,
							Time:   tm,
							Values: v,
							count:  count,
						})
						numResults++
					}
				}
			}
			if numResults == 0 {
				// Report an empty result
				results.Add(scanResult{
					Event: desc.Name(),
					Group: q.Group,
					Time:  tm,
				})
			}
			return
		}(tm)
	}
	wg.Wait()
	return nil

}

func copyValues(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	m := make(map[string]string, len(values))
	for k, v := range values {
		m[k] = v
	}
	return m
}

func fieldValues(field string, labels []string) map[string]string {
	m := make(map[string]string, len(labels))
	for _, label := range labels {
		if len(field) > 0 {
			size := int(field[0])
			field = field[1:]
			if 0 < size && size <= len(field) {
				m[label] = field[:size]
				field = field[size:]
			}
			continue
		}
		break
	}
	return m
}

// valueQuery return a frequency map of event label values
func (db *DB) valueQuery(results *scanResults, q Query) error {
	wg := new(sync.WaitGroup)
	ts := q.Resolution.TimeSequence(q.Start, q.End)
	for _, t := range ts {
		// db.acquireQueue()
		wg.Add(1)
		go func(t time.Time) {
			defer wg.Done()
			// defer db.releaseQueue()
			var n int64
			desc := q.Event.Describe()
			key := db.Key(q.Resolution, desc.Name(), t)
			conn := db.Redis.Get()
			defer conn.Close()
			reply, err := redis.Int64Map(conn.Do("HGETALL", key))
			if err != nil {
				return
			}
			labels := desc.Labels()
			for field, count := range reply {
				if count == 0 {
					continue
				}
				match := false
				for _, values := range q.Values {
					match = matchField(field, labels, values)
					if match {
						break
					}
				}
				if match || len(q.Values) == 0 {
					for k, v := range fieldValues(field, labels) {
						results.Add(scanResult{
							Event:  desc.Name(),
							Group:  q.Group,
							Time:   t,
							Values: map[string]string{k: v},
							count:  n,
						})
					}
				}
			}
		}(t)
	}
	wg.Wait()
	return nil
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

type scanResults struct {
	mu      sync.Mutex
	results Results
}

type scanResult struct {
	Event  string
	Group  []string
	Time   time.Time
	Values map[string]string
	err    error
	count  int64
}

func (r scanResult) AppendTo(results Results) Results {
	values := r.Values
	if r.Group != nil {
		values = make(map[string]string, len(r.Group))
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

func (rs *scanResults) Add(r scanResult) {
	rs.mu.Lock()
	rs.results = r.AppendTo(rs.results)
	rs.mu.Unlock()
}
func (rs *scanResults) Snapshot(r Results) Results {
	rs.mu.Lock()
	r = append(r, rs.results...)
	rs.mu.Unlock()
	return r
}
