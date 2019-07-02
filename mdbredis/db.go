package mdbredis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/go-redis/redis"
)

type DB struct {
	redis       redis.UniversalClient
	keyPrefix   string
	scanSize    int64
	mu          sync.RWMutex
	events      map[string]meter.Storer
	resolutions map[time.Duration]Resolution
}

var _ meter.DB = (*DB)(nil)

func (db *DB) Storer(event string) meter.Storer {
	db.mu.RLock()
	e, ok := db.events[event]
	db.mu.RUnlock()
	if ok {
		return e
	}
	return nil
}

type storer struct {
	*DB
	event string
	Resolution
}

func (db *DB) AddEvent(event string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.events == nil {
		db.events = make(map[string]meter.Storer)
	}
	storers := make([]meter.Storer, 0, len(db.resolutions))
	for _, res := range db.resolutions {
		storers = append(storers, &storer{
			DB:         db,
			event:      event,
			Resolution: res,
		})
	}
	db.events[event] = meter.TeeStore(storers...)
}

const (
	labelSeparator        = '\x1f'
	fieldTerminator       = '\x1e'
	nilByte          byte = 0
	sNilByte              = "\x00"
	defaultKeyPrefix      = "meter"
)

func Resolutions(resolutions ...Resolution) (map[time.Duration]Resolution, error) {
	m := make(map[time.Duration]Resolution, len(resolutions))
	for _, res := range resolutions {
		step := res.Step()
		if _, duplicate := m[step]; duplicate {
			return nil, fmt.Errorf(`Duplicate resolution %s`, step)
		}
		m[step] = res
	}
	return m, nil
}

func Open(rc redis.UniversalClient, scanSize int, keyPrefix string, resolutions ...Resolution) (*DB, error) {
	byDuration, err := Resolutions(resolutions...)
	if err != nil {
		return nil, err
	}
	if scanSize <= 0 {
		scanSize = defaultScanSize
	}
	db := DB{
		redis:       rc,
		scanSize:    int64(scanSize),
		keyPrefix:   keyPrefix,
		events:      make(map[string]meter.Storer),
		resolutions: byDuration,
	}
	return &db, nil
}

func (db *DB) Close() error {
	return db.redis.Close()
}

type scanResult struct {
	Time   int64
	Count  int64
	Fields meter.Fields
	Event  string
}
type scanners struct {
	*DB
	res Resolution
}

func (s scanners) Scanner(event string) meter.Scanner {
	return &storer{
		DB:         s.DB,
		event:      event,
		Resolution: s.res,
	}
}

func (db *DB) Query(ctx context.Context, q meter.Query, events ...string) (meter.Results, error) {
	res, ok := db.resolutions[q.Step]
	if !ok {
		return nil, fmt.Errorf("Invalid query step %s", q.Step)
	}
	s := scanners{db, res}
	return q.Scan(ctx, s, events...)
}

func (db *storer) Store(s *meter.Snapshot) error {
	if len(s.Counters) == 0 {
		return nil
	}
	labels := s.Labels
	sort.Strings(labels)
	pipeline := db.redis.Pipeline()
	defer pipeline.Close()
	var buf []byte
	key := db.Key(s.Time)
	pipeline.Expire(key, db.TTL())
	for j := range s.Counters {
		c := &s.Counters[j]
		buf := appendField(buf[:0], s.Labels, c.Values)
		field := string(buf)
		pipeline.HIncrBy(key, field, c.Count)
	}
	_, err := pipeline.Exec()
	return err
}

func (db *storer) appendKey(data []byte, tm time.Time) []byte {
	if db.keyPrefix != "" {
		data = append(data, db.keyPrefix...)
		data = append(data, labelSeparator)
	}
	data = append(data, db.Resolution.Name()...)
	data = append(data, labelSeparator)
	data = append(data, db.MarshalTime(tm)...)
	data = append(data, labelSeparator)
	data = append(data, db.event...)
	return data
}

func (db *storer) Key(tm time.Time) string {
	return string(db.appendKey(nil, tm))
}

const defaultScanSize = 1000

func (db *storer) Scan(ctx context.Context, q meter.TimeRange, match meter.Fields) (results meter.ScanResults, err error) {
	tm, end, step := db.Truncate(q.Start), db.Truncate(q.End), db.Step()
	for ; !tm.After(end); tm = tm.Add(step) {
		results, err = db.scan(tm, match, results)
		if err != nil {
			return
		}
	}
	return
}

func (db *storer) scan(tm time.Time, match meter.Fields, results meter.ScanResults) (meter.ScanResults, error) {
	key := db.Key(tm)
	ts := tm.Unix()
	scan := db.redis.HScan(key, 0, "*", db.scanSize).Iterator()
	var fields meter.Fields
	for i := 0; scan.Next(); i++ {
		if i%2 == 0 {
			fields = parseFields(fields[:0], scan.Val())
			sort.Sort(fields)
		} else if fields.MatchSorted(match) {
			n, err := strconv.ParseInt(scan.Val(), 10, 64)
			if err != nil {
				return results, err
			}
			results = results.Add(fields.Copy(), ts, float64(n))
		}
	}
	return results, scan.Err()
}

// func (db *DB) Scan(ctx context.Context, q *meter.Query) meter.ScanIterator {
// 	items := make(chan meter.ScanItem)
// 	errc := make(chan error)
// 	go func() {
// 		defer close(items)
// 		defer close(errc)
// 		// TODO: [redis] Handle q.Step <= 0 to Scan for keys before HSCAN
// 		ts := q.Sequence()
// 		wg := new(sync.WaitGroup)
// 		done := ctx.Done()
// 		for _, tm := range ts {
// 			wg.Add(1)
// 			tm := tm
// 			go func() {
// 				defer wg.Done()
// 				select {
// 				case errc <- db.scan(tm, q, items, done):
// 				case <-done:
// 				}
// 			}()

// 		}
// 		wg.Wait()
// 	}()

// 	return meter.NewScanIterator(ctx, items, errc)
// }

func parseFields(fields meter.Fields, s string) meter.Fields {
	pos := strings.IndexByte(s, fieldTerminator)
	if 0 <= pos && pos < len(s) {
		s = s[:pos]
	}
	offset := 0
	var label string
	var n int

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == labelSeparator {
			if n%2 == 0 {
				label = s[offset:i]
				offset = i + 1
			} else {
				value := s[offset:i]
				offset = i + 1
				if value == "\x00" {
					value = ""
				}
				fields = append(fields, meter.Field{
					Label: label,
					Value: value,
				})
				label = ""
			}
			n++
		}
	}
	if offset < len(s) {
		value := s[offset:]
		if value == "\x00" {
			value = ""
		}

		fields = append(fields, meter.Field{
			Label: label,
			Value: value,
		})
	}
	return fields
}
func appendField(data []byte, labels, values []string) []byte {
	n := len(values)
	for i := 0; i < len(labels); i++ {
		label := labels[i]
		if i != 0 {
			data = append(data, labelSeparator)
		}
		data = append(data, label...)
		data = append(data, labelSeparator)
		if i < n {
			value := values[i]
			data = append(data, value...)
		} else {
			data = append(data, nilByte)
		}
	}
	data = append(data, fieldTerminator)
	return data
}
