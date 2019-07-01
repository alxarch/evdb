package redisdb

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
	db    *DB
	event string
	res   Resolution
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
			db:    db,
			event: event,
			res:   res,
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

func (db *DB) Query(ctx context.Context, q meter.Query, events ...string) (meter.Results, error) {
	res, ok := db.resolutions[q.Step]
	if !ok {
		return nil, fmt.Errorf("Invalid query step %s", q.Step)
	}
	ch := make(chan scanResult, len(events))
	errc := make(chan error, len(events))
	done := ctx.Done()
	ts := q.Sequence()
	wg := new(sync.WaitGroup)
	results := meter.Results(make([]meter.Result, 0, len(events)))
	go func() {
		defer close(errc)
		for {
			select {
			case r, ok := <-ch:
				if ok {
					results = results.Add(r.Event, r.Fields, r.Time, float64(r.Count))
				} else {
					return
				}
			case <-done:
				return
			}
		}
	}()
	for i := range events {
		event := events[i]
		if _, ok := db.events[event]; !ok {
			continue
		}
		s := storer{
			event: events[i],
			res:   res,
			db:    db,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			for _, tm := range ts {
				err = s.scan(tm, &q, ch, done)
				if err != nil {
					break
				}

			}
			errc <- err
		}()

	}
	wg.Wait()
	close(ch)
	for err := range errc {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (db *storer) Store(s *meter.Snapshot) error {
	if len(s.Counters) == 0 {
		return nil
	}
	labels := s.Labels
	sort.Strings(labels)
	pipeline := db.db.redis.Pipeline()
	defer pipeline.Close()
	var buf []byte
	r := db.res
	key := db.Key(s.Time)
	pipeline.Expire(key, r.TTL())
	for j := range s.Counters {
		c := &s.Counters[j]
		buf := appendField(buf[:0], s.Labels, c.Values)
		field := string(buf)
		pipeline.HIncrBy(key, field, c.Count)
	}
	_, err := pipeline.Exec()
	return err
}

func (s *storer) appendKey(data []byte, tm time.Time) []byte {
	if s.db.keyPrefix != "" {
		data = append(data, s.db.keyPrefix...)
		data = append(data, labelSeparator)
	}
	data = append(data, s.res.Name()...)
	data = append(data, labelSeparator)
	data = append(data, s.res.MarshalTime(tm)...)
	data = append(data, labelSeparator)
	data = append(data, s.event...)
	return data
}

func (s *storer) Key(tm time.Time) string {
	return string(s.appendKey(nil, tm))
}

const defaultScanSize = 1000

func (s *storer) scan(tm time.Time, q *meter.Query, items chan<- scanResult, done <-chan struct{}) error {
	key := s.Key(tm)
	ts := tm.Unix()
	scan := s.db.redis.HScan(key, 0, "*", s.db.scanSize).Iterator()
	i := 0
	var fields, grouped meter.Fields
	match := q.Match.Sorted()
	for scan.Next() {
		if i%2 == 0 {
			fields = parseFields(fields[:0], scan.Val())
			sort.Sort(fields)
		} else if fields.MatchSorted(match) {
			n, err := strconv.ParseInt(scan.Val(), 10, 64)
			if err != nil {
				return err
			}
			if len(q.Group) > 0 {
				grouped = fields.GroupBy(q.EmptyValue, q.Group)
			} else {
				grouped = fields.Copy()
			}
			select {
			case items <- scanResult{
				Time:   ts,
				Event:  s.event,
				Fields: grouped,
				Count:  n,
			}:
			case <-done:
				return nil
			}
		}
		i++
	}
	return scan.Err()
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
