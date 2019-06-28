package redisdb

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/go-redis/redis"
)

type DB struct {
	Event      string
	Redis      redis.UniversalClient
	KeyPrefix  string
	ScanSize   int64
	Resolution Resolution
}

const (
	labelSeparator        = '\x1f'
	fieldTerminator       = '\x1e'
	nilByte          byte = 0
	sNilByte              = "\x00"
	defaultKeyPrefix      = "meter"
)

type Event struct {
	Name        string
	Resolutions []Resolution
}

func E(name string, resolutions ...Resolution) Event {
	return Event{Name: name, Resolutions: resolutions}
}

func EventStore(rc redis.UniversalClient, scanSize int, keyPrefix string, events ...Event) meter.EventStore {
	type dbKey struct {
		Name       string
		Resolution string
	}
	dbs := make(map[dbKey]*DB)
	stores := make(map[string]meter.Storer, len(events))
	for i := range events {
		e := &events[i]
		tee := []meter.Storer{}
		for _, r := range e.Resolutions {
			key := dbKey{e.Name, r.Name()}
			db := dbs[key]
			if db == nil {
				db = &DB{
					Redis:      rc,
					Event:      e.Name,
					ScanSize:   int64(scanSize),
					KeyPrefix:  keyPrefix,
					Resolution: r,
				}
				dbs[key] = db
			}
			tee = append(tee, db)
		}
		stores[e.Name] = meter.TeeStore(tee...)
	}
	return stores
}

func (db *DB) Close() error {
	return db.Redis.Close()
}
func (db *DB) Scanner(event string) meter.Scanner {
	if event == db.Event {
		return db
	}
	return nil
}
func (db *DB) Query(ctx context.Context, q *meter.Query, events ...string) (meter.Results, error) {
	return meter.ScanQuerier(db).Query(ctx, q, events...)
}

func (db *DB) Store(s *meter.Snapshot) error {
	if len(s.Counters) == 0 {
		return nil
	}
	labels := s.Labels
	sort.Strings(labels)
	pipeline := db.Redis.Pipeline()
	defer pipeline.Close()
	var buf []byte
	r := db.Resolution
	key := db.Key(s.Event, s.Time)
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
func (db *DB) appendKey(data []byte, tm time.Time) []byte {
	if db.KeyPrefix != "" {
		data = append(data, db.KeyPrefix...)
		data = append(data, labelSeparator)
	}
	r := db.Resolution
	data = append(data, r.Name()...)
	data = append(data, labelSeparator)
	data = append(data, r.MarshalTime(tm)...)
	data = append(data, labelSeparator)
	data = append(data, db.Event...)
	return data
}

func (db *DB) Key(event string, tm time.Time) string {
	return string(db.appendKey(nil, tm))
}

const defaultScanSize = 1000

func (db *DB) scanSize() int64 {
	size := int64(db.ScanSize)
	if size > 0 {
		return size
	}
	return defaultScanSize
}

func (db *DB) scan(tm time.Time, q *meter.Query, items chan<- meter.ScanItem, done <-chan struct{}) error {
	key := db.Key(db.Event, tm)
	ts := tm.Unix()
	scan := db.Redis.HScan(key, 0, "*", db.scanSize()).Iterator()
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
			grouped = fields.Copy().GroupBy(q.EmptyValue, q.Group)
			select {
			case items <- meter.ScanItem{
				Time:   ts,
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

func (db *DB) Scan(ctx context.Context, q *meter.Query) meter.ScanIterator {
	items := make(chan meter.ScanItem)
	errc := make(chan error)
	go func() {
		defer close(items)
		defer close(errc)
		// TODO: [redis] Handle q.Step <= 0 to Scan for keys before HSCAN
		ts := q.Sequence()
		wg := new(sync.WaitGroup)
		done := ctx.Done()
		for _, tm := range ts {
			wg.Add(1)
			tm := tm
			go func() {
				defer wg.Done()
				select {
				case errc <- db.scan(tm, q, items, done):
				case <-done:
				}
			}()

		}
		wg.Wait()
	}()

	return meter.NewScanIterator(ctx, items, errc)
}

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
