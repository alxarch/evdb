package mdbredis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	redis "github.com/alxarch/fastredis"
	"github.com/alxarch/fastredis/resp"
	meter "github.com/alxarch/go-meter/v2"
)

type DB struct {
	redis       *redis.Pool
	keyPrefix   string
	scanSize    int64
	events      map[string]meter.Storer
	resolutions map[time.Duration]Resolution
}

var _ meter.DB = (*DB)(nil)

func (db *DB) Storer(event string) meter.Storer {
	if e, ok := db.events[event]; ok {
		return e
	}
	return nil
}

type storer struct {
	*DB
	event string
	Resolution
}

func (db *DB) storer(event string) meter.Storer {
	storers := make([]meter.Storer, 0, len(db.resolutions))
	for _, res := range db.resolutions {
		storers = append(storers, &storer{
			DB:         db,
			event:      event,
			Resolution: res,
		})
	}
	return meter.TeeStore(storers...)
}

const (
	labelSeparator        = '\x1f'
	fieldTerminator       = '\x1e'
	nilByte          byte = 0
	sNilByte              = "\x00"
	defaultKeyPrefix      = "meter"
)

func Open(options Options, events ...string) (*DB, error) {
	byDuration, err := resolutionsByDuration(options.Resolutions...)
	if err != nil {
		return nil, err
	}
	if options.ScanSize <= 0 {
		options.ScanSize = defaultScanSize
	}
	db := DB{
		redis:       redis.NewPool(&options.Redis),
		scanSize:    options.ScanSize,
		keyPrefix:   options.KeyPrefix,
		events:      make(map[string]meter.Storer),
		resolutions: byDuration,
	}
	for _, event := range events {
		db.events[event] = db.storer(event)
	}
	return &db, nil
}

func (db *DB) Close() error {
	// return db.redis.Close()
	return nil
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
	if now := time.Now(); q.End.After(now) {
		q.End = now
	}
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
	p := redis.BlankPipeline()
	defer p.Close()
	var buf []byte
	key := db.Key(s.Time)
	p.Expire(key, db.TTL())
	for j := range s.Counters {
		c := &s.Counters[j]
		buf := appendField(buf[:0], s.Labels, c.Values)
		field := string(buf)
		p.HIncrBy(key, field, c.Count)
	}
	return db.redis.Do(p, nil)
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

// func (db *storer) readAll(key string) (map[string]int64, error) {
// 	p := redis.Get()
// 	conn := db.redis.Get()
// 	defer conn.Close()
// 	return redis.Int64Map(conn.Do("HGETALL", key))
// }
func (db *storer) Scan(ctx context.Context, q meter.TimeRange, match meter.Fields) (results meter.ScanResults, err error) {
	var (
		p             = redis.BlankPipeline()
		key           []byte
		fields        meter.Fields
		ts            int64
		index         = map[string]*meter.ScanResult{}
		skip          = &meter.ScanResult{}
		tm, end, step = db.Truncate(q.Start), db.Truncate(q.End), db.Step()
		scan          = func(v resp.Value, k []byte) error {
			r := index[string(k)]
			if r == skip {
				return nil
			}
			if r == nil {
				fields = parseFields(fields[:0], string(k))
				sort.Sort(fields)
				if !fields.MatchSorted(match) {
					index[string(k)] = skip
					return nil
				}

				results = append(results, meter.ScanResult{
					Fields: fields.Copy(),
					Data:   meter.DataPoints{{Timestamp: ts, Value: 0}},
				})
				r = &results[len(results)-1]
				index[string(k)] = r
			}
			n, err := strconv.ParseFloat(string(v.Bytes()), 64)
			if err != nil {
				return err
			}
			r.Data = r.Data.Add(ts, n)
			return nil
		}
	)
	defer p.Close()
	conn, err := db.redis.Get(time.Time{})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	for ; !tm.After(end); tm = tm.Add(step) {
		key = db.appendKey(key[:0], tm)
		iter := redis.HScan(string(key), "", db.scanSize)
		ts = tm.Unix()
		if err := iter.Each(conn, scan); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return
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
