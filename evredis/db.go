package evredis

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/evutil"
	redis "github.com/alxarch/fastredis"
	"github.com/alxarch/fastredis/resp"
	errors "golang.org/x/xerrors"
)

// DB is an evdb backend using Redis
type DB struct {
	redis *redis.Pool
	evdb.Scanner
	keyPrefix   string
	scanSize    int64
	events      map[string]evdb.Storer
	resolutions map[time.Duration]Resolution
	extra       evutil.SyncStore
}

var _ evdb.DB = (*DB)(nil)

// Open opens a new DB
func Open(options Options, events ...string) (*DB, error) {
	byDuration, err := resolutionsByDuration(options.Resolutions...)
	if err != nil {
		return nil, err
	}
	if options.ScanSize <= 0 {
		options.ScanSize = defaultScanSize
	}
	pool := new(redis.Pool)
	if err := pool.ParseURL(options.Redis); err != nil {
		return nil, err
	}
	db := DB{
		redis:       pool,
		scanSize:    options.ScanSize,
		keyPrefix:   options.KeyPrefix,
		events:      make(map[string]evdb.Storer),
		resolutions: byDuration,
	}
	for _, event := range events {
		db.events[event] = db.storer(event)
	}
	db.Scanner = evdb.NewScanner(&db)
	return &db, nil
}

// Register registers a new event
func (db *DB) Register(event string) (evdb.Storer, error) {
	if w, ok := db.events[event]; ok {
		return w, nil
	}
	if w := db.extra.Storer(event); w != nil {
		return w, nil
	}
	if w := db.storer(event); db.extra.Register(event, w) {
		return w, nil
	}
	return db.extra.Storer(event), nil
}

// Storer provides a Storer for an event
func (db *DB) Storer(event string) evdb.Storer {
	if e, ok := db.events[event]; ok {
		return e
	}
	return nil
}

const (
	labelSeparator        = '\x1f'
	fieldTerminator       = '\x1e'
	nilByte          byte = 0
	sNilByte              = "\x00"
	defaultKeyPrefix      = "meter"
)

// Close closes a DB
func (db *DB) Close() error {
	// return db.redis.Close()
	return nil
}

// ScanQuery implements evdb.ScanQuerier interface
func (db *DB) ScanQuery(ctx context.Context, q *evdb.ScanQuery) (evdb.Results, error) {
	res, ok := db.resolutions[q.Step]
	if !ok {
		return nil, errors.Errorf("Invalid query step: %s", q.Step)
	}
	s := storer{
		DB:         db,
		event:      q.Event,
		Resolution: res,
	}
	return s.Scan(ctx, q.TimeRange, q.Fields)
}

type storer struct {
	*DB
	event string
	Resolution
}

func (db *DB) storer(event string) evdb.Storer {
	storers := make([]evdb.Storer, 0, len(db.resolutions))
	for _, res := range db.resolutions {
		storers = append(storers, &storer{
			DB:         db,
			event:      event,
			Resolution: res,
		})
	}
	return evutil.TeeStore(storers...)
}
func (db *storer) Store(s *evdb.Snapshot) error {
	if len(s.Counters) == 0 {
		return nil
	}
	labels := s.Labels
	sort.Strings(labels)
	p := db.redis.Pipeline()
	defer redis.ReleasePipeline(p)
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
func (db *storer) Scan(ctx context.Context, q evdb.TimeRange, m evdb.MatchFields) (results evdb.Results, err error) {
	var (
		key           []byte
		fields        evdb.Fields
		ts            int64
		index         = map[string]*evdb.Result{}
		skip          = &evdb.Result{}
		start         = db.Truncate(q.Start)
		tm, end, step = start, db.Truncate(q.End), db.Step()
		scan          = func(k []byte, v resp.Value) error {
			r := index[string(k)]
			if r == skip {
				return nil
			}
			if r == nil {
				fields = parseFields(fields[:0], string(k))
				sort.Sort(fields)
				if !m.Match(fields) {
					index[string(k)] = skip
					return nil
				}

				results = append(results, evdb.Result{
					TimeRange: q,
					Event:     db.event,
					Fields:    fields.Copy(),
					Data:      evdb.BlankData(&q, 0),
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
	conn, err := db.redis.Get()
	if err != nil {
		return nil, err
	}
	defer db.redis.Put(conn)
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

func parseFields(fields evdb.Fields, s string) evdb.Fields {
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
				fields = append(fields, evdb.Field{
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

		fields = append(fields, evdb.Field{
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
