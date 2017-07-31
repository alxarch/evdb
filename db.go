package meter

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type Query struct {
	Events     []string
	Start      time.Time
	End        time.Time
	Labels     map[string][]string
	Resolution *Resolution
	Grouped    bool
	MaxRecords int
}

type DB struct {
	Aliases  Aliases
	Registry *Registry
	Redis    redis.UniversalClient
}

const DefaultMaxResults = 10000

var (
	MaxRecordsError = errors.New("Max records reached")
)

func (q *Query) Records(r *Registry) (rs RecordSequence, err error) {
	var queries [][]string
	if len(q.Labels) == 0 {
		queries = [][]string{[]string{}}
	} else {
		queries = PermutationPairs(q.Labels)
	}
	records := []*Record{}
	for _, eventName := range q.Events {
		if e := r.Get(eventName); e != nil {
			records = append(records, e.Records(q.Resolution, q.Start, q.End, queries...)...)
		} else {
			return nil, fmt.Errorf("Event %s not found.", eventName)
		}
	}
	if q.MaxRecords > 0 && len(records) > q.MaxRecords {
		records = records[:q.MaxRecords]
		err = MaxRecordsError
	}
	rs = RecordSequence(records)
	return

}

func (db *DB) Records(q Query) (rs RecordSequence, err error) {
	rs, err = q.Records(db.Registry)
	if err == nil || err == MaxRecordsError {
		if e := ReadRecords(db.Redis, rs); e != nil {
			err = e
		}
	}
	return
}

func (db *DB) Results(q Query) (results []*Result, err error) {
	var rs RecordSequence
	if rs, err = db.Records(q); err == nil {
		if q.Grouped {
			results = rs.Group()
		} else {
			results = rs.Results()
		}
	}
	return
}

type SummaryQuery struct {
	Time       time.Time
	Event      string
	Labels     []string
	Group      string
	Resolution *Resolution
}

type Summary map[string]int64

func (s Summary) Add(other Summary) {
	for key, count := range other {
		s[key] += count
	}
}
func (db *DB) SummaryScan(q SummaryQuery) (sum Summary, err error) {
	res := q.Resolution
	if res == nil {
		return nil, NilResolutionError
	}
	event := db.Registry.Get(q.Event)
	if event == nil {
		return nil, UnregisteredEventError
	}
	group := db.Aliases.Alias(q.Group)
	if !event.HasLabel(group) {
		return nil, InvalidEventLabelError
	}
	labels := event.AliasedLabels(q.Labels, db.Aliases)
	var match string
	{
		labels[event.ValueIndex(group)] = "*"
		match = event.Field(labels...)
	}
	cursor := uint64(0)
	key := event.Key(res, q.Time, labels)
	fields := []string{}
	for {
		reply := db.Redis.HScan(key, cursor, match, -1)
		var keys []string
		if keys, cursor, err = reply.Result(); err != nil {
			return
		}
		fields = append(fields, keys...)
		if cursor == 0 {
			break
		}
	}
	var values []interface{}
	if values, err = db.Redis.HMGet(key, fields...).Result(); err != nil {
		return
	}
	sum = Summary(make(map[string]int64, len(values)))
	for i, field := range fields {
		labels := Labels(strings.Split(field, LabelSeparator))
		if key, ok := labels.Get(group); ok {
			switch value := values[i].(type) {
			case string:
				if n, e := strconv.ParseInt(value, 10, 64); e == nil {
					sum[key] += n
				}
			case int64:
				sum[key] += value
			}
		}
	}
	return

}
func NewDB(r redis.UniversalClient) *DB {
	return &DB{
		Registry: defaultRegistry,
		Aliases:  defaultAliases,
		Redis:    r,
	}
}
