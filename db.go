package meter

import (
	"errors"
	"fmt"
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
	records := []Record{}
	for _, eventName := range q.Events {
		if t := r.Get(eventName); t != nil {
			records = append(records, t.Records(q.Resolution, q.Start, q.End, queries)...)
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
