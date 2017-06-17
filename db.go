package meter

import (
	"fmt"
	"net/url"
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
	MaxResults int
}
type DB struct {
	aliases Aliases
	reg     *Registry
	redis   redis.UniversalClient
}

const DefaultMaxResults = 10000

func (db *DB) LabelQueries(q url.Values) [][]string {
	if len(q) > 0 {
		labels := make(map[string][]string)
		for label, values := range q {
			labels[db.aliases.Alias(label)] = values
		}
		return PermutationPairs(labels)
	}
	// Append an empty query for overall stats
	return [][]string{[]string{}}
}
func (db *DB) Records(q Query) (rs RecordSequence, err error) {
	queries := db.LabelQueries(q.Labels)
	records := []Record{}
	for _, eventName := range q.Events {
		if t := db.reg.Get(eventName); t != nil {
			records = append(records, t.Records(q.Resolution, q.Start, q.End, queries)...)
		} else {
			return nil, fmt.Errorf("Event %s not found.", eventName)
		}
	}
	if q.MaxResults > 0 && len(records) > q.MaxResults {
		records = records[:q.MaxResults]
	}
	if err = ReadRecords(db.redis, records); err != nil {
		return
	}
	rs = RecordSequence(records)
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
