package meter

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/go-redis/redis"
)

type Record struct {
	Name   string
	Time   time.Time
	Key    string
	Field  string
	Labels []string
	Result *redis.StringCmd
}

func NewRecord(name string, t time.Time, a Attributes) *Record {
	return &Record{
		Name:   name,
		Time:   t,
		Labels: a.Copy(),
	}
}

func (r *Record) Count() int64 {
	if r.Result != nil {
		if n, err := r.Result.Int64(); err == nil {
			return n
		}
	}
	return 0
}

func (r *Record) MarshalJSON() ([]byte, error) {
	obj := make(map[string]interface{})
	for i := 0; i < len(r.Labels); i += 2 {
		obj[r.Labels[i]] = r.Labels[i+1]
	}
	obj["count"] = r.Count()
	obj["time"] = r.Time.String()
	obj["name"] = r.Name

	return json.Marshal(obj)
}

func ReadRecords(r redis.UniversalClient, records []*Record) error {
	pipeline := r.Pipeline()
	defer pipeline.Close()
	for _, r := range records {
		r.Result = pipeline.HGet(r.Key, r.Field)
	}
	_, err := pipeline.Exec()
	return err
}

type RecordSequence []*Record

func (s RecordSequence) Results() []*Result {
	grouped := make(map[string]*Result)
	for _, r := range s {
		key := r.Name + ":" + r.Field
		result, ok := grouped[key]
		if !ok {
			result = &Result{
				Event:  r.Name,
				Labels: Attributes(r.Labels).Map(),
				Data:   make([]DataPoint, 0, len(s)),
			}
			grouped[key] = result
		}
		result.Data = append(result.Data, DataPoint{r.Time.Unix(), r.Count()})
	}
	results := make([]*Result, len(grouped))
	i := 0
	for _, r := range grouped {
		sort.Slice(r.Data, func(i, j int) bool {
			return r.Data[i][0] < r.Data[j][0]
		})
		results[i] = r
		i++
	}
	return results
}
