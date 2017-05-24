package meter

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis"
)

func (r *Registry) Mux(client redis.UniversalClient, maxresults int) *http.ServeMux {
	resolutions := make(map[string]*Resolution)
	for _, t := range r.types {
		for _, f := range t.filters {
			r := f.Resolution()
			resolutions[r.Name] = r
		}
	}
	mux := http.NewServeMux()
	for name, res := range resolutions {
		mux.Handle("/"+name, &Controller{
			Redis:      client,
			Registry:   r,
			Resolution: res,
			MaxRange:   res.Duration(),
			MaxResults: maxresults,
		})
	}
	return mux
}

type Controller struct {
	Redis      redis.UniversalClient
	Resolution *Resolution
	Registry   *Registry
	MaxResults int
	MaxRange   time.Duration
}

func (c Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	res := NoResolution
	if c.Resolution != nil {
		res = c.Resolution
	}
	start, end, err := res.ParseRange(q.Get("start"), q.Get("end"), c.MaxRange)
	delete(q, "start")
	delete(q, "end")
	if err != nil {
		http.Error(w, "Invalid date range", http.StatusBadRequest)
		return
	}

	format := q.Get("format")
	delete(q, "format")

	eventNames := q["event"]
	delete(q, "event")

	queries := PermutationPairs(q)
	// Append an empty query for overall stats
	if len(queries) == 0 {
		queries = append(queries, []string{})
	}
	records := make([]*Record, 0, c.MaxResults)
	for _, eventName := range eventNames {
		if t := c.Registry.Get(eventName); t != nil {
			for _, f := range t.Filters() {
				if f.Resolution() == res {
					records = append(records, t.Records(f, start, end, queries...)...)
				}
			}
		}
	}
	if c.MaxResults > 0 && len(records) > c.MaxResults {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		records = records[:c.MaxResults]
	}
	err = ReadRecords(c.Redis, records)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var results interface{}
	switch format {
	case "results":
		results = RecordSequence(records).Results()
	default:
		results = records
	}

	enc := json.NewEncoder(w)
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := enc.Encode(results); err != nil {
		log.Printf("Failed to write stats response: %s", err)
	}
}

type DataPoint [2]int64

type Result struct {
	Event  string
	Labels map[string]string
	Data   []DataPoint
}
