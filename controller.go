package meter

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (db *DB) Mux(maxresults int) *http.ServeMux {
	resolutions := make(map[string]*Resolution)
	for _, t := range db.reg.types {
		for _, f := range t.filters {
			r := f.Resolution()
			resolutions[r.Name] = r
		}
	}
	mux := http.NewServeMux()
	for name, res := range resolutions {
		mux.Handle("/"+name, &Controller{
			db:         db,
			Resolution: res,
			MaxRange:   res.Duration(),
			MaxResults: maxresults,
		})
	}
	return mux
}

type Controller struct {
	Resolution *Resolution
	db         *DB
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
	if err != nil {
		http.Error(w, "Invalid date range", http.StatusBadRequest)
		return
	}

	format := q.Get("format")
	eventNames := q["event"]
	_, grouped := q["groupbyevent"]
	query := Query{
		Resolution: res,
		MaxResults: c.MaxResults,
		Events:     eventNames,
		Labels:     ParseQueryLabels(q["label"]),
		Grouped:    grouped,
		Start:      start,
		End:        end,
	}

	var results interface{}
	switch format {
	case "results":
		results, err = c.db.Results(query)
	default:
		results, err = c.db.Records(query)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	enc := json.NewEncoder(w)
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := enc.Encode(results); err != nil {
		log.Printf("Failed to write stats response: %s", err)
	}
}

type DataPoint struct {
	Timestamp int64
	Value     float64
}

func (d *DataPoint) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf("[%d,%f]", d.Timestamp, d.Value)
	return []byte(s), nil
}

type Result struct {
	Event  string
	Labels map[string]string
	Data   []DataPoint
}

func ParseQueryLabels(values []string) url.Values {
	labels := url.Values{}
	for _, p := range values {
		parts := strings.SplitN(p, ":", 2)
		if len(parts) == 2 {
			labels.Add(parts[0], parts[1])
		}
	}
	return labels
}
