package meter

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func (db *DB) Mux(maxrecords int, resolutions ...*Resolution) *http.ServeMux {
	mux := http.NewServeMux()
	for _, res := range resolutions {
		mux.Handle("/"+res.Name, &Controller{
			DB:         db,
			Resolution: res,
			MaxRecords: maxrecords,
		})
	}
	return mux
}

type Controller struct {
	Resolution *Resolution
	DB         *DB
	MaxRecords int
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
	start, end, err := res.ParseDateRange(q.Get("start"), q.Get("end"))
	if err != nil {
		http.Error(w, "Invalid date range", http.StatusBadRequest)
		return
	}

	format := q.Get("format")
	eventNames := q["event"]
	_, grouped := q["grouped"]
	query := Query{
		Resolution: res,
		MaxRecords: c.MaxRecords,
		Events:     eventNames,
		Labels:     SubQuery(q, "q:"),
		Grouped:    grouped,
		Start:      start,
		End:        end,
	}

	var results interface{}
	switch format {
	case "results":
		results, err = c.DB.Results(query)
	default:
		results, err = c.DB.Records(query)
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
	Value     int64
}

func (d *DataPoint) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf("[%d,%d]", d.Timestamp, d.Value)
	return []byte(s), nil
}

type Result struct {
	Event  string
	Labels map[string]string
	Data   []DataPoint
}
