package meter

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
)

func (db *DB) Mux(maxrecords int, resolutions ...*Resolution) *http.ServeMux {
	mux := http.NewServeMux()
	for _, res := range resolutions {
		mux.Handle("/"+res.Name+"/summary", &SummaryController{
			DB:         db,
			Resolution: res,
		})
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

func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	_, grouped := q["grouped"]
	delete(q, "grouped")
	aq := url.Values{}
	for k, v := range q {
		aq[Alias(k)] = v
	}
	query := Query{
		Resolution: res,
		MaxRecords: c.MaxRecords,
		Events:     eventNames,
		Labels:     aq,
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

type SummaryController struct {
	Resolution *Resolution
	DB         *DB
	MaxRecords int
}

func (c *SummaryController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	delete(q, "start")
	delete(q, "end")

	eventName := q.Get("event")
	delete(q, "event")
	group := q.Get("group")
	delete(q, "group")
	labels := []string{}
	for k, v := range q {
		if len(v) > 0 {
			labels = append(labels, k, v[0])
		}
	}
	ts := TimeSequence(start, end, res.Step())
	results := Summary(make(map[string]int64))
	for _, tm := range ts {
		q := SummaryQuery{
			Time:       tm,
			Event:      eventName,
			Resolution: res,
			Labels:     labels,
			Group:      group,
		}
		s, err := c.DB.SummaryScan(q)
		if err != nil {
			switch err {
			case UnregisteredEventError, InvalidEventLabelError:
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		results.Add(s)
	}

	enc := json.NewEncoder(w)
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := enc.Encode(results); err != nil {
		log.Printf("Failed to write stats response: %s", err)
	}
}
