package meter2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/araddon/dateparse"
)

const (
	QueryParamEvent      = "event"
	QueryParamResolution = "res"
	QueryParamStart      = "start"
	QueryParamEnd        = "end"
	QueryParamGroup      = "group"
)

func ParseQuery(q url.Values) (queries []ScanQuery, err error) {
	eventNames := q[QueryParamEvent]
	queries = make([]ScanQuery, len(eventNames))
	delete(q, QueryParamEvent)
	if len(eventNames) == 0 {
		err = fmt.Errorf("Missing query.%s", QueryParamEvent)
		return
	}
	s := ScanQuery{}
	if _, ok := q[QueryParamResolution]; ok {
		s.Resolution = q.Get(QueryParamResolution)
		delete(q, QueryParamResolution)
	} else {
		err = fmt.Errorf("Missing query.%s", QueryParamResolution)
		return
	}
	if _, ok := q[QueryParamGroup]; ok {
		s.Group = q.Get(QueryParamGroup)
		delete(q, QueryParamGroup)
	}

	if start, ok := q[QueryParamStart]; !ok {
		err = fmt.Errorf("Missing query.%s", QueryParamStart)
		return
	} else if s.Start, err = dateparse.ParseAny(start[0]); err != nil {
		err = fmt.Errorf("Invalid query.%s: %s", QueryParamStart, err)
		return
	}
	delete(q, QueryParamStart)
	if end, ok := q[QueryParamEnd]; !ok {
		err = fmt.Errorf("Missing query.%s", QueryParamEnd)
		return
	} else if s.End, err = dateparse.ParseAny(end[0]); err != nil {
		err = fmt.Errorf("Invalid query.%s: %s", QueryParamEnd, err)
		return
	}
	delete(q, QueryParamEnd)
	s.Query = q
	for i, name := range eventNames {
		s.Event = name
		queries[i] = s
	}
	return

}

func (db *DB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		code := http.StatusMethodNotAllowed
		http.Error(w, http.StatusText(code), code)
		return
	}
	q := r.URL.Query()
	queries, err := ParseQuery(q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	results, _ := db.Query(queries...)
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(results)
	// http.NotFound(w, r)
}
