package httpdb

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	meter "github.com/alxarch/go-meter/v2"
)

// HTTPClient does HTTP requests
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Querier runs queries over http
type Querier struct {
	URL string
	HTTPClient
}

// Query implements Querier interface
func (qr *Querier) Query(ctx context.Context, q meter.Query, events ...string) (meter.Results, error) {
	u, err := q.URL(qr.URL, events...)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	c := qr.HTTPClient
	if c == nil {
		c = http.DefaultClient
	}
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New(`Invalid response status`)
	}
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var results meter.Results
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, err
	}
	return results, nil

}

// QueryHandler returns an HTTP endpoint for a QueryRunner
func QueryHandler(qr meter.Querier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		events := values["event"]
		q := meter.Query{}
		q.SetValues(values)
		if q.Start.IsZero() {
			q.Start = time.Unix(0, 0)
		}
		if q.End.IsZero() {
			q.End = time.Now()
		}
		typ := meter.ResultTypeFromString(values.Get("results"))
		if typ == meter.TotalsResult {
			q.Step = -1
		}
		ctx := r.Context()
		results, err := qr.Query(ctx, q, events...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var x interface{}
		switch typ {
		case meter.TotalsResult:
			x = results.Totals()
		case meter.FieldSummaryResult:
			x = results.FieldSummaries()
		case meter.EventSummaryResult:
			x = results.EventSummaries(q.EmptyValue)
		default:
			x = results
		}
		enc := json.NewEncoder(w)
		w.Header().Set("Content-Type", "application/json")
		enc.Encode(x)
	}
}
