package mdbhttp

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
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

func (qr *Querier) queryURL(q *meter.Query, events ...string) (string, error) {
	u, err := url.Parse(qr.URL)
	if err != nil {
		return "", err
	}
	values := url.Values(make(map[string][]string))
	values.Set("start", strconv.FormatInt(q.Start.Unix(), 10))
	values.Set("end", strconv.FormatInt(q.End.Unix(), 10))
	for _, label := range q.Group {
		values.Add("group", label)
	}
	if q.Step != 0 {
		values.Set("step", q.Step.String())
	}
	match := q.Match.Sorted()
	for _, field := range match {
		values.Add(`match.`+field.Label, field.Value)
	}
	if q.EmptyValue != "" {
		values.Set("empty", q.EmptyValue)
	}
	for _, event := range events {
		values.Add("event", event)
	}
	u.RawQuery = values.Encode()
	return u.String(), nil

}

// Query implements Querier interface
func (qr *Querier) Query(ctx context.Context, q meter.Query, events ...string) (meter.Results, error) {
	u, err := qr.queryURL(&q, events...)
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
		ParseQuery(&q, values)
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

// ParseQuery sets query values from a URL query
func ParseQuery(q *meter.Query, values url.Values) {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			q.Step, _ = time.ParseDuration(step[0])
		} else {
			q.Step = 0
		}
	} else {
		q.Step = -1
	}
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	if start > 0 {
		q.Start = time.Unix(start, 0).In(time.UTC)
	}
	if end, _ := strconv.ParseInt(values.Get("end"), 10, 64); end > 0 {
		q.End = time.Unix(end, 0).In(time.UTC)
	}

	match := q.Match[:0]
	for key, values := range values {
		if strings.HasPrefix(key, "match.") {
			label := strings.TrimPrefix(key, "match.")
			for _, value := range values {
				match = append(match, meter.Field{
					Label: label,
					Value: value,
				})
			}
		}
	}
	group, ok := values["group"]
	if ok && len(group) == 0 {
		group = make([]string, 0, len(match))
		for i := range match {
			m := &match[i]
			group = appendDistinct(group, m.Label)
		}
	}
	sort.Stable(match)
	q.Match, q.Group = match, group
	q.EmptyValue = values.Get("empty")
}

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

func appendDistinct(dst []string, src ...string) []string {
	for i, s := range src {
		if indexOf(dst, s[:i]) == -1 {
			dst = append(dst, s)
		}
	}
	return dst
}
