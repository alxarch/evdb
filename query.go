package meter

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
)

// Query is a query for event results
type Query struct {
	TimeRange
	Match      Fields   `json:"match,omitempty"`
	Group      []string `json:"group,omitempty"`
	EmptyValue string   `json:"empty,omitempty"`
}

// URL adds the query to a URL
func (q *Query) URL(baseURL string, events ...string) (string, error) {
	u, err := url.Parse(baseURL)
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

// TruncateTimestamp truncates a timestamp to Query.Step
func (q *Query) TruncateTimestamp(ts int64) int64 {
	step := int64(q.Step / time.Second)
	return stepTS(ts, step)
}

// SetValues sets query values from a URL query
func (q *Query) SetValues(values url.Values) {
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
				match = append(match, Field{
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

// TimeRange is a range of time with a specific step
type TimeRange struct {
	Start time.Time     `json:"start"`
	End   time.Time     `json:"end"`
	Step  time.Duration `json:"step"`
}

// QueryRunner runs queries
type QueryRunner interface {
	RunQuery(ctx context.Context, q *Query, events ...string) (Results, error)
}

// QueryHandler returns an HTTP endpoint for a QueryRunner
func QueryHandler(qr QueryRunner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		events := values["event"]
		q := Query{}
		q.SetValues(values)
		if q.Start.IsZero() {
			q.Start = time.Unix(0, 0)
		}
		if q.End.IsZero() {
			q.End = time.Now()
		}
		typ := ResultTypeFromString(values.Get("results"))
		if typ == TotalsResult {
			q.Step = -1
		}
		ctx := r.Context()
		results, err := qr.RunQuery(ctx, &q, events...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var x interface{}
		switch typ {
		case TotalsResult:
			x = results.Totals()
		case FieldSummaryResult:
			x = results.FieldSummaries()
		case EventSummaryResult:
			x = results.EventSummaries(q.EmptyValue)
		default:
			x = results
		}
		enc := json.NewEncoder(w)
		w.Header().Set("Content-Type", "application/json")
		enc.Encode(x)
	}
}

// HTTPQueryRunner runs queries over http
type HTTPQueryRunner struct {
	URL    string
	Client *http.Client
}

// RunQuery implements QueryRunner interface
func (qr *HTTPQueryRunner) RunQuery(ctx context.Context, q *Query, events ...string) (Results, error) {
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
	c := qr.Client
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
	var results Results
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, err
	}
	return results, nil

}
