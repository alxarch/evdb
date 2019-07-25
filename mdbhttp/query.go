package mdbhttp

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	errors "golang.org/x/xerrors"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/alxarch/httperr"
)

// Querier runs queries over http
type Querier struct {
	URL string
	HTTPClient
}

// HTTPClient does HTTP requests
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ParseTime parses time in various formats
func ParseTime(v string) (time.Time, error) {
	if strings.Contains(v, ":") {
		if strings.Contains(v, ".") {
			return time.ParseInLocation(time.RFC3339Nano, v, time.UTC)
		}
		return time.ParseInLocation(time.RFC3339, v, time.UTC)
	}
	if strings.Contains(v, "-") {
		return time.ParseInLocation("2006-01-02", v, time.UTC)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(n, 0), nil
}

func (qr *Querier) evalURL(q *meter.ScanQuery, exp ...string) (string, error) {
	u, err := url.Parse(qr.URL)
	if err != nil {
		return "", err
	}
	values := QueryValues(q.TimeRange)
	for _, e := range exp {
		values.Add("eval", e)
	}
	u.RawQuery = values.Encode()
	return u.String(), nil
}

func (qr *Querier) queryURL(r meter.TimeRange) (string, error) {
	u, err := url.Parse(qr.URL)
	if err != nil {
		return "", err
	}
	values := QueryValues(r)
	// // for _, event := range events {
	// values.Add("event", q.Event)
	// // }
	u.RawQuery = values.Encode()
	return u.String(), nil

}

// Eval implements meter.Evaler interface
func (qr *Querier) Query(ctx context.Context, r meter.TimeRange, q string) ([]interface{}, error) {
	body := strings.NewReader(q)
	u, err := qr.queryURL(r)
	if err != nil {
		return nil, err

	}
	req, err := http.NewRequest(http.MethodPost, u, body)
	if err != nil {
		return nil, err
	}
	return qr.send(ctx, req)
}

func (qr *Querier) send(ctx context.Context, req *http.Request) ([]interface{}, error) {
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
	if httperr.IsError(res.StatusCode) {
		return nil, httperr.FromResponse(res)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Errorf(`Failed to read response: %s`, err)
	}
	var results []interface{}
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, errors.Errorf(`Failed to parse response: %s`, err)
	}
	return results, nil
}

func (qr *Querier) get(ctx context.Context, u string) ([]interface{}, error) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	return qr.send(ctx, req)
}

// QueryValues converts a meter.Query to url.Values
func QueryValues(r meter.TimeRange) url.Values {
	values := url.Values(make(map[string][]string))
	values.Set("start", strconv.FormatInt(r.Start.Unix(), 10))
	values.Set("end", strconv.FormatInt(r.End.Unix(), 10))
	values.Set("step", r.Step.String())
	// for _, label := range q.Group {
	// 	values.Add("group", label)
	// }
	// if q.Step != 0 {
	// 	values.Set("step", q.Step.String())
	// }
	// match := q.Match.Sorted()
	// for _, field := range match {
	// 	values.Add(`match.`+field.Label, field.Value)
	// }
	// if q.EmptyValue != "" {
	// 	values.Set("empty", q.EmptyValue)
	// }
	return values
}

// QueryHandler returns an HTTP endpoint for a QueryRunner
func QueryHandler(querier meter.Querier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		var q meter.TimeRange
		if err := ParseQueryTimeRange(&q, values); err != nil {
			httperr.RespondJSON(w, httperr.BadRequest(err))
			return
		}
		if q.Start.IsZero() {
			q.Start = time.Unix(0, 0)
		}
		if q.End.IsZero() {
			q.End = time.Now()
		}
		results, err := querier.Query(r.Context(), q, values.Get("query"))
		if err != nil {
			httperr.RespondJSON(w, errors.Errorf("Query evaluation failed: %s", err))
			return
		}
		httperr.RespondJSON(w, results)
	}
}

func ParseQueryTimeRange(tr *meter.TimeRange, values url.Values) error {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			tr.Step, _ = time.ParseDuration(step[0])
		} else {
			tr.Step = 0
		}
	} else {
		tr.Step = -1
	}
	start, err := ParseTime(values.Get("start"))
	if err != nil {
		return err
	}
	if !start.IsZero() {
		tr.Start = start
	}
	end, err := ParseTime(values.Get("end"))
	if err != nil {
		return err
	}
	if !end.IsZero() {
		tr.End = end
	}
	return nil

}

// ParseQuery sets query values from a URL query
func ParseQuery(values url.Values) (q meter.ScanQuery, err error) {
	if err = ParseQueryTimeRange(&q.TimeRange, values); err != nil {
		return
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
	// q.Match, q.Group = match, group
	q.Match = match
	// q.EmptyValue = values.Get("empty")
	return
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

func parseQuery(start, end, step, query string) (*Query, error) {
	q := Query{
		Query: query,
	}
	tmin, err := ParseTime(start)
	if err != nil {
		return nil, errors.Errorf("Invalid start: %s", err)
	}
	q.Start = tmin
	tmax, err := ParseTime(end)
	if err != nil {
		return nil, errors.Errorf("Invalid end: %s", err)
	}
	q.End = tmax
	d, err := time.ParseDuration(step)
	if err != nil {
		return nil, errors.Errorf("Invalid step: %s", err)
	}
	q.Step = d
	return &q, nil
}
func ParseJSONQuery(data []byte) (*Query, error) {
	q := struct {
		Query string `json:"query"`
		Start string `json:"start"`
		End   string `json:"end"`
		Step  string `json:"step"`
	}{}
	if err := json.Unmarshal(data, &q); err != nil {
		return nil, errors.Errorf("Invalid JSON body: %s", err)
	}
	return parseQuery(q.Start, q.End, q.Step, q.Query)
}
func ParseFormQuery(data string) (*Query, error) {
	values, err := url.ParseQuery(data)
	if err != nil {
		return nil, errors.Errorf("Invalid querystring: %s", err)
	}
	return parseQuery(values.Get("start"), values.Get("end"), values.Get("step"), values.Get("query"))
}
