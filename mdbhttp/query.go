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

func (qr *Querier) evalURL(q *meter.Query, exp ...string) (string, error) {
	u, err := url.Parse(qr.URL)
	if err != nil {
		return "", err
	}
	values := QueryValues(q)
	for _, e := range exp {
		values.Add("eval", e)
	}
	u.RawQuery = values.Encode()
	return u.String(), nil
}

func (qr *Querier) queryURL(q *meter.Query, events ...string) (string, error) {
	u, err := url.Parse(qr.URL)
	if err != nil {
		return "", err
	}
	values := QueryValues(q)
	for _, event := range events {
		values.Add("event", event)
	}
	u.RawQuery = values.Encode()
	return u.String(), nil

}

// Eval implements meter.Evaler interface
func (qr *Querier) Eval(ctx context.Context, q meter.Query, exp ...string) (meter.Results, error) {
	u, err := qr.evalURL(&q, exp...)
	if err != nil {
		return nil, err
	}
	return qr.send(ctx, u)
}

func (qr *Querier) send(ctx context.Context, u string) (meter.Results, error) {
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
	if httperr.IsError(res.StatusCode) {
		return nil, httperr.FromResponse(res)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Errorf(`Failed to read response: %s`, err)
	}
	var results meter.Results
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, errors.Errorf(`Failed to parse response: %s`, err)
	}
	return results, nil
}

// Query implements Querier interface
func (qr *Querier) Query(ctx context.Context, q meter.Query, events ...string) (meter.Results, error) {
	u, err := qr.queryURL(&q, events...)
	if err != nil {
		return nil, err
	}
	return qr.send(ctx, u)

}

// QueryValues converts a meter.Query to url.Values
func QueryValues(q *meter.Query) url.Values {
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
	return values
}

// QueryHandler returns an HTTP endpoint for a QueryRunner
func QueryHandler(querier meter.Querier) http.HandlerFunc {
	evaler := meter.QueryEvaler(querier)
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		q, err := ParseQuery(values)
		if err != nil {
			httperr.RespondJSON(w, httperr.BadRequest(err))
			return
		}
		if q.Start.IsZero() {
			q.Start = time.Unix(0, 0)
		}
		if q.End.IsZero() {
			q.End = time.Now()
		}
		if q.Group == nil {
			q.Group = q.Match.Labels()
		}
		switch mode := strings.Trim(r.URL.Path, "/"); mode {
		case "eval":
			eval := values["eval"]
			if len(eval) == 0 {
				err := errors.New("Missing query.eval")
				httperr.RespondJSON(w, httperr.BadRequest(err))
			}
			results, err := evaler.Eval(r.Context(), q, eval...)
			if err != nil {
				httperr.RespondJSON(w, errors.Errorf("Query evaluation failed: %s", err))
				return
			}
			httperr.RespondJSON(w, results)
		case "totals":
			q.Step = -1
			fallthrough
		case "events", "fields", "raw":
			events := values["event"]
			if len(events) == 0 {
				err := errors.New("Missing query.event")
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}
			results, err := querier.Query(r.Context(), q, events...)
			if err != nil {
				err := errors.Errorf("Query failed: %s", err)
				httperr.RespondJSON(w, err)
				return
			}
			var x interface{}
			switch meter.ResultTypeFromString(mode) {
			case meter.TotalsResult:
				x = results.Totals()
			case meter.FieldSummaryResult:
				x = results.FieldSummaries()
			case meter.EventSummaryResult:
				x = results.EventSummaries(q.EmptyValue)
			default:
				x = results
			}
			httperr.RespondJSON(w, x)
		default:
			httperr.RespondJSON(w, httperr.NotFound(nil))
		}
	}
}

// ParseQuery sets query values from a URL query
func ParseQuery(values url.Values) (q meter.Query, err error) {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			q.Step, _ = time.ParseDuration(step[0])
		} else {
			q.Step = 0
		}
	} else {
		q.Step = -1
	}
	start, err := ParseTime(values.Get("start"))
	if err != nil {
		return
	}
	if !start.IsZero() {
		q.Start = start
	}
	end, err := ParseTime(values.Get("end"))
	if err != nil {
		return
	}
	if !end.IsZero() {
		q.End = end
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
