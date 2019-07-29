package evhttp

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/evql"
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

func TimeRangeURL(rawURL string, t *evdb.TimeRange) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	values := u.Query()
	TimeRangeValues(values, *t)
	u.RawQuery = values.Encode()
	return u.String(), nil
}
func ScanURL(rawURL string, q *evdb.ScanQuery) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	values, err := QueryValues(q)
	if err != nil {
		return "", err
	}
	u.RawQuery = values.Encode()
	return u.String(), nil
}

// ScanQuerier runs scan queries over http
type ScanQuerier struct {
	URL string
	HTTPClient
}

func (s *ScanQuerier) ScanQuery(ctx context.Context, q *evdb.ScanQuery) (evdb.Results, error) {
	u, err := ScanURL(s.URL, q)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	var results evdb.Results
	if err := sendJSON(ctx, s.HTTPClient, req, &results); err != nil {
		return nil, err
	}
	return results, nil

}

// Query implements evdb.Evaler interface
func (qr *Querier) Query(ctx context.Context, r evdb.TimeRange, q string) ([]interface{}, error) {
	body := strings.NewReader(q)
	u, err := TimeRangeURL(qr.URL, &r)
	if err != nil {
		return nil, err

	}
	req, err := http.NewRequest(http.MethodPost, u, body)
	if err != nil {
		return nil, err
	}
	var results []interface{}
	if err := sendJSON(ctx, qr.HTTPClient, req, &results); err != nil {
		return nil, err
	}
	return results, nil

}

// TimeRangeValues assigns a time range to a url.Values
func TimeRangeValues(values url.Values, q evdb.TimeRange) {
	values.Set("start", strconv.FormatInt(q.Start.Unix(), 10))
	values.Set("end", strconv.FormatInt(q.End.Unix(), 10))
	values.Set("step", q.Step.String())
}

func matcherFromString(s string) (evdb.Matcher, error) {
	ss := strings.SplitN(s, ":", 2)
	if len(ss) == 2 {
		switch ss[0] {
		case "equals":
			return evdb.MatchString(ss[1]), nil
		case "prefix":
			return evdb.MatchPrefix(ss[1]), nil
		case "suffix":
			return evdb.MatchSuffix(ss[1]), nil
		case "regexp":
			return regexp.Compile(ss[1])
		}
	}
	return nil, errors.Errorf("Invalid matcher string %q", s)
}

func MatchFieldValues(mf evdb.MatchFields) (url.Values, error) {
	q := url.Values(make(map[string][]string, len(mf)))
	for label, m := range mf {
		switch m := m.(type) {
		case *regexp.Regexp:
			q.Set("match.regexp."+label, m.String())
		case evdb.MatchSuffix:
			q.Set("match.suffix."+label, string(m))
		case evdb.MatchPrefix:
			q.Set("match.prefix."+label, string(m))
		case evdb.MatchString:
			q.Set("match."+label, string(m))
		default:
			return nil, errors.Errorf("Cannot convert %q matcher to query", label)
		}
	}
	return q, nil
}

// QueryValues converts a evdb.Query to url.Values
func QueryValues(q *evdb.ScanQuery) (url.Values, error) {
	values, err := MatchFieldValues(q.Fields)
	if err != nil {
		return nil, err
	}
	TimeRangeValues(values, q.TimeRange)
	values.Set("event", q.Event)
	return values, nil
}

// QueryHandler returns an HTTP endpoint for a QueryRunner
func QueryHandler(scanner evdb.Scanner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var q Query
		switch r.Method {
		case http.MethodGet:
			values := r.URL.Query()
			q.Query = values.Get("query")
			if err := ParseQueryTimeRange(&q.TimeRange, values); err != nil {
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}
		case http.MethodPost:
			defer r.Body.Close()
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}

			m, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			switch m {
			case "application/x-www-form-urlencoded":
				values, err := url.ParseQuery(string(data))
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				if err := ParseQueryTimeRange(&q.TimeRange, values); err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				q.Query = values.Get("query")

			case "application/json":
				if err := json.Unmarshal(data, &q); err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
			default:
				err := errors.Errorf("Invalid MIME type: %q", m)
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}
		default:
			httperr.RespondJSON(w, httperr.MethodNotAllowed(nil))
			return
		}
		if q.Step < time.Second {
			err := errors.New("Invalid query.step")
			httperr.RespondJSON(w, httperr.BadRequest(err))
			return
		}
		now := time.Now()
		if q.End.IsZero() || q.End.After(now) {
			q.End = now
		}
		if q.Start.IsZero() || q.Start.After(q.End) {
			q.Start = q.End.Add(-1 * q.Step)
		}

		p := new(evql.Parser)
		if err := p.Reset(q.Query); err != nil {
			httperr.RespondJSON(w, httperr.BadRequest(err))
			return
		}
		queries := p.Queries(q.TimeRange)
		if len(queries) == 0 {
			err := errors.New("Empty query")
			httperr.RespondJSON(w, httperr.BadRequest(err))
			return
		}
		results, err := scanner.Scan(r.Context(), queries...)
		if err != nil {
			httperr.RespondJSON(w, errors.Errorf("Query evaluation failed: %s", err))
			return
		}
		out := p.Eval(nil, q.TimeRange, results)
		httperr.RespondJSON(w, out)
	}
}
func ScanQueryHandler(scan evdb.Scanner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var queries []evdb.ScanQuery
		switch r.Method {
		case http.MethodGet:
			var q evdb.ScanQuery
			values := r.URL.Query()
			if err := ParseQueryTimeRange(&q.TimeRange, values); err != nil {
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}
			for _, event := range values["event"] {
				q.Event = event
				queries = append(queries, q)
			}
		case http.MethodPost:
			defer r.Body.Close()
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}
			m, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			switch m {
			case "application/x-www-form-urlencoded":
				var q evdb.ScanQuery
				values, err := url.ParseQuery(string(data))
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				if err := ParseQueryTimeRange(&q.TimeRange, values); err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				for _, event := range values["event"] {
					q.Event = event
					queries = append(queries, q)
				}
			case "application/json":
				var q evdb.ScanQuery
				if err := json.Unmarshal(data, &q); err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				queries = append(queries, q)
			}
		default:
			httperr.RespondJSON(w, httperr.MethodNotAllowed(nil))
			return
		}

		results, err := scan.Scan(r.Context(), queries...)
		if err != nil {
			httperr.RespondJSON(w, httperr.InternalServerError(err))
		}
		httperr.RespondJSON(w, results)
	}
}

func ParseQueryTimeRange(tr *evdb.TimeRange, values url.Values) error {
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
func ParseQuery(values url.Values) (q evdb.ScanQuery, err error) {
	if err = ParseQueryTimeRange(&q.TimeRange, values); err != nil {
		return
	}
	var m evdb.MatchFields
	for key := range values {
		if !strings.HasPrefix(key, "match.") {
			continue
		}
		label := strings.TrimPrefix(key, "match.")
		var typ string
		if parts := strings.SplitN(label, ".", 2); len(parts) == 2 {
			label, typ = parts[1], parts[0]
		}
		switch strings.ToLower(typ) {
		case "regexp":
			rx, err := regexp.Compile(values.Get(key))
			if err != nil {
				return q, errors.Errorf("Invalid query.%s: %s", key, err)
			}
			m[label] = rx
		case "suffix":
			m[label] = evdb.MatchSuffix(values.Get(key))
		case "prefix":
			m[label] = evdb.MatchPrefix(values.Get(key))
		case "equals":
			m[label] = evdb.MatchString(values.Get(key))
		case "":
			m[label] = evdb.MatchAny(values[key]...)
		default:
			return q, errors.Errorf("Invalid match type %q", typ)
		}
	}
	// group, ok := values["group"]
	// if ok && len(group) == 0 {
	// 	group = make([]string, 0, len(m))
	// 	for label := range m {
	// 		group = append(group, label)
	// 	}
	// 	sort.Strings(group)
	// }
	q.Fields = m
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

type jsonQuery struct {
	Query string `json:"query"`
	Start string `json:"start"`
	End   string `json:"end"`
	Step  string `json:"step"`
}

func (q *Query) MarshalJSON() ([]byte, error) {
	tmp := jsonQuery{
		Start: q.Start.Format(time.RFC3339Nano),
		End:   q.End.Format(time.RFC3339Nano),
		Step:  q.Step.String(),
		Query: q.Query,
	}
	return json.Marshal(&tmp)
}

func (q *Query) UnmarshalJSON(data []byte) error {
	var tmp jsonQuery
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	q.Query = tmp.Query
	start, err := ParseTime(tmp.Start)
	if err != nil {
		return err
	}
	end, err := ParseTime(tmp.End)
	if err != nil {
		return err
	}
	step, err := time.ParseDuration(tmp.Step)
	if err != nil {
		return err
	}
	q.Start, q.End, q.Step = start, end, step
	return nil
}

func ParseFormQuery(data string) (*Query, error) {
	values, err := url.ParseQuery(data)
	if err != nil {
		return nil, errors.Errorf("Invalid querystring: %s", err)
	}
	return parseQuery(values.Get("start"), values.Get("end"), values.Get("step"), values.Get("query"))
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

func sendJSON(ctx context.Context, c HTTPClient, req *http.Request, x interface{}) error {
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	if c == nil {
		c = http.DefaultClient
	}
	res, err := c.Do(req)
	if err != nil {
		return err
	}
	if httperr.IsError(res.StatusCode) {
		return httperr.FromResponse(res)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Errorf(`Failed to read response: %s`, err)
	}
	return json.Unmarshal(data, x)

}

// func matcherFromStrings(values []string) (evdb.Matcher, error) {
// 	var matchers evdb.Matchers
// 	for len(values) > 0 {
// 		v, tail := values[0], values[1:]
// 		m, err := matcherFromString(v)
// 		if err != nil {
// 			return nil, err
// 		}
// 		matchers = append(matchers, m)
// 		values = tail
// 	}
// 	switch len(matchers) {
// 	case 0:
// 		return nil, nil
// 	case 1:
// 		return matchers[0], nil
// 	default:
// 		return matchers, nil
// 	}
// }
