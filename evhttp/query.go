package evhttp

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/evql"
	"github.com/alxarch/evdb/evutil"
	"github.com/alxarch/httperr"
	errors "golang.org/x/xerrors"
)

// Execer runs queries over http
type Execer struct {
	URL string
	HTTPClient
}

var _ evql.Execer = (*Execer)(nil)

// Exec implements evql.Execet interface over HTTP
func (ex *Execer) Exec(ctx context.Context, r evdb.TimeRange, q string) ([]evdb.Results, error) {
	body := strings.NewReader(q)
	u, err := TimeRangeURL(ex.URL, &r)
	if err != nil {
		return nil, err

	}
	req, err := http.NewRequest(http.MethodPost, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/evql")
	var results []evdb.Results
	if err := sendJSON(ctx, ex.HTTPClient, req, &results); err != nil {
		return nil, err
	}
	return results, nil

}

// ExecHandler returns an HTTP endpoint that executes evql queries
func ExecHandler(scanner evdb.Scanner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var q query
		switch r.Method {
		case http.MethodGet:
			values := r.URL.Query()
			q.Query = values.Get("query")
			q.Format = values.Get("format")
			t, err := TimeRangeFromURL(values)
			if err != nil {
				httperr.RespondJSON(w, httperr.BadRequest(err))
				return
			}
			q.TimeRange = t
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
				t, err := TimeRangeFromURL(values)
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				q.TimeRange = t
				q.Query = values.Get("query")
				q.Format = values.Get("format")
			case "application/evql":
				q.Query = string(data)
				values := r.URL.Query()
				q.Format = values.Get("format")
				t, err := TimeRangeFromURL(values)
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				q.TimeRange = t
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

		e, err := evql.Parse(q.Query)
		if err != nil {
			httperr.RespondJSON(w, httperr.BadRequest(err))
			return
		}
		queries := e.Queries(q.TimeRange)
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
		rows := e.Eval(nil, q.TimeRange, results)
		if out, ok := evutil.FormatResults(q.Format, rows...); ok {
			httperr.RespondJSON(w, out)
			return
		}
		err = errors.Errorf("Invalid query format: %q", q.Format)
		httperr.RespondJSON(w, httperr.BadRequest(err))
	}
}

type query struct {
	Query string
	evdb.TimeRange
	Format string
}

type jsonQuery struct {
	Query  string `json:"query"`
	Format string `json:"format,omitempty"`
	Start  string `json:"start"`
	End    string `json:"end"`
	Step   string `json:"step"`
}

func (q *query) MarshalJSON() ([]byte, error) {
	tmp := jsonQuery{
		Start: q.Start.Format(time.RFC3339Nano),
		End:   q.End.Format(time.RFC3339Nano),
		Step:  q.Step.String(),
		Query: q.Query,
	}
	return json.Marshal(&tmp)
}

func (q *query) UnmarshalJSON(data []byte) error {
	var tmp jsonQuery
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	q.Query = tmp.Query
	q.Format = tmp.Format
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
