package evhttp

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"

	"github.com/alxarch/evdb"
	"github.com/alxarch/httperr"
)

// Querier runs scan queries over http
type Querier struct {
	URL string
	HTTPClient
}

// Query implements Querier interface
func (s *Querier) Query(ctx context.Context, q *evdb.Query) (evdb.Results, error) {
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

// QueryHandler returns a handler that serves Query HTTP requests
func QueryHandler(scan evdb.Scanner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var queries []evdb.Query
		switch r.Method {
		case http.MethodGet:
			values := r.URL.Query()
			q, err := QueryFromURL(values)
			if err != nil {
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
				values, err := url.ParseQuery(string(data))
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				q, err := QueryFromURL(values)
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				for _, event := range values["event"] {
					q.Event = event
					queries = append(queries, q)
				}
			case "application/json":
				var q evdb.Query
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
