package mdbhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/alxarch/go-meter/v2"
	"github.com/alxarch/httperr"
	errors "golang.org/x/xerrors"
)

type Query struct {
	Query string
	meter.TimeRange
}

// Handler creates an HTTP endpoint for a meter.DB
func Handler(db meter.DB, events ...string) http.HandlerFunc {
	scanner := meter.NewScanner(db)
	querier := meter.NewQuerier(scanner)
	queryHandler := QueryHandler(querier)
	storeHandlers := make(map[string]http.Handler, len(events))
	for _, event := range events {
		storer := db.Storer(event)
		handler := StoreHandler(storer)
		storeHandlers[event] = InflateRequest(handler)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			switch r.URL.Path {
			case "/simplequery":
				queryHandler(w, r)
			case "/", "/index.html":
				w.Header().Set("Content-Type", "text/html")
				w.Write([]byte(indexHTML))
			default:
				httperr.RespondJSON(w, map[string]interface{}{
					"events": events,
				})
			}
		case http.MethodPost:
			mime, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			defer r.Body.Close()
			switch r.URL.Path {
			case "/":
				data, err := ioutil.ReadAll(r.Body)
				var q *Query
				switch mime {
				case "application/json":
					q, err = ParseJSONQuery(data)
				case "application/x-www-form-urlencoded":
					q, err = ParseFormQuery(string(data))
				case "text/plain":
					q, err = ParseFormQuery(r.URL.RawQuery)
					q.Query = string(data)
				}
				if err != nil {
					httperr.RespondJSON(w, httperr.BadRequest(err))
					return
				}
				results, err := querier.Query(r.Context(), q.TimeRange, q.Query)
				if err != nil {
					httperr.RespondJSON(w, httperr.InternalServerError(err))
					return
				}

				data, err = json.Marshal(results)
				if err != nil {
					httperr.RespondJSON(w, httperr.InternalServerError(err))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(data)
			default:
				event := strings.Trim(r.URL.Path, "/")
				storer := storeHandlers[event]
				if storer == nil {
					err := errors.Errorf("Unknown event %q", event)
					httperr.RespondJSON(w, httperr.NotFound(err))
					return
				}
				storer.ServeHTTP(w, r)

			}
		default:
			httperr.RespondJSON(w, httperr.MethodNotAllowed(nil))
		}
	}

}

type db struct {
	Querier
	events map[string]meter.Storer
}

func (db *db) Storer(event string) meter.Storer {
	if s, ok := db.events[event]; ok {
		return s
	}
	return nil
}
func (db *db) Close() error {
	return nil
}
func (db *db) ScanQuery(ctx context.Context, q *meter.ScanQuery) (meter.Results, error) {
	db.queryURL(q.TimeRange)
	return nil, nil
}

// DB connects to a remote meter.DB over HTTP
func DB(baseURL string, client HTTPClient, events ...string) (meter.DB, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "https", "http":
	default:
		return nil, fmt.Errorf("Invalid scheme %q", u.Scheme)
	}
	db := db{
		Querier: Querier{
			URL:        baseURL,
			HTTPClient: client,
		},
		events: make(map[string]meter.Storer, len(events)),
	}
	for _, event := range events {
		storeURL := u
		storeURL.Path = path.Join(u.Path, event)
		storer := Storer{
			URL:        storeURL.String(),
			HTTPClient: db.HTTPClient,
		}
		db.events[event] = &storer
	}
	return &db, nil
}

type opener struct{}

func (opener) Open(baseURL string, events ...string) (meter.DB, error) {
	return DB(baseURL, http.DefaultClient, events...)
}

const indexHTML = `
<form method="POST">
<fieldset>
<label for="start">Start: <input name="start" type="date"/></label>
<label for="end">End: <input name="end" type="date"/></label>
<label for="step">Step: <select name="step">
<option value="1s">1s</option>
<option value="1m">1m</option>
<option value="1h">1h</option>
<option value="24h">1d</option>
<option value="168h">1w</option>
</select></label>
</fieldset>
<textarea name="query"></textarea>
<button>send</button>
</form>
`
