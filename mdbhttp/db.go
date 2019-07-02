package mdbhttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/alxarch/go-meter/v2"
)

func sendJSON(w http.ResponseWriter, data interface{}) error {
	enc := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/json")
	return enc.Encode(data)
}

// Handler creates an HTTP endpoint for a meter.DB
func Handler(db meter.DB, events ...string) http.HandlerFunc {
	queryHandler := QueryHandler(db)
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
			case "/":
				queryHandler(w, r)
			case "/events":
				sendJSON(w, map[string]interface{}{
					"events": events,
				})
			}
		case http.MethodPost:
			defer r.Body.Close()
			event := strings.Trim(r.URL.Path, "/")
			storer := storeHandlers[event]
			if storer == nil {
				http.NotFound(w, r)
				return
			}
			storer.ServeHTTP(w, r)
		default:
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
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
