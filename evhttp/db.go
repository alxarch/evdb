package evhttp

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/alxarch/evdb"
)

type Query struct {
	Query string
	evdb.TimeRange
}

// Mux creates an HTTP endpoint for a evdb.DB
func Mux(db evdb.DB, events ...string) http.Handler {
	mux := http.NewServeMux()
	for _, event := range events {
		storer := db.Storer(event)
		handler := StoreHandler(storer)
		handler = InflateRequest(handler)
		mux.HandleFunc("/store/"+event, handler)
	}
	mux.HandleFunc("/scan", ScanQueryHandler(db))
	mux.HandleFunc("/query", QueryHandler(db))
	mux.HandleFunc("/", serveIndexHTML)
	mux.HandleFunc("/index.html", serveIndexHTML)
	return mux
}

type db struct {
	evdb.Scanner
	Querier
	events map[string]evdb.Storer
}

var _ (evdb.DB) = (*db)(nil)

func (db *db) Storer(event string) evdb.Storer {
	if s, ok := db.events[event]; ok {
		return s
	}
	return nil
}
func (db *db) Close() error {
	return nil
}
func (db *db) ScanQuery(ctx context.Context, q *evdb.ScanQuery) (evdb.Results, error) {
	return nil, nil
}

// DB connects to a remote evdb.DB over HTTP
func DB(baseURL string, client HTTPClient, events ...string) (evdb.DB, error) {
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
		events: make(map[string]evdb.Storer, len(events)),
	}
	scanURL := u
	scanURL.Path = path.Join(u.Path, "/scan")
	scan := ScanQuerier{scanURL.String(), client}
	db.Scanner = evdb.NewScanner(&scan)
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

func (opener) Open(baseURL string, events ...string) (evdb.DB, error) {
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
<button>send</button>
</fieldset>
<textarea style="width: 100%" rows="30" name="query"></textarea>
</form>
`

func serveIndexHTML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(indexHTML))

}
