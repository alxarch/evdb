package evhttp

import (
	"net/http"

	"github.com/alxarch/evdb"
)

// DefaultMux creates an HTTP endpoint for a evdb.DB
func DefaultMux(db evdb.DB, events ...string) http.Handler {
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

func serveIndexHTML(w http.ResponseWriter, r *http.Request) {
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
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(indexHTML))

}
