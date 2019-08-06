package evhttp

import (
	"net/http"

	"github.com/alxarch/evdb"
)

// DefaultMux creates an HTTP endpoint for a evdb.DB
func DefaultMux(r evdb.Scanner, w evdb.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/scan", QueryHandler(r))
	mux.HandleFunc("/query", QueryHandler(r))
	mux.HandleFunc("/", serveIndexHTML)
	mux.HandleFunc("/index.html", serveIndexHTML)
	if w != nil {
		h := StoreHandler(w, "/store/")
		h = InflateRequest(h)
		mux.HandleFunc("/store/", h)
	}
	return mux
}

func serveIndexHTML(w http.ResponseWriter, r *http.Request) {
	const indexHTML = `
<form method="POST" action="query">
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
