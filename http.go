package meter

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

func Handler(events MultiEventDB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", StoreHandler(events))
	mux.HandleFunc("/query", ScanHandler(events))
	mux.HandleFunc("/summary", SummaryHandler(events))
	mux.HandleFunc("/fields", FieldSummaryHandler(events))
	mux.HandleFunc("/labels", LabelsHandler(events))
	return mux
}

type StoreRequest struct {
	Time     time.Time `json:"time,omitempty"`
	Event    string    `json:"event"`
	Labels   []string  `json:"labels"`
	Counters Snapshot  `json:"counters"`
}

func (r *StoreRequest) SetEvent(event *Event, tm time.Time) {
	for i := range r.Counters {
		r.Counters[i] = Counter{}
	}
	*r = StoreRequest{
		Time:     tm,
		Event:    event.Name,
		Labels:   append(r.Labels[:0], event.Labels...),
		Counters: event.Flush(r.Counters[:0]),
	}
}
func (r *StoreRequest) Reset() {
	for i := range r.Counters {
		r.Counters[i] = Counter{}
	}
	*r = StoreRequest{
		Labels:   r.Labels[:0],
		Counters: r.Counters[:0],
	}
}

func byName(events ...*EventDB) map[string]*EventDB {
	m := make(map[string]*EventDB, len(events))
	for _, event := range events {
		m[event.Event()] = event
	}
	return m
}

func LabelsHandler(db MultiEventDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		events := r.URL.Query()["events"]
		labels, err := db.Labels(events...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		serveJSON(w, labels)
	}
}

func serveJSON(w http.ResponseWriter, x interface{}) error {
	enc := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/json")
	return enc.Encode(x)
}

func StoreHandler(db MultiEventDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var body io.Reader
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer zr.Close()
			body = zr
		case "deflate":
			zr := flate.NewReader(r.Body)
			defer zr.Close()
			body = zr
		default:
			body = r.Body
		}
		req := StoreRequest{}
		dec := json.NewDecoder(body)
		if err := dec.Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Time.IsZero() {
			req.Time = time.Now()
		}
		db, _ := db.Get(req.Event)
		if db == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := db.Store(req.Time, req.Labels, req.Counters); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func FieldSummaryHandler(db MultiEventDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		values := r.URL.Query()
		q := Query{}
		q.SetValues(values)
		sum, err := db.FieldSummary(&q, values["event"]...)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		serveJSON(w, sum)
	}
}

func SummaryHandler(db MultiEventDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := Query{}
		values := r.URL.Query()
		q.SetValues(values)
		q.Step = -1
		sum, err := db.EventSummary(&q, values["event"]...)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		serveJSON(w, sum.Table())
	}
}

func ScanHandler(db MultiEventDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := Query{}
		values := r.URL.Query()
		q.SetValues(values)
		results, err := db.Query(&q, values["event"]...)
		defer results.Close()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		serveJSON(w, results)
	}
}

type HTTPClient struct {
	*http.Client
	URL string
}

type syncError struct {
	Event      string
	StatusCode int
}

func (e *syncError) Error() string {
	return fmt.Sprintf("Sync %q failed: HTTP Status %d", e.Event, e.StatusCode)
}

type syncBuffer struct {
	buffer bytes.Buffer
	gzip   *gzip.Writer
	json   *json.Encoder
}

var syncBuffers sync.Pool

func getSyncBuffer() *syncBuffer {
	if x := syncBuffers.Get(); x != nil {
		return x.(*syncBuffer)
	}
	return new(syncBuffer)
}

func putSyncBuffer(b *syncBuffer) {
	syncBuffers.Put(b)
}

func (b *syncBuffer) Encode(x interface{}) error {
	b.buffer.Reset()
	if b.gzip == nil {
		b.gzip = gzip.NewWriter(&b.buffer)
	} else {
		b.gzip.Reset(&b.buffer)
	}
	if b.json == nil {
		b.json = json.NewEncoder(b.gzip)
	}
	if err := b.json.Encode(x); err != nil {
		return err
	}
	return b.gzip.Close()
}

func (c *HTTPClient) Sync(e *Event, tm time.Time) (err error) {
	s := getSnapshot()
	defer putSnapshot(s)
	if s = e.Flush(s[:0]); len(s) == 0 {
		return
	}
	defer func() {
		if err != nil {
			// Merge back snapshot if sync failed
			e.Merge(s)
		}
	}()
	body := getSyncBuffer()
	defer putSyncBuffer(body)
	store := StoreRequest{
		Time:     tm,
		Event:    e.Name,
		Labels:   e.Labels,
		Counters: s,
	}
	err = body.Encode(&store)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, c.URL, &body.buffer)
	if err != nil {
		return
	}
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		err = &syncError{
			Event:      e.Name,
			StatusCode: res.StatusCode,
		}
	}
	return
}

// DebugHandler returns a debug handler for a db
func DebugHandler(db *badger.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		DumpKeys(db, w)
	})
	return mux

}
