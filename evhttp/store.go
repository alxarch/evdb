package evhttp

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	meter "github.com/alxarch/evdb"
	"github.com/alxarch/httperr"
)

// Storer is a remote Storer over HTTP
type Storer struct {
	HTTPClient
	URL string
}

// Store implements Storer interface
func (c *Storer) Store(r *meter.Snapshot) (err error) {
	body := getSyncBuffer()
	defer putSyncBuffer(body)
	err = body.Encode(r)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, c.URL, &body.buffer)
	if err != nil {
		return
	}
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")

	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	if httperr.IsError(res.StatusCode) {
		return httperr.FromResponse(res)
	}
	defer res.Body.Close()
	return
}

// Store is implements Store over HTTP
type Store struct {
	HTTPClient
	BaseURL string
}

// Storer implements Store interface
func (s *Store) Storer(event string) meter.Storer {
	u, err := url.Parse(s.BaseURL)
	if err != nil {
		return nil
	}
	u.Path = path.Join(u.Path, event)
	return &Storer{
		HTTPClient: s.HTTPClient,
		URL:        u.String(),
	}
}

// StoreHandler returns an HTTP handler for a Store
func StoreHandler(store meter.Store, prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		path = strings.TrimPrefix(path, prefix)
		event := strings.Trim(path, "/")
		s := store.Storer(event)
		if s == nil {
			httperr.RespondJSON(w, httperr.NotFound(nil))
			return
		}
		h := storeHandler{s}
		h.ServeHTTP(w, r)
	}
}

type storeHandler struct {
	meter.Storer
}

func (h *storeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	s := meter.Snapshot{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&s); err != nil {
		httperr.RespondJSON(w, httperr.BadRequest(err))
		return
	}
	if s.Time.IsZero() {
		s.Time = time.Now()
	}
	if err := h.Store(&s); err != nil {
		httperr.RespondJSON(w, err)
		return
	}
	httperr.RespondJSON(w, json.RawMessage(`{"statusCode":200,"message":"OK"}`))
}

// NewStoreHandler returns an HTTP endpoint for a Storer
func NewStoreHandler(s meter.Storer) http.Handler {
	return &storeHandler{s}
}

// InflateRequest middleware inflates request body
func InflateRequest(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		defer body.Close()
		switch r.Header.Get("Content-Encoding") {
		case "gzip":
			// err is returned on first read
			zr, _ := gzip.NewReader(body)
			r.Header.Del("Content-Encoding")
			r.Body = zr
		case "deflate":
			zr := flate.NewReader(body)
			r.Header.Del("Content-Encoding")
			r.Body = zr
		}
		next.ServeHTTP(w, r)
	}
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
