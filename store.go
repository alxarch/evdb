package meter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type StoreRequest struct {
	Event    string    `json:"event"`
	Time     time.Time `json:"time,omitempty"`
	Labels   []string  `json:"labels"`
	Counters Snapshot  `json:"counters"`
}

type StoreEntry struct {
	Time   time.Time
	Fields *Fields
	Count  int64
}

type EventStore interface {
	Store(req *StoreRequest) error
}

func (event *Event) Store(tm time.Time, db EventStore) error {
	s := getSnapshot()
	defer putSnapshot(s)
	if s = event.Flush(s[:0]); len(s) == 0 {
		return nil
	}
	req := StoreRequest{
		Event:    event.Name,
		Labels:   event.Labels,
		Time:     tm,
		Counters: s,
	}
	if err := db.Store(&req); err != nil {
		event.Merge(s)
		return err
	}
	return nil
}

func StoreHandler(s EventStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := StoreRequest{}
		defer r.Body.Close()
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Time.IsZero() {
			req.Time = time.Now()
		}
		if err := s.Store(&req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"OK"}`))
	}
}

type HTTPStore struct {
	*http.Client
	URL string
}

func (c *HTTPStore) Store(r *StoreRequest) (err error) {
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
		err = fmt.Errorf("Invalid HTTP status: [%d] %s", res.StatusCode, res.Status)
	}
	return
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
