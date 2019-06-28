package meter

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Snapshot is a snaphot of event counters
type Snapshot struct {
	Event    string       `json:"event"`
	Time     time.Time    `json:"time,omitempty"`
	Labels   []string     `json:"labels"`
	Counters CounterSlice `json:"counters"`
}

// Storer stores events
type Storer interface {
	Store(s *Snapshot) error
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

// StoreHandler returns an HTTP endpoint for an EventStore
func StoreHandler(s Storer) http.HandlerFunc {
	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		req := Snapshot{}
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			code := http.StatusBadRequest
			http.Error(w, http.StatusText(code), code)
			return
		}
		if req.Time.IsZero() {
			req.Time = time.Now()
		}
		if err := s.Store(&req); err != nil {
			code := http.StatusInternalServerError
			http.Error(w, http.StatusText(code), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"OK"}`))
	}
	return InflateRequest(http.HandlerFunc(handler))
}

// HTTPStore is a remote EventStore over HTTP
type HTTPStore struct {
	*http.Client
	URL string
}

// Store implements EventStore interface
func (c *HTTPStore) Store(r *Snapshot) (err error) {
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

// MemoryStore is an in-memory EventStore for debugging
type MemoryStore struct {
	data  []Snapshot
	Event string
}

// Last retuns the last posted StoreRequest
func (m *MemoryStore) Last() *Snapshot {
	if n := len(m.data) - 1; 0 <= n && n < len(m.data) {
		return &m.data[n]
	}
	return nil

}

// Store implements EventStore interface
func (m *MemoryStore) Store(req *Snapshot) error {
	if req.Event != m.Event {
		return errors.New("Invalid event")
	}
	last := m.Last()
	if last == nil || req.Time.After(last.Time) {
		m.data = append(m.data, *req)
		return nil
	}
	return errors.New("Invalid time")
}

// Scanner implements the EventScanner interface
func (m *MemoryStore) Scanner(event string) Scanner {
	if event == m.Event {
		return m
	}
	return nil
}

// Scan implements the Scanner interface
func (m *MemoryStore) Scan(ctx context.Context, q *Query) ScanIterator {
	errc := make(chan error)
	items := make(chan ScanItem)
	data := m.data
	ctx, cancel := context.WithCancel(ctx)
	it := scanIterator{
		errc:   errc,
		items:  items,
		cancel: cancel,
	}
	done := ctx.Done()
	match := q.Match.Sorted()
	groups := q.Group
	if len(groups) > 0 {
		sort.Strings(groups)
	}
	step := int64(q.Step / time.Second)
	if step < 1 {
		step = 1
	}
	go func() {
		defer close(items)
		defer close(errc)
		for i := range data {
			d := &data[i]
			if d.Time.Before(q.Start) {
				continue
			}
			for j := range d.Counters {
				c := &d.Counters[j]
				fields := ZipFields(d.Labels, c.Values)
				ok := fields.MatchSorted(match)
				if ok {
					if len(groups) > 0 {
						fields = fields.GroupBy(q.EmptyValue, groups)
					}
					select {
					case items <- ScanItem{
						Fields: fields,
						Time:   stepTS(d.Time.Unix(), step),
						Count:  c.Count,
					}:
					case <-done:
						return
					}
				}
			}
		}
	}()
	return &it
}

// SyncTask dumps an Event to an EventStore
func (e *Event) SyncTask(db Storer) func(time.Time) error {
	return func(tm time.Time) error {
		s := getCounterSlice()
		defer putCounterSlice(s)
		if s = e.Flush(s[:0]); len(s) == 0 {
			return nil
		}
		req := Snapshot{
			Event:    e.Name,
			Labels:   e.Labels,
			Time:     tm,
			Counters: s,
		}
		if err := db.Store(&req); err != nil {
			e.Merge(s)
			return err
		}
		return nil

	}
}

// TeeStore stores to multiple stores
func TeeStore(stores ...Storer) Storer {
	return teeStorer(stores)
}

type teeStorer []Storer

func (tee teeStorer) Store(s *Snapshot) error {
	if len(tee) == 0 {
		return nil
	}
	errc := make(chan error, len(tee))
	wg := new(sync.WaitGroup)
	wg.Add(len(tee))
	for i := range tee {
		db := tee[i]
		go func() {
			errc <- db.Store(s)
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

// EventStore multiplexes stores for multiple events
type EventStore map[string]Storer

// Add adds a store for events
func (m EventStore) Add(db Storer, events ...string) {
	for _, event := range events {
		m[event] = db
	}
}

// Store implements storer interface
func (m EventStore) Store(s *Snapshot) error {
	db := m[s.Event]
	if db == nil {
		return errors.New("Unknown event")
	}
	return db.Store(s)
}
