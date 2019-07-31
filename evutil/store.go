package evutil

import (
	"context"
	"sync"
	"time"

	db "github.com/alxarch/evdb"
	errors "golang.org/x/xerrors"
)

// StorerFunc is a function that implements Storer interface
type StorerFunc func(s *db.Snapshot) error

// Store implements storer interface
func (fn StorerFunc) Store(s *db.Snapshot) error {
	return fn(s)
}

// MemoryStorer is an in-memory EventStore for debugging
type MemoryStorer struct {
	data []db.Snapshot
}

// MemoryStore is a Store collecting snapshots in memory
type MemoryStore map[string]*MemoryStorer

// Last retuns the last posted StoreRequest
func (m *MemoryStorer) Last() *db.Snapshot {
	if n := len(m.data) - 1; 0 <= n && n < len(m.data) {
		return &m.data[n]
	}
	return nil

}

// Len retuns the number of stored snapshots
func (m *MemoryStorer) Len() int {
	return len(m.data)
}

// Store implements EventStore interface
func (m *MemoryStorer) Store(s *db.Snapshot) error {
	last := m.Last()
	if last == nil || s.Time.After(last.Time) {
		s = s.Copy()
		m.data = append(m.data, *s)
		return nil
	}
	return errors.New("Invalid time")
}

// NewMemoryStore creates a new MemoryStore
func NewMemoryStore(events ...string) MemoryStore {
	store := make(map[string]*MemoryStorer, len(events))
	for _, e := range events {
		store[e] = new(MemoryStorer)
	}
	return store
}

// Storer implements Store interface
func (m MemoryStore) Storer(event string) db.Storer {
	if s := m[event]; s != nil {
		return s
	}
	return nil
}

// Scan implements the Scanner interface
func (m MemoryStore) Scan(ctx context.Context, queries ...db.ScanQuery) (db.Results, error) {
	var results db.Results
	for _, q := range queries {
		store, ok := m[q.Event]
		if !ok {
			continue
		}
		step := int64(q.Step / time.Second)
		if step < 1 {
			step = 1
		}
		for i := range store.data {
			d := &store.data[i]
			if d.Time.Before(q.Start) {
				continue
			}
			for j := range d.Counters {
				c := &d.Counters[j]
				fields := ZipFields(d.Labels, c.Values)
				ok := q.Fields.Match(fields)
				if ok {
					tm := stepTS(d.Time.Unix(), step)
					results = results.Add(q.Event, fields, tm, float64(c.Count))
				}
			}
		}
	}
	return results, nil
}

func stepTS(ts, step int64) int64 {
	if step > 0 {
		return ts - ts%step
	}
	if step == 0 {
		return ts
	}
	return 0
}

// TeeStore stores to multiple stores
func TeeStore(stores ...db.Storer) db.Storer {
	return teeStorer(stores)
}

type teeStorer []db.Storer

func (tee teeStorer) Add(s db.Storer) teeStorer {
	if s, ok := s.(teeStorer); ok {
		return append(tee, s...)
	}
	return append(tee, s)
}

func (tee teeStorer) Store(s *db.Snapshot) error {
	if len(tee) == 0 {
		return nil
	}
	if len(tee) == 1 {
		return tee[0].Store(s)
	}
	errc := make(chan error, len(tee))
	wg := new(sync.WaitGroup)
	for i := range tee {
		db := tee[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
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

// MuxStore maps event names to Storers
type MuxStore map[string]db.Storer

// Storer implements Store interface
func (m MuxStore) Storer(event string) db.Storer {
	return m[event]
}

// Set sets a Storer for an event
func (m MuxStore) Set(event string, s db.Storer) MuxStore {
	if m == nil {
		m = make(map[string]db.Storer)
	}
	m[event] = s
	return m
}

// Add set an additional Storer for an event
func (m MuxStore) Add(event string, s db.Storer) (MuxStore, db.Storer) {
	if m == nil {
		m = make(map[string]db.Storer)
		m[event] = s
		return m, s
	}
	p := m[event]
	if p == nil {
		m[event] = s
		return m, s
	}
	if p, ok := p.(teeStorer); ok {
		p = p.Add(s)
		m[event] = p
		return m, p
	}
	var tee teeStorer
	tee = tee.Add(p)
	tee = tee.Add(s)
	m[event] = tee
	return m, tee

}

// SyncStore provides a mutable Store safe for concurrent use
type SyncStore struct {
	mu  sync.RWMutex
	mux MuxStore
}

// Storer implements Store interface
func (s *SyncStore) Storer(event string) (w db.Storer) {
	s.mu.RLock()
	w = s.mux.Storer(event)
	s.mu.RUnlock()
	return
}

// Register sets the Storer if none exists
func (s *SyncStore) Register(event string, w db.Storer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, duplicate := s.mux[event]; duplicate {
		return false
	}
	s.mux = s.mux.Set(event, w)
	return true
}

// Set sets the Storer for an event
func (s *SyncStore) Set(event string, w db.Storer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mux = s.mux.Set(event, w)
}

// Add sets an additional Storer for an event
func (s *SyncStore) Add(event string, w db.Storer) db.Storer {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mux, w = s.mux.Add(event, w)
	return w
}

// AutoStore is a Store that creates a new Storer if it is not Registered
type AutoStore struct {
	New   func(event string) db.Storer
	store SyncStore
}

// Storer implements Store interface
func (a *AutoStore) Storer(event string) (w db.Storer) {
	if w := a.store.Storer(event); w != nil {
		return w
	}
	if w := a.New(event); w != nil && a.store.Register(event, w) {
		return w
	}
	return a.store.Storer(event)
}
