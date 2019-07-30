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

func NewMemoryStore(events ...string) MemoryStore {
	store := make(map[string]*MemoryStorer, len(events))
	for _, e := range events {
		store[e] = new(MemoryStorer)
	}
	return store
}
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

type MuxStore map[string]db.Storer

func (m MuxStore) Add(s db.Storer, events ...string) MuxStore {
	if m == nil {
		m = make(map[string]db.Storer)
	}
	for _, event := range events {
		m[event] = s
	}
	return m

}
func (m MuxStore) Storer(event string) db.Storer {
	return m[event]
}

type MuxScanner map[string]db.ScanQuerier

func (m MuxScanner) Add(s db.ScanQuerier, events ...string) MuxScanner {
	if m == nil {
		m = make(map[string]db.ScanQuerier)
	}
	for _, event := range events {
		m[event] = s
	}
	return m

}

func (m MuxScanner) Scan(ctx context.Context, queries ...db.ScanQuery) (db.Results, error) {
	queries = db.ScanQueries(queries).Compact()
	wg := new(sync.WaitGroup)
	errc := make(chan error, len(queries))
	var out db.Results
	var mu sync.Mutex
	for i := range queries {
		q := &queries[i]
		s := m[q.Event]
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Merge all overlapping queries
			results, err := s.ScanQuery(ctx, q)
			if err == nil {
				mu.Lock()
				out = append(out, results...)
				mu.Unlock()
			}
			errc <- err
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
