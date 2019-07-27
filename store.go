package meter

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Store interface {
	Storer(event string) Storer
}

// Snapshot is a snaphot of event counters
type Snapshot struct {
	Time     time.Time    `json:"time,omitempty"`
	Labels   []string     `json:"labels"`
	Counters CounterSlice `json:"counters"`
}

// Storer stores events
type Storer interface {
	Store(s *Snapshot) error
}
type StorerFunc func(s *Snapshot) error

func (fn StorerFunc) Store(s *Snapshot) error {
	return fn(s)
}

// MemoryStorer is an in-memory EventStore for debugging
type MemoryStorer struct {
	data []Snapshot
}

type MemoryStore map[string]*MemoryStorer

// Last retuns the last posted StoreRequest
func (m *MemoryStorer) Last() *Snapshot {
	if n := len(m.data) - 1; 0 <= n && n < len(m.data) {
		return &m.data[n]
	}
	return nil

}

// Store implements EventStore interface
func (m *MemoryStorer) Store(req *Snapshot) error {
	last := m.Last()
	if last == nil || req.Time.After(last.Time) {
		m.data = append(m.data, *req)
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
func (m MemoryStore) Storer(event string) Storer {
	if s := m[event]; s != nil {
		return s
	}
	return nil
}

// Scan implements the Scanner interface
func (m MemoryStore) Scan(ctx context.Context, queries ...ScanQuery) (Results, error) {
	var results Results
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
				ok := fields.MatchSorted(&q.Match)
				if ok {
					tm := stepTS(d.Time.Unix(), step)
					results = results.Add(q.Event, fields, tm, float64(c.Count))
				}
			}
		}
	}
	return results, nil
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
	// if len(tee) == 1 {
	// 	return tee[0].Store(s)
	// }
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

type EventStore map[string]Storer

func (s EventStore) Add(event string, stor Storer) error {
	if _, ok := s[event]; ok {
		return errors.New("Duplicate event")
	}
	s[event] = stor
	return nil
}

func (s EventStore) Storer(event string) Storer {
	return s[event]
}
