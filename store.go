package evdb

import (
	"context"
	"sync"
	"time"

	"github.com/alxarch/evdb/events"
	errors "golang.org/x/xerrors"
)

// Store provides snapshot storers for events
type Store interface {
	Storer(event string) Storer
}

// Snapshot is a snaphot of event counters
type Snapshot struct {
	Time     time.Time        `json:"time,omitempty"`
	Labels   []string         `json:"labels"`
	Counters []events.Counter `json:"counters"`
}

func (s *Snapshot) Reset() {
	*s = Snapshot{
		Counters: events.CounterSlice(s.Counters).Reset(),
	}
}

// Storer stores event snapshots
type Storer interface {
	Store(s *Snapshot) error
}

// StorerFunc is a function that implements Storer interface
type StorerFunc func(s *Snapshot) error

// Store implements storer interface
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

// SyncTask dumps an Event to an EventStore
func SyncTask(e *events.Event, db Storer) func(time.Time) error {
	return func(tm time.Time) error {
		s := getSnapshot()
		defer putSnapshot(s)
		if s.Counters = e.Flush(s.Counters[:0]); len(s.Counters) == 0 {
			return nil
		}
		s.Labels, s.Time = e.Labels, tm
		if err := db.Store(s); err != nil {
			e.Merge(s.Counters)
			return err
		}
		return nil

	}
}

var snapshotPool sync.Pool

func getSnapshot() *Snapshot {
	if x := snapshotPool.Get(); x != nil {
		return x.(*Snapshot)
	}
	return new(Snapshot)
}

func putSnapshot(s *Snapshot) {
	if s != nil {
		s.Reset()
		snapshotPool.Put(s)
	}
}
