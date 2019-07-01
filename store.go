package meter

import (
	"context"
	"errors"
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
