package evdb

import (
	"sync"
	"time"

	"github.com/alxarch/evdb/events"
)

// Store provides snapshot storers for events
type Store interface {
	Storer(event string) (Storer, error)
}

// Storer stores event snapshots
type Storer interface {
	Store(s *Snapshot) error
}

// Snapshot is a snaphot of event counters
type Snapshot struct {
	Time     time.Time        `json:"time,omitempty"`
	Labels   []string         `json:"labels"`
	Counters []events.Counter `json:"counters"`
}

// Reset resets a snapshot
func (s *Snapshot) Reset() {
	*s = Snapshot{
		Counters: events.Counters(s.Counters).Reset(),
	}
}

// Copy copies a snapshot
func (s *Snapshot) Copy() *Snapshot {
	if s == nil {
		return nil
	}
	cp := Snapshot{
		Time:     s.Time,
		Labels:   append(make([]string, 0, len(s.Labels)), s.Labels...),
		Counters: append(make([]events.Counter, 0, len(s.Counters)), s.Counters...),
	}
	return &cp
}

func stringsEqual(a, b []string) bool {
	if len(a) == len(b) {
		b = b[:len(a)]
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	return false
}

// Flush flushes an event a Storer
func Flush(e *events.Event, db Storer) error {
	return FlushAt(e, db, time.Now())
}

// FlushAt flushes an event a Storer
func FlushAt(e *events.Event, db Storer, tm time.Time) error {
	s := getSnapshot()
	defer putSnapshot(s)
	s.Counters = e.Flush(s.Counters[:0])
	if len(s.Counters) == 0 {
		return nil
	}
	s.Labels, s.Time = e.Labels, tm
	if err := db.Store(s); err != nil {
		e.Merge(s.Counters)
		return err
	}
	return nil
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
