package evdb

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/internal/misc"
	errors "golang.org/x/xerrors"
)

// BatchInterval flushes event snapshots once on each event tick
func BatchInterval(interval time.Duration, logger *log.Logger) Option {
	return fnOption(func(db DB) (DB, error) {
		return newBatchDB(db, interval, logger)
	})
}

type batchDB struct {
	db     DB
	mu     sync.RWMutex
	events map[string]*batchEvent
	logger *log.Logger
	once   sync.Once
	done   chan struct{}
	wg     sync.WaitGroup
	tick   *time.Ticker
}

type batchEvent struct {
	name   string
	mu     sync.RWMutex
	events []*events.Event
}

func (b *batchEvent) find(labels []string) *events.Event {
	for _, e := range b.events {
		if misc.StringsEqual(labels, e.Labels) {
			return e
		}
	}
	return nil
}

func (b *batchEvent) Store(s *Snapshot) error {
	b.mu.RLock()
	e := b.find(s.Labels)
	b.mu.RUnlock()
	if e != nil {
		e.Merge(s.Counters)
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e = b.find(s.Labels)
	if e != nil {
		e.Merge(s.Counters)
		return nil
	}
	e = events.New(b.name, s.Labels...)
	e.Merge(s.Counters)
	b.events = append(b.events, e)
	return nil
}

func newBatchDB(db DB, interval time.Duration, logger *log.Logger) (*batchDB, error) {
	tick := time.NewTicker(interval)
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	batch := batchDB{
		db:     db,
		logger: logger,
		tick:   tick,
		events: make(map[string]*batchEvent),
		done:   make(chan struct{}),
	}
	go batch.run()
	return &batch, nil
}

func (b *batchDB) Storer(event string) (Storer, error) {
	b.mu.RLock()
	e := b.events[event]
	b.mu.RUnlock()
	if e != nil {
		return e, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.events[event]; e != nil {
		return e, nil
	}
	e = new(batchEvent)
	e.name = event
	b.events[event] = e
	return e, nil
}
func (b *batchDB) Scan(ctx context.Context, queries ...ScanQuery) (Results, error) {
	return b.db.Scan(ctx, queries...)
}
func (b *batchDB) Close() error {
	b.once.Do(func() {
		defer close(b.done)
		b.tick.Stop()
	})
	b.wg.Wait()
	return b.db.Close()
}

func (b *batchDB) flushEvent(e *batchEvent, store Storer, tm time.Time) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, event := range e.events {
		b.wg.Add(1)
		go func(e *events.Event) {
			defer b.wg.Done()
			if err := FlushAt(e, store, tm); err != nil {
				b.logger.Println(errors.Errorf("Failed to store event %s%s: %s", e.Name, e.Labels, err))
			}
		}(event)
	}
}

func (b *batchDB) flush(tm time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for name, e := range b.events {
		store, err := b.db.Storer(name)
		if err != nil {
			b.logger.Println(errors.Errorf("Failed to store %q: %s", name, err))
			continue
		}
		b.flushEvent(e, store, tm)
	}
}

func (b *batchDB) run() {
	for {
		select {
		case <-b.done:
			b.flush(time.Now())
			return
		case tm := <-b.tick.C:
			b.flush(tm)
		}
	}
}

type fnOption func(db DB) (DB, error)

func (fn fnOption) apply(db DB) (DB, error) {
	return fn(db)
}
