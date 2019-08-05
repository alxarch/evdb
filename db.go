package evdb

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/alxarch/evdb/internal/misc"

	"github.com/alxarch/evdb/events"

	errors "golang.org/x/xerrors"
)

// DB handles scan and store requests
type DB interface {
	Scanner
	Store
	Close() error
}

// Opener opens new DB instances from a URL configuration
type Opener interface {
	Open(configURL string) (DB, error)
}

var (
	openerMu sync.Mutex
	openers  = map[string]Opener{}
)

// Register registers a DB opener for a URL scheme
func Register(scheme string, op Opener) error {
	openerMu.Lock()
	defer openerMu.Unlock()
	_, alreadyRegistered := openers[scheme]
	if alreadyRegistered {
		return errors.Errorf(`Scheme %q already registered`, scheme)
	}
	openers[scheme] = op
	return nil
}

// Open opens a DB using a configURL and applies options
func Open(configURL string, options ...Option) (DB, error) {
	u, err := url.Parse(configURL)
	if err != nil {
		return nil, err
	}
	scheme := u.Scheme
	openerMu.Lock()
	opener := openers[scheme]
	openerMu.Unlock()
	if opener == nil {
		return nil, fmt.Errorf("Scheme %q not registered", scheme)
	}
	db, err := opener.Open(configURL)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		db, err = option.apply(db)
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

// Option is a DB option
type Option interface {
	apply(db DB) (DB, error)
}

type matchDB struct {
	match Matcher
	DB
}

func newMatchDB(db DB, m Matcher) (DB, error) {
	if db, ok := db.(*matchDB); ok {
		db.match = mergeMatchers(db.match, m)
		return db, nil
	}
	return &matchDB{
		match: m,
		DB:    db,
	}, nil
}

func (m *matchDB) Scan(ctx context.Context, queries ...ScanQuery) (Results, error) {
	cp := make([]ScanQuery, 0, len(queries))
	for _, q := range queries {
		if m.match.MatchString(q.Event) {
			cp = append(cp, q)
		}
	}
	return m.DB.Scan(ctx, cp...)
}

func (m *matchDB) Storer(event string) (Storer, error) {
	if !m.match.MatchString(event) {
		return nil, errors.Errorf("Event %q does not match %s", event, m.match)
	}
	return m.DB.Storer(event)
}

func (m *matchDB) apply(db DB) (DB, error) {
	return newMatchDB(db, m.match)
}

// MatchEvents filters queries and stores for a db
func MatchEvents(m Matcher) Option {
	return &matchDB{match: m}
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
	e = events.NewEvent(b.name, s.Labels...)
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

func (b *batchDB) flush(tm time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for name, e := range b.events {
		store, err := b.db.Storer(name)
		if err != nil {
			b.logger.Println(errors.Errorf("Failed to store %q: %s", name, err))
			continue
		}
		b.wg.Add(1)
		go func(e *batchEvent) {
			defer b.wg.Done()
			for _, event := range e.events {
				task := SyncTask(event, store)
				if err := task(tm); err != nil {
					b.logger.Println(errors.Errorf("Failed to store event %s%s: %s", event.Name, event.Labels, err))
				}
			}
		}(e)
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

// BatchInterval flushes event snapshots once on each event tick
func BatchInterval(interval time.Duration, logger *log.Logger) Option {
	return fnOption(func(db DB) (DB, error) {
		return newBatchDB(db, interval, logger)
	})
}

type fnOption func(db DB) (DB, error)

func (fn fnOption) apply(db DB) (DB, error) {
	return fn(db)
}
