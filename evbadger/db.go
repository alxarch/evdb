package evbadger

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/alxarch/evdb/internal/misc"

	errors "golang.org/x/xerrors"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/blob"
	"github.com/dgraph-io/badger/v2"
)

// DB is a collection of Events stored in BadgerDB
type DB struct {
	evdb.Scanner
	badger *badger.DB
	mu     sync.RWMutex
	events map[string]*eventDB
}

var _ evdb.DB = (*DB)(nil)

// Open opens a new Event collection stored in BadgerDB
func Open(b *badger.DB) (*DB, error) {
	eventIDs, err := loadEventIDs(b)
	if err != nil {
		return nil, err
	}
	db := DB{
		badger: b,
		events: make(map[string]*eventDB, len(eventIDs)),
	}
	db.Scanner = evdb.NewScanner(&db)

	for event, id := range eventIDs {
		db.events[event] = &eventDB{
			badger: b,
			id:     id,
		}
	}

	return &db, nil
}

// Storer implements Store interface
func (db *DB) Storer(event string) (evdb.Storer, error) {
	db.mu.RLock()
	w := db.events[event]
	db.mu.RUnlock()
	if w != nil {
		return w, nil
	}

	ids, err := loadEventIDs(db.badger, event)
	if err != nil {
		return nil, err
	}
	id, ok := ids[event]
	if !ok {
		return nil, errors.Errorf("Failed to register event id")
	}
	e := eventDB{
		badger: db.badger,
		id:     id,
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if w = db.events[event]; w != nil {
		return w, nil
	}
	if db.events == nil {
		db.events = make(map[string](*eventDB))
	}
	db.events[event] = &e
	return &e, nil
}

// Query implements evdb.Querier interface
func (db *DB) Query(ctx context.Context, q *evdb.Query) (evdb.Results, error) {
	if s, ok := db.events[q.Event]; ok {
		return s.Query(ctx, q)
	}
	return nil, errors.Errorf("Invalid event %q", q.Event)
}

// Close implements evdb.DB interface
func (db *DB) Close() error {
	return db.badger.Close()
}

const (
	keySize         = 16
	keyVersion      = 0
	prefixByteValue = 1
	prefixByteEvent = 2
)

type keyBuffer [keySize]byte

type eventID uint32

func eventKeyAt(event eventID, tm time.Time) (k keyBuffer) {
	return eventKey(event, tm.Unix())
}

func eventKey(event eventID, ts int64) (k keyBuffer) {
	k[0] = keyVersion
	k[1] = prefixByteEvent
	binary.BigEndian.PutUint32(k[2:], uint32(event))
	binary.BigEndian.PutUint64(k[8:], uint64(ts))
	return k
}

func valueKey(event eventID, id uint64) (k keyBuffer) {
	k[0] = keyVersion
	k[1] = prefixByteValue
	binary.BigEndian.PutUint32(k[2:], uint32(event))
	binary.BigEndian.PutUint64(k[8:], id)
	return k
}

func parseEventKey(e eventID, k []byte) (int64, bool) {
	p, event, id := parseKey(k)
	return int64(id), p == prefixByteEvent && e == event
}

func parseValueKey(e eventID, k []byte) (uint64, bool) {
	p, event, id := parseKey(k)
	return id, p == prefixByteValue && e == event
}

func parseKey(k []byte) (byte, eventID, uint64) {
	if len(k) == keySize && k[0] == keyVersion {
		return k[1], eventID(binary.BigEndian.Uint32(k[2:])), binary.BigEndian.Uint64(k[8:])
	}
	return 0, 0, 0
}

func seekEvent(iter *badger.Iterator, event eventID, tm time.Time) {
	key := eventKey(event, tm.Unix())
	iter.Seek(key[:])
}

func seekValue(iter *badger.Iterator, event eventID, id uint64) {
	key := valueKey(event, id)
	iter.Seek(key[:])
}

// DumpKeys dumps keys from a badger.DB to a writer
func (db *DB) DumpKeys(w io.Writer) error {
	return db.badger.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		var k keyBuffer
		var fields evdb.Fields
		for iter.Seek(k[:]); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			switch typ, event, id := parseKey(key); typ {
			case prefixByteValue:
				if err := item.Value(fields.UnmarshalBinary); err != nil {
					return err
				}
				fmt.Fprintf(w, "v event %d field %d value %v\n", event, id, fields)
			case prefixByteEvent:
				item.Value(func(v []byte) error {
					fmt.Fprintf(w, "e event %d field %d size %d\n", event, id, len(v)/16)
					return nil
				})
			default:
				fmt.Fprintf(w, "? %x\n", key)
			}
		}
		return nil
	})
}

type errMissingEvent string

func (event errMissingEvent) Error() string {
	return fmt.Sprintf("Missing event %q", string(event))
}

func loadEvents(txn *badger.Txn) ([]string, error) {
	var key keyBuffer
	// Zero key holds registered events
	itm, err := txn.Get(key[:])
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var dbEvents []string
	if err := itm.Value(func(v []byte) error {
		dbEvents, _ = blob.ReadStrings(v)
		return nil
	}); err != nil {
		return nil, err
	}
	return dbEvents, nil
}

func loadEventIDs(db *badger.DB, events ...string) (map[string]eventID, error) {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	dbEvents, err := loadEvents(txn)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		// Just return events in store
		ids := make(map[string]eventID, len(dbEvents))
		for id, name := range dbEvents {
			ids[name] = eventID(id + 1)
		}
		return ids, nil
	}
	n := len(dbEvents)
	ids := make(map[string]eventID, len(events))
	for _, event := range events {
		id := misc.IndexOf(dbEvents, event) + 1
		if id == 0 {
			// Event is not registered in DB
			dbEvents = append(dbEvents, event)
			id = len(dbEvents)
		}
		ids[event] = eventID(id)
	}
	if len(dbEvents) != n {
		// New events have been added

		// Serialize registered event names
		v := blob.WriteStrings(nil, dbEvents)

		// Store event names at zero key
		var key keyBuffer
		if err := txn.Set(key[:], v); err != nil {
			return nil, err
		}

		if err := txn.Commit(); err != nil {
			return nil, err
		}
	}
	return ids, nil

}

type iLabel struct {
	Label string
	Index int
}

type labelIndex []iLabel

func (index labelIndex) Len() int {
	return len(index)
}

func (index labelIndex) Less(i, j int) bool {
	return index[i].Label < index[j].Label
}

func (index labelIndex) Swap(i, j int) {
	index[i], index[j] = index[j], index[i]
}

func newLabelIndex(labels ...string) labelIndex {
	index := labelIndex(make([]iLabel, len(labels)))
	for i, label := range labels {
		index[i] = iLabel{
			Label: label,
			Index: i,
		}
	}
	sort.Stable(index)
	return index
}

func (index labelIndex) WriteFields(dst []byte, values []string) []byte {
	dst = blob.WriteU32BE(dst, uint32(len(index)))
	for i := range index {
		idx := &index[i]
		dst = blob.WriteString(dst, idx.Label)
		var v string
		if 0 <= idx.Index && idx.Index < len(values) {
			v = values[idx.Index]
		}
		dst = blob.WriteString(dst, v)
	}
	return dst
}
