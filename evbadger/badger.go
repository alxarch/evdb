package evbadger

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/blob"
	"github.com/dgraph-io/badger/v2"
)

// DB is a collection of Events stored in BadgerDB
type DB struct {
	evdb.Scanner
	badger *badger.DB
	events map[string]*eventDB
}

// Open opens a new Event collection stored in BadgerDB
func Open(b *badger.DB, events ...string) (*DB, error) {
	eventIDs, err := loadEventIDs(b, events...)
	if err != nil {
		return nil, err
	}
	db := DB{
		badger: b,
		events: make(map[string]*eventDB, len(events)),
	}
	db.Scanner = evdb.NewScanner(&db)

	for i, event := range events {
		id := eventIDs[i]
		db.events[event] = &eventDB{
			badger: b,
			id:     eventID(id),
		}
	}

	return &db, nil
}

// Storer implements Storerer interface
func (db *DB) Storer(event string) evdb.Storer {
	if e, ok := db.events[event]; ok {
		return e
	}
	return nil
}

// ScanQuery implements evdb.ScanQuerier interface
func (db *DB) ScanQuery(ctx context.Context, q *evdb.ScanQuery) (evdb.Results, error) {
	if s, ok := db.events[q.Event]; ok {
		return s.ScanQuery(ctx, q)
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

func loadEventIDs(db *badger.DB, events ...string) ([]eventID, error) {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	dbEvents, err := loadEvents(txn)
	if err != nil {
		return nil, err
	}
	n := len(dbEvents)
	ids := make([]eventID, len(events))
	for i, event := range events {
		id := indexOf(dbEvents, event) + 1
		if id == 0 {
			// Event is not registered in DB
			dbEvents = append(dbEvents, event)
			id = len(dbEvents)
		}
		ids[i] = eventID(id)
	}
	if len(dbEvents) != n {
		// New events have been added

		// Serialize registered event names
		v := blob.WriteStrings(nil, dbEvents)

		// Store event names
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

var _ evdb.DB = (*DB)(nil)

type badgerOpener struct{}

func (_ badgerOpener) Open(configURL string) (evdb.DB, error) {
	options, events, err := parseURL(configURL)
	if err != nil {
		return nil, err
	}
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	return Open(db, events...)
}

const urlScheme = "badger"

func init() {
	evdb.Register(urlScheme, badgerOpener{})
}
