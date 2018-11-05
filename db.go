package meter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"golang.org/x/sync/errgroup"
)

type EventDB struct {
	*badger.DB
	Event  string
	mu     sync.RWMutex
	ids    map[string]uint64
	fields map[uint64]Fields
	once   OnceNoError
}

func NewEventDB(event string, db *badger.DB) *EventDB {
	return &EventDB{
		Event:  event,
		DB:     db,
		ids:    make(map[string]uint64),
		fields: make(map[uint64]Fields),
	}
}

const (
	keyPrefixSize  = 2
	keyPrefixEvent = "e:"
	keyPrefixValue = "v:"
)

func (db *EventDB) prefixSize() int {
	return keyPrefixSize + len(db.Event)
}

func (db *EventDB) appendEventKey(dst []byte, t time.Time) []byte {
	dst = append(dst, keyPrefixEvent...)
	dst = append(dst, db.Event...)
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(t.Unix()))
	dst = append(dst, buf[:]...)
	return dst
}

func (db *EventDB) appendValueKey(dst []byte, id uint64) []byte {
	dst = append(dst, keyPrefixValue...)
	dst = append(dst, db.Event...)
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], id)
	dst = append(dst, buf[:]...)
	return dst
}

func NormalizeStep(step time.Duration) time.Duration {
	if step < time.Second {
		return time.Second
	}
	return step - step%time.Second
}

type EventScanner interface {
	ScanEvent(event string, id uint64, fields Fields, ts, n int64)
}

// Sync syncs the field cache once
func (db *EventDB) Sync() (err error) {
	return db.once.Do(db.sync)
}

func (db *EventDB) sync() error {
	seek := db.appendValueKey(nil, 0)
	prefixSize := db.prefixSize()
	prefix := seek[:prefixSize]
	return db.View(func(txn *badger.Txn) (err error) {
		var (
			key  []byte
			item *badger.Item
			id   uint64
			iter = txn.NewIterator(badger.DefaultIteratorOptions)
		)
		defer iter.Close()
		for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
			item = iter.Item()
			key = item.Key()
			if len(key) == prefixSize+8 {
				id = binary.BigEndian.Uint64(key[prefixSize:])
				key, err = item.Value()
				if err != nil {
					return
				}
				db.set(key, id)
			}
		}
		return
	})

}

func (db *EventDB) Scan(start, end time.Time, match Fields, scan EventScanner) error {
	index, err := db.scanFields(match)
	if err != nil {
		return err
	}
	return db.scan(start, end, index, scan)
}

func (db *EventDB) scanFields(match Fields) (index map[uint64]Fields, err error) {
	if err = db.Sync(); err != nil {
		return
	}
	index = make(map[uint64]Fields)
	db.mu.RLock()
	defer db.mu.RUnlock()
	for id, fields := range db.fields {
		if fields.MatchSorted(match) {
			index[id] = fields
		}
	}
	return
}

func (db *EventDB) scan(start, end time.Time, index map[uint64]Fields, scan EventScanner) error {
	if scan == nil {
		return nil
	}
	minTS := start.Unix()
	seek := db.appendEventKey(nil, end)
	prefixSize := uint(db.prefixSize())
	prefix := seek[:prefixSize]

	return db.View(func(txn *badger.Txn) (err error) {
		var (
			ts     int64
			id     uint64
			n      int64
			key    []byte
			fields Fields
			item   *badger.Item
			iter   = txn.NewIterator(badger.IteratorOptions{
				Reverse: true,
			})
		)
		defer iter.Close()
		for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
			item = iter.Item()
			key = item.Key()
			if prefixSize < uint(len(key)) {
				if key = key[prefixSize:]; len(key) == 8 {
					ts = int64(binary.BigEndian.Uint64(key))
					if ts < minTS {
						return nil
					}
				}
			}
			key, err = item.Value()
			if err != nil {
				return
			}
			for len(key) >= 16 {
				id = binary.BigEndian.Uint64(key)
				n = int64(binary.BigEndian.Uint64(key[8:]))
				key = key[16:]
				fields = index[id]
				if fields != nil {
					scan.ScanEvent(db.Event, id, fields, ts, n)
				}
			}
		}
		return
	})
}

func (db *EventDB) rawFieldsID(data []byte) (id uint64, err error) {
	h := hashFNVa32(data)
	id = uint64(h) << 32
	seek := db.appendValueKey(nil, id)
	prefixSize := db.prefixSize()
	prefix := seek[:prefixSize+4]
	update := func(txn *badger.Txn) (err error) {
		var (
			item  *badger.Item
			key   []byte
			value []byte
			n     uint32
			iter  = txn.NewIterator(badger.DefaultIteratorOptions)
		)
		defer iter.Close()
		for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
			item = iter.Item()
			key = item.Key()
			if len(key) > prefixSize {
				value, err = item.Value()
				if err != nil {
					return
				}
				if bytes.Equal(value, data) {
					id = binary.BigEndian.Uint64(key[prefixSize:])
					return
				}
				n++
			}
		}
		id |= uint64(n)
		key = db.appendValueKey(nil, id)
		return txn.Set(key, data)
	}

	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		if err = db.Update(update); err != badger.ErrConflict {
			return
		}
	}
	return
}

var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4096)
		},
	}
)

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

func newLabeLIndex(labels ...string) labelIndex {
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
func (index labelIndex) AppendFields(dst []byte, values []string) []byte {
	dst = append(dst, byte(len(index)))
	for i := range index {
		idx := &index[i]
		dst = append(dst, byte(len(idx.Label)))
		dst = append(dst, idx.Label...)
		if 0 <= idx.Index && idx.Index < len(values) {
			v := values[idx.Index]
			dst = append(dst, byte(len(v)))
			dst = append(dst, v...)
		} else {
			dst = append(dst, 0)
		}
	}
	return dst
}

func (db *EventDB) store(key, value []byte) (err error) {
	txn := db.NewTransaction(true)
	item, err := txn.Get(key)
	switch err {
	case badger.ErrKeyNotFound:
	case nil:
		var v []byte
		// ValueCopy appends to b[:0] so it's no good
		v, err = item.Value()
		if err != nil {
			txn.Discard()
			return
		}
		value = append(value, v...)
	default:
		txn.Discard()
		return
	}
	err = txn.Set(key, value)
	if err != nil {
		txn.Discard()
		return
	}
	return txn.Commit(nil)
}

func (e *EventDB) getID(data []byte) (id uint64, ok bool) {
	e.mu.RLock()
	id, ok = e.ids[string(data)]
	e.mu.RUnlock()
	return
}

func (e *EventDB) set(data []byte, id uint64) {
	e.mu.Lock()
	if _, ok := e.fields[id]; ok {
		e.mu.Unlock()
		return
	}
	s := string(data)
	if e.ids == nil {
		e.ids = make(map[string]uint64)
	}
	e.ids[s] = id
	if e.fields == nil {
		e.fields = make(map[uint64]Fields)
	}
	e.fields[id] = FieldsFromString(s)
	e.mu.Unlock()
}

func (db *EventDB) StoreEvent(tm time.Time, event *Event) (err error) {
	s := event.Flush(nil)
	if tm.IsZero() {
		tm = time.Now()
	}
	err = db.Store(tm, event.Labels, s)
	if err != nil {
		event.Merge(s)
	}
	return
}

func (db *EventDB) Store(tm time.Time, labels []string, counters Snapshot) (err error) {
	var (
		index   = newLabeLIndex(labels...)
		id      uint64
		c       *Counter
		scratch [16]byte
		value   = bufferPool.Get().([]byte)
		buf     = bufferPool.Get().([]byte)
		ok      bool
	)
	defer func() {
		bufferPool.Put(value)
		bufferPool.Put(buf)
	}()
	for i := range counters {
		c = &counters[i]
		buf = index.AppendFields(buf[:0], c.Values)
		id, ok = db.getID(buf)
		if !ok {
			id, err = db.rawFieldsID(buf)
			if err != nil {
				return err
			}
			db.set(buf, id)
		}
		binary.BigEndian.PutUint64(scratch[:], id)
		binary.BigEndian.PutUint64(scratch[8:], uint64(c.Count))
		value = append(value, scratch[:]...)
	}
	buf = db.appendEventKey(buf[:0], tm)

retry:
	if err = db.store(buf, value); err == badger.ErrConflict {
		goto retry
	}
	return
}

type MultiEventDB map[string]*EventDB

func NewMultiEventDB(db *badger.DB, events ...string) MultiEventDB {
	mdb := make(map[string]*EventDB, len(events))
	for _, event := range events {
		mdb[event] = NewEventDB(event, db)
	}
	return mdb
}

func (db MultiEventDB) Get(event string) (*EventDB, error) {
	if e := db[event]; e != nil {
		return e, nil
	}
	return nil, fmt.Errorf("Unknown event %q", event)
}

func (db MultiEventDB) Summary(event string, q *Query) ([]Summary, error) {
	e := db[event]
	if e == nil {
		return nil, fmt.Errorf("Unknown event %q", event)
	}
	scan := NewSummaryScan(event, q)
	if err := e.Scan(q.Start, q.End, q.Match, scan); err != nil {
		return nil, err
	}
	return scan.Results, nil
}

func (db MultiEventDB) Scan(q *Query, events ...string) ([]*TimeSeries, error) {
	g := errgroup.Group{}
	scans := make([]*TimeSeriesScan, len(events))
	for i, event := range events {
		i, event := i, event
		g.Go(func() error {
			event, err := db.Get(event)
			if err != nil {
				return err
			}
			scan := NewTimeSeriesScan(event.Event, q)
			if err := event.Scan(q.Start, q.End, q.Match, scan); err != nil {
				return err
			}
			scans[i] = scan
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	size := 0
	for i := range scans {
		size += len(scans[i].Results)
	}
	results := make([]*TimeSeries, 0, size)
	for _, scan := range scans {
		for j := range scan.Results {
			results = append(results, &scan.Results[j])
		}
	}
	return results, nil
}

func DumpKeys(db *badger.DB, w io.Writer) error {
	return db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
		})
		defer iter.Close()
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			id := binary.BigEndian.Uint64(key[len(key)-8:])
			switch key[0] {
			case 'v':
				v, _ := item.Value()
				fields := FieldsFromString(string(v))
				fmt.Fprintf(w, "v %q %08x %v\n", key[2:len(key)-8], id, fields)
			case 'e':
				fmt.Fprintf(w, "e %q@%d\n", key[2:len(key)-8], id)
			}
		}
		return nil
	})
}
