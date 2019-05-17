package meter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

type BadgerEvents map[string]*badgerEvent

func (store BadgerEvents) Store(s *StoreRequest) error {
	e := store[s.Event]
	if e == nil {
		return errMissingEvent(s.Event)
	}
	return e.store(s.Time.Unix(), s.Labels, s.Counters)
}

func (store BadgerEvents) Scanner(event string) Scanner {
	if s, ok := store[event]; ok {
		return s
	}
	return nil
}

type badgerEvent struct {
	*badger.DB
	id     eventID
	fields FieldCache
}

func Open(db *badger.DB, events ...string) (BadgerEvents, error) {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	dbEvents, err := loadRegisteredEvents(txn)
	if err != nil {
		return nil, err
	}
	// Read registered event names
	n := len(dbEvents)
	store := make(map[string]*badgerEvent, len(events))

	for _, event := range events {
		id := indexOf(dbEvents, event) + 1
		if id == 0 {
			// Event is not registered in DB
			dbEvents = append(dbEvents, event)
			id = len(dbEvents)
		}
		store[event] = &badgerEvent{
			DB: db,
			id: eventID(id),
		}
	}

	if len(dbEvents) != n {
		// New events have been added

		// Serialize registered event names
		v := appendStringSlice(nil, dbEvents)

		// Store event names
		var key keyBuffer
		if err := txn.Set(key[:], v); err != nil {
			return nil, err
		}

		if err := txn.Commit(nil); err != nil {
			return nil, err
		}
	}

	return store, nil
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

func loadFields(txn *badger.Txn, event eventID, id uint64) (fields Fields, err error) {
	key := valueKey(event, id)
	item, err := txn.Get(key[:])
	if err != nil {
		return
	}
	v, err := item.Value()
	if err != nil {
		return
	}

	err = fields.UnmarshalText(v)
	return
}

func (b *badgerEvent) Labels() ([]string, error) {
	return b.fields.Labels(), nil
}

func (e *badgerEvent) store(ts int64, labels []string, counters Snapshot) (err error) {
	var (
		cache   = &e.fields
		index   = newLabelIndex(labels...)
		scratch [16]byte
		value   = getBuffer()[:0]
		buf     = getBuffer()[:0]
	)
	for i := range counters {
		c := &counters[i]
		buf = index.AppendFields(buf[:0], c.Values)
		id, ok := cache.RawID(buf)
		if !ok {
			id, err = e.loadID(buf)
			if err != nil {
				putBuffer(value)
				putBuffer(buf)
				return
			}
			cache.SetRaw(id, buf)
		}
		binary.BigEndian.PutUint64(scratch[:], id)
		binary.BigEndian.PutUint64(scratch[8:], uint64(c.Count))
		value = append(value, scratch[:]...)
	}
	key := eventKey(e.id, ts)

retry:
	if err = store(e.DB, key[:], value); err == badger.ErrConflict {
		goto retry
	}
	putBuffer(value)
	putBuffer(buf)
	return nil
}

func (b *badgerEvent) loadID(data []byte) (id uint64, err error) {
	h := hashFNVa32(data)
	id = uint64(h) << 32 // Shift 0000xxxx to xxxx0000
	update := func(txn *badger.Txn) error {
		seek := valueKey(b.id, id)
		// prefix := seek[:12] // 4 byte prefix + 4 bytes reserved + 4/8 bytes of fnv hash
		n := uint32(0)
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		iter.Seek(seek[:])
		for ; iter.Valid(); iter.Next() {
			item := iter.Item()
			vid, ok := parseValueKey(b.id, item.Key())
			if !ok {
				break
			}
			value, err := item.Value()
			if err != nil {
				return err
			}
			if bytes.Equal(value, data) {
				id = vid
				return nil
			}
			n++
		}
		id = uint64(h)<<32 | uint64(n)
		key := valueKey(b.id, id)
		// Need to make a copy of data
		val := make([]byte, len(data))
		copy(val, data)
		return txn.Set(key[:], val)
	}

	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		if err = b.DB.Update(update); err != badger.ErrConflict {
			return
		}
	}
	return
}

func store(db *badger.DB, key, value []byte) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	item, err := txn.Get(key)
	switch err {
	case badger.ErrKeyNotFound:
	case nil:
		// ValueCopy appends to b[:0] so it's no good
		v, err := item.Value()
		if err != nil {
			return err
		}
		value = append(value, v...)
	default:
		return err
	}
	if err := txn.Set(key, value); err != nil {
		return err
	}
	return txn.Commit(nil)
}

// dumpKeys dumps keys from a badger.DB to a writer
func DumpKeys(db *badger.DB, w io.Writer) error {
	return db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		var k keyBuffer
		var fields Fields
		for iter.Seek(k[:]); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			switch typ, event, id := parseKey(key); typ {
			case prefixByteValue:
				v, _ := item.Value()
				if err := fields.UnmarshalText(v); err != nil {
					return err
				}

				fmt.Fprintf(w, "v event %d field %d value %v\n", event, id, fields)
			case prefixByteEvent:
				v, _ := item.Value()
				fmt.Fprintf(w, "e event %d field %d size %d\n", event, id, len(v)/16)
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

func (b *badgerEvent) query(ctx context.Context, q *Query, items chan<- ScanItem) error {
	var (
		queryFields = make(map[uint64]Fields, 16)
		match       = q.Match.Sorted()
		done        = ctx.Done()
		minT, maxT  = q.Start.Unix(), q.End.Unix()
		step        = int64(q.Step / time.Second)
	)

	txn := b.DB.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	seekEvent(iter, b.id, q.Start)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(b.id, key)
		if ok && minT <= ts && ts < maxT {
			value, err := item.Value()
			if err != nil {
				return err
			}
			ts = stepTS(ts, step)
			var id, n uint64
			for len(value) >= 16 {
				id, n, value = binary.BigEndian.Uint64(value), binary.BigEndian.Uint64(value[8:]), value[16:]
				fields, ok := queryFields[id]
				if !ok {
					fields = b.fields.Fields(id)
					if fields == nil {
						fields, err := loadFields(txn, b.id, id)
						if err != nil {
							if err == badger.ErrKeyNotFound {
								// Skip unknown id
								continue
							}
							return err
						}
						b.fields.Set(id, fields)
					}
					if fields.MatchSorted(match) {
						if len(q.Group) > 0 {
							fields = fields.GroupBy(q.EmptyValue, q.Group)
						}
					} else {
						fields = nil
					}
					queryFields[id] = fields
				}
				if fields == nil {
					continue
				}
				select {
				case items <- ScanItem{
					Time:   ts,
					Fields: fields,
					Count:  int64(n),
				}:
				case <-done:
					return nil
				}
			}
		}
	}
	return nil
}

func (b *badgerEvent) Scan(ctx context.Context, q *Query) ScanIterator {
	if b == nil {
		return emptyScanIterator{}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	items := make(chan ScanItem)
	errc := make(chan error, 1)
	iter := scanIterator{
		items:  items,
		errc:   errc,
		cancel: cancel,
	}
	go func() {
		defer close(errc)
		defer close(items)
		errc <- b.query(ctx, q, items)
	}()

	return &iter
}

func (store BadgerEvents) Compaction(now time.Time) error {
	var (
		db   *badger.DB
		wg   sync.WaitGroup
		errc = make(chan error, len(store))
	)
	wg.Add(len(store))
	for event := range store {
		b := store[event]
		db = b.DB
		go func() {
			defer wg.Done()
			errc <- b.Compaction(now)
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	if db == nil {
		return nil
	}
	return db.RunValueLogGC(0.5)
}

func (b *badgerEvent) Compaction(now time.Time) error {
	txn := b.DB.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.IteratorOptions{})
	defer iter.Close()
	seekEvent(iter, b.id, time.Time{})
	if !iter.Valid() {
		return nil
	}
	key := iter.Item().Key()
	ts, ok := parseEventKey(b.id, key)
	const step = int64(time.Hour)
	ts = stepTS(ts, step)
	max := now.Truncate(time.Hour).Add(-1 * time.Hour).Unix()
	for start, end, n := ts, ts+step, 0; ok && start < max; start, end, n = end, start+step, 0 {
		for ; iter.Valid(); iter.Next() {
			key = iter.Item().Key()
			ts, ok = parseEventKey(b.id, key)
			if ok && start < ts && ts < end {
				n++
			} else if start == ts {
				continue
			} else {
				break
			}
		}
		if n > 0 {
			err := b.compaction(start, end)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type compactionEntry struct {
	id uint64
	n  int64
}

type compactionBuffer []compactionEntry

func (cc compactionBuffer) Len() int {
	return len(cc)
}
func (cc compactionBuffer) Swap(i, j int) {
	cc[i], cc[j] = cc[j], cc[i]
}

func (cc compactionBuffer) Less(i, j int) bool {
	return cc[i].id < cc[j].id
}

func (cc compactionBuffer) Read(value []byte) compactionBuffer {
	for tail := value; len(tail) >= 16; tail = tail[16:] {
		id := binary.BigEndian.Uint64(tail)
		n := int64(binary.BigEndian.Uint64(tail[8:]))
		cc = append(cc, compactionEntry{id, n})
	}
	return cc
}

var compactionBuffers sync.Pool

func getCompactionBuffer() compactionBuffer {
	if x := compactionBuffers.Get(); x != nil {
		return x.(compactionBuffer)
	}
	return make([]compactionEntry, 0, 64)
}

func putCompactionBuffer(cc compactionBuffer) {
	compactionBuffers.Put(cc[:0])
}

func (cc compactionBuffer) Compact() compactionBuffer {
	sort.Sort(cc)
	var last *compactionEntry
	j := 0
	for i := range cc {
		c := &cc[i]
		if last != nil && last.id == c.id {
			last.n += c.n
			continue
		}
		last = c
		cc[j] = *c
		j++
	}
	return cc[:j]
}

func (cc compactionBuffer) Reset() compactionBuffer {
	return cc[:0]
}

func (cc compactionBuffer) AppendTo(out []byte) []byte {
	for i := range cc {
		c := &cc[i]
		out = appendUint64(out, c.id)
		out = appendUint64(out, uint64(c.n))
	}
	return out
}

func (b *badgerEvent) compaction(start, end int64) error {
	txn := b.DB.NewTransaction(true)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opt)
	defer iter.Close()
	seek := eventKey(b.id, start)
	cc := getCompactionBuffer()
	defer putCompactionBuffer(cc)

	for iter.Seek(seek[:]); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(b.id, key)
		if !ok || ts >= end {
			break
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		cc = cc.Read(v)
		if ts > start {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		if ts < start {
			panic("Invalid seek")
		}
	}
	cc = cc.Compact()
	if len(cc) > 0 {
		value := getBuffer()
		value = cc.AppendTo(value[:0])
		defer putBuffer(value)
		if err := txn.Set(seek[:], value); err != nil {
			return err
		}
		return txn.Commit(nil)
	}
	return nil
}

func loadRegisteredEvents(txn *badger.Txn) ([]string, error) {
	var key keyBuffer
	// Zero key holds registered events
	itm, err := txn.Get(key[:])
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v, err := itm.Value()
	if err != nil {
		return nil, err
	}
	dbEvents, _ := shiftStringSlice(v)
	return dbEvents, nil
}
