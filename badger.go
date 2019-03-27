package meter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"golang.org/x/sync/errgroup"
)

type badgerStore struct {
	*badger.DB
	cache FieldCache
}

func (b *badgerStore) seekEvent(iter *badger.Iterator, tm time.Time) {
	key := getBuffer()
	key = appendEventKey(key[:0], tm)
	iter.Seek(key)
	putBuffer(key)
}

func (b *badgerStore) seekValue(iter *badger.Iterator, id uint64) {
	key := getBuffer()
	key = appendValueKey(key[:0], id)
	iter.Seek(key)
	putBuffer(key)
}

func (b *badgerStore) loadFields(txn *badger.Txn, id uint64) (string, error) {
	buf := getBuffer()
	buf = appendValueKey(buf[:0], id)
	item, err := txn.Get(buf)
	putBuffer(buf)
	if err != nil {
		return "", err
	}
	v, err := item.Value()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (b *badgerStore) Labels() ([]string, error) {
	return b.cache.Labels(), nil
}

func (b *badgerStore) store(tm time.Time, labels []string, counters Snapshot) (err error) {
	var (
		cache   = &b.cache
		index   = newLabeLIndex(labels...)
		scratch [16]byte
		value   = getBuffer()
		buf     = getBuffer()
	)
	for i := range counters {
		c := &counters[i]
		buf = index.AppendFields(buf[:0], c.Values)
		id, ok := cache.RawID(buf)
		if !ok {
			id, err = b.loadID(buf)
			if err != nil {
				putBuffer(value)
				putBuffer(buf)
				return
			}
			cache.SetString(id, string(buf))
		}
		binary.BigEndian.PutUint64(scratch[:], id)
		binary.BigEndian.PutUint64(scratch[8:], uint64(c.Count))
		value = append(value, scratch[:]...)
	}
	buf = appendEventKey(buf[:0], tm)

retry:
	if err = b.storeEvent(buf, value); err == badger.ErrConflict {
		goto retry
	}
	putBuffer(value)
	putBuffer(buf)
	return nil

}

func (b *badgerStore) loadID(data []byte) (id uint64, err error) {
	h := hashFNVa32(data)
	id = uint64(h) << 32
	seek := appendValueKey(nil, id)
	prefix := seek[:6] // 1 byte prefix + 1 byte reserved + 4 bytes of fnv hash
	update := func(txn *badger.Txn) error {
		n := uint32(0)
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			id, _ = parseValueKey(item.Key())
			value, err := item.Value()
			if err != nil {
				return err
			}
			if bytes.Equal(value, data) {
				return nil
			}
			n++
		}
		id |= uint64(n)
		key := appendValueKey(nil, id)
		return txn.Set(key, data)
	}

	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		if err = b.Update(update); err != badger.ErrConflict {
			return
		}
	}
	return
}

func (b *badgerStore) storeEvent(key, value []byte) error {
	txn := b.NewTransaction(true)
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
func (b *badgerStore) dumpKeys(w io.Writer) error {
	return b.View(func(txn *badger.Txn) error {
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

func (b *badgerStore) query(ctx context.Context, q *Query, items chan<- ScanItem) error {
	var (
		queryFields = make(map[uint64]Fields, 16)
		match       = q.Match.Sorted()
		done        = ctx.Done()
		minT, maxT  = q.Start.Unix(), q.End.Unix()
		step        = int64(q.Step)
	)

	txn := b.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	b.seekEvent(iter, q.Start)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(key)
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
					fields = b.cache.Fields(id)
					if fields == nil {
						s, err := b.loadFields(txn, id)
						if err != nil {
							return err
						}
						fields = b.cache.SetString(id, s)
					}
					if match.MatchSorted(fields) {
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

func (b *badgerStore) Scan(ctx context.Context, q *Query) ScanIterator {
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

func (b *badgerStore) Compaction(now time.Time) error {
	txn := b.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.IteratorOptions{})
	defer iter.Close()
	b.seekEvent(iter, time.Time{})
	if !iter.Valid() {
		return nil
	}
	key := iter.Item().Key()
	ts, ok := parseEventKey(key)
	const step = int64(time.Hour)
	ts = stepTS(ts, step)
	max := now.Truncate(time.Hour).Add(-1 * time.Hour).Unix()
	for start, end, n := ts, ts+step, 0; ok && start < max; start, end, n = end, start+step, 0 {
		for ; iter.Valid(); iter.Next() {
			key = iter.Item().Key()
			ts, ok = parseEventKey(key)
			if ok && start < ts && ts < end {
				n++
			} else if start == ts {
				continue
			} else {
				break
			}
		}
		if n > 0 {
			err := compaction(b.DB, start, end)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func parseKey(k []byte) (uint64, byte) {
	if len(k) == 10 {
		return binary.BigEndian.Uint64(k[2:]), k[0]
	}
	return 0, 255
}

func parseEventKey(k []byte) (int64, bool) {
	id, b := parseKey(k)
	return int64(id), b == prefixByteEvent
}

func parseValueKey(k []byte) (uint64, bool) {
	id, b := parseKey(k)
	return id, b == prefixByteValue
}

type badgerEvents map[string]*badgerStore

func (evs badgerEvents) Scanner(event string) Scanner {
	return evs[event]
}

func NewBadgerEvents(opts badger.Options, events ...string) (badgerEvents, error) {
	mu := sync.Mutex{}
	evs := make(map[string]*badgerStore, len(events))
	dir := opts.Dir
	vdir := opts.ValueDir
	grp := errgroup.Group{}
	for _, event := range events {
		e := event
		o := opts
		o.Dir = path.Join(dir, event)
		o.ValueDir = path.Join(vdir, event)
		grp.Go(func() error {
			db, err := badger.Open(o)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			evs[e] = &badgerStore{
				DB: db,
			}
			return nil
		})
	}
	return evs, grp.Wait()
}

func (b badgerEvents) Store(s *StoreRequest) (err error) {
	e := b[s.Event]
	if e == nil {
		return fmt.Errorf("Unknown event %q", s.Event)
	}
	return e.store(s.Time, s.Labels, s.Counters)
}

func appendUint64(dst []byte, n uint64) []byte {
	return append(dst,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

// func appendShortString(dst []byte, s string) []byte {
// 	dst = append(dst, byte(len(s)))
// 	dst = append(dst, s...)
// 	return dst
// }

// func eventKey(dst []byte, event string, ts int64) []byte {
// 	dst = append(dst, prefixByteEvent)
// 	dst = appendShortString(dst, event)
// 	dst = appendUint64(dst, uint64(ts))
// 	return dst
// }

// func valueKey(dst []byte, event string, ts int64) []byte {
// 	dst = append(dst, prefixByteValue)
// 	dst = appendShortString(dst, event)
// 	dst = appendUint64(dst, uint64(ts))
// 	return dst
// }

func appendKey(dst []byte, prefix byte, id uint64) []byte {
	return append(dst,
		prefix,
		0,
		byte(id>>56),
		byte(id>>48),
		byte(id>>40),
		byte(id>>32),
		byte(id>>24),
		byte(id>>16),
		byte(id>>8),
		byte(id),
	)
}

func appendValueKey(dst []byte, id uint64) []byte {
	return appendKey(dst, prefixByteValue, id)
}

func appendEventKey(dst []byte, tm time.Time) []byte {
	ts := tm.Unix()
	return appendKey(dst, prefixByteValue, uint64(ts))
}

// func (b *badgerStore) Scan(q *Query) (ScanResults, error) {
// 	var results ScanResults
// 	cache := &b.cache
// 	match := q.Match
// 	if !sort.IsSorted(match) {
// 		match = match.Sorted()
// 	}
// 	step := int64(normalizeStep(q.Step))
// 	minT, maxT := q.Start.Unix(), q.End.Unix()
// 	grouped := make(map[uint64]Fields, 64)
// 	txn := b.NewTransaction(false)
// 	defer txn.Discard()
// 	iter := txn.NewIterator(badger.DefaultIteratorOptions)
// 	defer iter.Close()
// 	b.seekEvent(iter, q.Start)
// 	for ; iter.Valid(); iter.Next() {
// 		item := iter.Item()
// 		key := item.Key()
// 		ts, ok := parseEventKey(key)
// 		if ok && minT <= ts && ts < maxT {
// 			v, err := item.Value()
// 			if err != nil {
// 				return results, err
// 			}
// 			ts = stepTS(ts, step)
// 			for ; len(v) >= 16; v = v[16:] {
// 				id := binary.BigEndian.Uint64(v)
// 				fields, ok := grouped[id]
// 				if !ok {
// 					fields = cache.Fields(id)
// 					if fields == nil {
// 						s, err := b.loadFields(txn, id)
// 						if err != nil {
// 							return results, err
// 						}
// 						fields = cache.SetString(id, s)
// 					}
// 					if match.MatchSorted(fields) {
// 						if len(q.Group) > 0 {
// 							fields = fields.GroupBy(q.EmptyValue, q.Group)
// 						}
// 					} else {
// 						fields = nil
// 					}
// 					grouped[id] = fields
// 				}
// 				if fields == nil {
// 					continue
// 				}
// 				n := binary.BigEndian.Uint64(v[8:])
// 				results = results.Add(b.event, fields, int64(n), int64(ts))
// 			}
// 		}
// 	}
// 	return results, nil
// }

// const (
// 	eventKey = 'e'
// 	valueKey = 'v'
// )

// func appendEventKey(key []byte, event string, ts int64) []byte {
// 	return appendKey(key, eventKey, event, uint64(ts))
// }
// func appendValueKey(key []byte, event string, id uint64) []byte {
// 	return appendKey(key, valueKey, event, id)
// }
// func appendKey(key []byte, typ byte, event string, ts uint64) []byte {
// 	key = append(key, typ)
// 	key = append(key, byte(len(event)))
// 	key = append(key, event...)
// 	buf := [8]byte{}
// 	binary.BigEndian.PutUint64(buf[:], ts)
// 	return append(key, buf[:]...)
// }

// type rawValues map[uint64]string

// func (b *badgerStore) loadValues(event string) (rawValues, error) {
// 	values := make(map[uint64]string)
// 	err := b.db.View(func(txn *badger.Txn) error {
// 		iter := txn.NewIterator(badger.IteratorOptions{})
// 		b.seekValue(iter, event, 0)
// 		for ; iter.Valid(); iter.Next() {
// 			item := iter.Item()
// 			key := item.Key()
// 			_, e, id := ParseKey(key)
// 			if string(e) != event {
// 				break
// 			}
// 			value, err := item.Value()
// 			if err != nil {
// 				iter.Close()
// 				return err
// 			}
// 			values[id] = string(value)
// 		}
// 		iter.Close()
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	return values, nil
// }

// type fieldCache struct {
// 	FieldCache
// 	synced uint32
// }

// func loadFields(txn *badger.Txn, cache *FieldCache, event string, id uint64) (Fields, error) {
// 	fields := cache.Fields(id)
// 	if fields != nil {
// 		return fields, nil
// 	}
// 	s, err := loadFields(txn, event, id)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return cache.Set(id, s), nil
// }

// func (cache *fieldCache) reset(index map[uint64]string) {
// 	ids := make(map[string]uint64, len(index))
// 	fields := make(map[uint64]Fields, len(index))
// 	for id, s := range index {
// 		fields[id] = FieldsFromString(s)
// 		ids[s] = id
// 	}
// 	cache.ids, cache.fields = ids, fields
// }

// func (cache *fieldCache) Sync(load func() (rawValues, error)) error {
// 	if atomic.LoadUint32(&cache.synced) == 1 {
// 		return nil
// 	}
// 	cache.mu.Lock()
// 	defer cache.mu.Unlock()
// 	if cache.synced == 0 {
// 		values, err := load()
// 		if err != nil {
// 			return err
// 		}
// 		defer atomic.StoreUint32(&cache.synced, 1)
// 		cache.reset(values)
// 	}
// 	return nil
// }
