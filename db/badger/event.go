package badgerdb

import (
	"bytes"
	"context"
	"encoding/binary"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/dgraph-io/badger/v2"
)

type eventDB struct {
	badger *badger.DB
	id     eventID
	fields meter.FieldCache
}

func (e *eventDB) Labels() ([]string, error) {
	return e.fields.Labels(), nil
}

// Store implements Store interface
func (e *eventDB) Store(s *meter.Snapshot) error {
	return e.store(s.Time.Unix(), s.Labels, s.Counters)
}

func (e *eventDB) store(ts int64, labels []string, counters meter.CounterSlice) (err error) {
	var (
		cache   = &e.fields
		index   = newLabelIndex(labels...)
		scratch [16]byte
		value   = getBuffer()[:0]
		buf     = getBuffer()[:0]
	)
	for i := range counters {
		c := &counters[i]
		buf = index.WriteFields(buf[:0], c.Values)
		id, ok := cache.BlobID(buf)
		if !ok {
			id, err = e.loadID(buf)
			if err != nil {
				putBuffer(value)
				putBuffer(buf)
				return
			}
			cache.SetBlob(id, buf)
		}
		binary.BigEndian.PutUint64(scratch[:], id)
		binary.BigEndian.PutUint64(scratch[8:], uint64(c.Count))
		value = append(value, scratch[:]...)
	}
	key := eventKey(e.id, ts)

retry:
	if err = store(e.badger, key[:], value); err == badger.ErrConflict {
		goto retry
	}
	putBuffer(value)
	putBuffer(buf)
	return nil
}

func store(db *badger.DB, key, value []byte) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	item, err := txn.Get(key)
	switch err {
	case badger.ErrKeyNotFound:
	case nil:
		// ValueCopy appends to b[:0] so it's no good
		item.Value(func(v []byte) error {
			value = append(value, v...)
			return nil
		})
	default:
		return err
	}
	if err := txn.Set(key, value); err != nil {
		return err
	}
	return txn.Commit()
}

func (e *eventDB) loadID(data []byte) (id uint64, err error) {
	h := hashFNVa32(data)
	id = uint64(h) << 32 // Shift 0000xxxx to xxxx0000
	update := func(txn *badger.Txn) error {
		seek := valueKey(e.id, id)
		// prefix := seek[:12] // 4 byte prefix + 4 bytes reserved + 4/8 bytes of fnv hash
		n := uint32(0)
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		iter.Seek(seek[:])
		for ; iter.Valid(); iter.Next() {
			item := iter.Item()
			vid, ok := parseValueKey(e.id, item.Key())
			if !ok {
				break
			}
			err := item.Value(func(value []byte) error {
				if bytes.Equal(value, data) {
					id = vid
				}
				return nil
			})
			if err != nil {
				return err
			}
			if id == vid {
				return nil
			}
			n++
		}
		id = uint64(h)<<32 | uint64(n)
		key := valueKey(e.id, id)
		// Need to make a copy of data
		val := make([]byte, len(data))
		copy(val, data)
		return txn.Set(key[:], val)
	}

	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		if err = e.badger.Update(update); err != badger.ErrConflict {
			return
		}
	}
	return
}

func (e *eventDB) Fields(id uint64) (meter.Fields, error) {
	fields := e.fields.Fields(id)
	if fields != nil {
		return fields, nil
	}
	txn := e.badger.NewTransaction(false)
	defer txn.Discard()
	key := valueKey(e.id, id)
	item, err := txn.Get(key[:])
	if err != nil {
		return nil, err
	}
	if err := item.Value(fields.UnmarshalBinary); err != nil {
		return nil, err
	}
	e.fields.Set(id, fields)
	return fields, nil
}

type resolver func(uint64) (meter.Fields, error)

func (e *eventDB) resolver(q *meter.Query) resolver {
	cache := make(map[uint64]meter.Fields)
	match := q.Match.Sorted()
	return func(id uint64) (meter.Fields, error) {
		fields, ok := cache[id]
		if ok {
			return fields, nil
		}
		fields, err := e.Fields(id)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return nil, err
			}
			fields = nil
		} else if fields.MatchSorted(match) {
			if len(q.Group) > 0 {
				fields = fields.GroupBy(q.EmptyValue, q.Group)
			}
		} else {
			fields = nil
		}
		cache[id] = fields
		return fields, nil

	}

}

func (e *eventDB) Scan(ctx context.Context, q *meter.Query) (meter.ScanResults, error) {
	type scanItem struct {
		meter.Fields
		V float64
	}
	var (
		resolver   = e.resolver(q)
		results    meter.ScanResults
		minT, maxT = q.Start.Unix(), q.End.Unix()
		batch      []scanItem
		scanValue  = func(value []byte) error {
			var id, n uint64
			for len(value) >= 16 {
				id, n, value = binary.BigEndian.Uint64(value), binary.BigEndian.Uint64(value[8:]), value[16:]
				fields, err := resolver(id)
				if err != nil {
					return err
				}
				if fields == nil {
					continue
				}
				batch = append(batch, scanItem{
					Fields: fields,
					V:      float64(n),
				})
			}
			return nil
		}
	)

	txn := e.badger.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	seekEvent(iter, e.id, q.Start)
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(e.id, key)
		if ok && minT <= ts && ts < maxT {
			batch = batch[:0]
			err := item.Value(scanValue)
			if err != nil {
				return nil, err
			}
			if len(batch) == 0 {
				continue
			}
			for i := range batch {
				item := &batch[i]
				results = results.Add(item.Fields, ts, item.V)
			}
		}
	}
	return results, nil
}
