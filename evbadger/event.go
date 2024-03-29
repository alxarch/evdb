package evbadger

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/evutil"
	"github.com/dgraph-io/badger/v2"
)

type eventDB struct {
	badger *badger.DB
	id     eventID
	fields evutil.FieldCache
}

func (e *eventDB) Labels() ([]string, error) {
	return e.fields.Labels(), nil
}

// Store implements Store interface
func (e *eventDB) Store(s *evdb.Snapshot) error {
	return e.store(s.Time.Unix(), s.Labels, s.Counters)
}

func (e *eventDB) store(ts int64, labels []string, counters []events.Counter) (err error) {
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

func (e *eventDB) Fields(id uint64) (evdb.Fields, error) {
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

type resolver func(uint64) (evdb.Fields, error)

func (e *eventDB) resolver(m evdb.MatchFields) resolver {
	cache := make(map[uint64]evdb.Fields)
	return func(id uint64) (evdb.Fields, error) {
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
		} else if !m.Match(fields) {
			fields = nil
		}
		cache[id] = fields
		return fields, nil

	}

}

func (e *eventDB) Query(ctx context.Context, q *evdb.Query) (results evdb.Results, err error) {
	var (
		ok         bool
		resolver   = e.resolver(q.Fields)
		minT, maxT = q.Start.Unix(), q.End.Unix()
		step       = fixStep(q.Step)
		ts         int64
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
				results = results.Add(q.Event, fields, ts, float64(int64(n)))
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
		ts, ok = parseEventKey(e.id, key)
		if ok && minT <= ts && ts < maxT {
			ts = stepTS(ts, step)
			err = item.Value(scanValue)
			if err != nil {
				return nil, err
			}
		}
	}
	for i := range results {
		results[i].TimeRange = q.TimeRange
	}
	return
}

func fixStep(step time.Duration) int64 {
	switch {
	case step >= time.Second:
		return int64(step / time.Second)
	case 0 < step && step < time.Second:
		return 1
	case step < 0:
		return -1
	default:
		return 0
	}
}

func stepTS(ts, step int64) int64 {
	if step > 0 {
		return ts - ts%step
	}
	if step == 0 {
		return ts
	}
	return 0
}
