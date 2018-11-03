package meter

import (
	"bytes"
	"encoding/binary"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

type DB struct {
	TTL     time.Duration
	MinStep time.Duration
	*badger.DB
}

const (
	keyPrefixSize  = 2
	keyPrefixEvent = "e:"
	keyPrefixValue = "v:"
)

func (db *DB) prefixSize(event string) int {
	return keyPrefixSize + len(event)
}

func (db *DB) appendEventKey(dst []byte, event string, t time.Time) []byte {
	dst = append(dst, keyPrefixEvent...)
	dst = append(dst, event...)
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(t.UnixNano()))
	dst = append(dst, buf[:]...)
	return dst
}

func (db *DB) appendValueKey(dst []byte, event string, id uint64) []byte {
	dst = append(dst, keyPrefixValue...)
	dst = append(dst, event...)
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], id)
	dst = append(dst, buf[:]...)
	return dst
}

func (db *DB) NormalizeStep(step time.Duration) time.Duration {
	minStep := db.MinStep
	if minStep < time.Second {
		minStep = time.Second
	}
	if step < minStep {
		return minStep
	}
	return step - step%minStep
}

type EventScanner interface {
	ScanEvent(event string, id uint64, fields Fields, ts, n int64)
}

func (db *DB) scanValues(event string, matcher RawFieldMatcher) (index map[uint64]Fields, err error) {
	if matcher == nil {
		matcher = identityFieldMatcher{}
	}
	seek := db.appendValueKey(nil, event, 0)
	prefixSize := db.prefixSize(event)
	prefix := seek[:prefixSize]
	index = make(map[uint64]Fields)
	err = db.View(func(txn *badger.Txn) (err error) {
		var (
			key  []byte
			item *badger.Item
			id   uint64
			iter = txn.NewIterator(badger.IteratorOptions{
				PrefetchValues: false,
			})
		)
		defer iter.Close()
		for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
			item = iter.Item()
			key = item.Key()
			if len(key) > prefixSize {
				id = binary.BigEndian.Uint64(key[prefixSize:])
				key, err = item.Value()
				if err != nil {
					return
				}
				if matcher.MatchRawFields(key) {
					index[id] = FieldsFromString(string(key))
				}
			}
		}
		return
	})
	if err != nil {
		return nil, err
	}
	return index, nil
}

func (db *DB) scanEvents(
	event string,
	start, end time.Time,
	step time.Duration,
	index map[uint64]Fields,
	scan EventScanner,
) error {
	if scan == nil {
		return nil
	}
	step = db.NormalizeStep(step)
	end = end.Truncate(step)
	minTS := start.Truncate(step).UnixNano()
	seek := db.appendEventKey(nil, event, end)
	prefixSize := uint(db.prefixSize(event))
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
					ts -= ts % int64(step)
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
					scan.ScanEvent(event, id, fields, ts, n)
				}
			}
		}
		return
	})
}

func (db *DB) rawFieldsID(event string, data []byte) (id uint64, err error) {
	h := hashFNVa32(data)
	id = uint64(h) << 32
	seek := db.appendValueKey(nil, event, id)
	prefixSize := db.prefixSize(event)
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
		key = db.appendValueKey(nil, event, id)
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

func (db *DB) Store(tm time.Time, event string, labels []string, counters Snapshot) (err error) {
	var (
		value   = bufferPool.Get().([]byte)
		buf     = bufferPool.Get().([]byte)
		scratch [16]byte
		id      uint64
		c       *Counter
	)
	defer func() {
		bufferPool.Put(value)
		bufferPool.Put(buf)
	}()
	for i := range counters {
		c = &counters[i]
		buf = ZipFields(buf[:0], labels, c.Values)
		id, err = db.rawFieldsID(event, buf)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint64(scratch[:], id)
		binary.BigEndian.PutUint64(scratch[8:], uint64(c.Count))
		value = append(value, scratch[:]...)
	}
	buf = db.appendEventKey(buf[:0], event, tm)

retry:
	err = db.Update(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(buf)
		if err == badger.ErrKeyNotFound {
			return txn.Set(buf, value)
		}
		v, err := item.Value()
		if err != nil {
			return
		}
		value = append(value, v...)
		if db.TTL > 0 {
			return txn.SetWithTTL(buf, value, db.TTL)
		}
		return txn.Set(buf, value)
	})
	if err == badger.ErrConflict {
		goto retry
	}
	return
}

type RawFieldMatcher interface {
	MatchRawFields(raw []byte) bool
}

type identityFieldMatcher struct{}

func (identityFieldMatcher) MatchRawFields(_ []byte) bool {
	return true
}
