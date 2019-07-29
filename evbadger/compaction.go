package evbadger

import (
	"encoding/binary"
	"sort"
	"sync"
	"time"

	"github.com/alxarch/evdb/blob"
	"github.com/dgraph-io/badger/v2"
)

// Compaction merges event snapshot compacting data to hourly batches
func (db *DB) Compaction(now time.Time) error {
	var (
		wg   sync.WaitGroup
		errc = make(chan error, len(db.events))
		gc   *badger.DB
	)
	for event := range db.events {
		b := db.events[event]
		wg.Add(1)
		go func() {
			defer wg.Done()
			errc <- compactionScan(db.badger, b.id, now)
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	if gc == nil {
		return nil
	}
	return gc.RunValueLogGC(0.5)
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

func (cc compactionBuffer) AppendBlob(s []byte) ([]byte, error) {
	for i := range cc {
		c := &cc[i]
		s = blob.WriteU64BE(s, c.id)
		s = blob.WriteU64BE(s, uint64(c.n))
	}
	return s, nil
}

func compactionScan(db *badger.DB, id eventID, now time.Time) error {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(badger.IteratorOptions{})
	defer iter.Close()
	seekEvent(iter, id, time.Time{})
	if !iter.Valid() {
		return nil
	}
	key := iter.Item().Key()
	ts, ok := parseEventKey(id, key)
	const step = int64(time.Hour)
	// Truncate timestamp to step
	ts -= -ts % step

	max := now.Truncate(time.Hour).Add(-1 * time.Hour).Unix()
	for start, end, n := ts, ts+step, 0; ok && start < max; start, end, n = end, start+step, 0 {
		for ; iter.Valid(); iter.Next() {
			key = iter.Item().Key()
			ts, ok = parseEventKey(id, key)
			if ok && start < ts && ts < end {
				n++
			} else if start == ts {
				continue
			} else {
				break
			}
		}
		if n > 0 {
			err := compactionTask(db, id, start, end)
			if err != nil {
				return err
			}
		}
	}
	return nil

}

func compactionTask(db *badger.DB, id eventID, start, end int64) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opt)
	defer iter.Close()
	seek := eventKey(id, start)
	cc := getCompactionBuffer()
	defer putCompactionBuffer(cc)

	for iter.Seek(seek[:]); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(id, key)
		if !ok || ts >= end {
			break
		}
		err := item.Value(func(v []byte) error {
			cc = cc.Read(v)
			return nil

		})
		if err != nil {
			return err
		}
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
		value, _ = cc.AppendBlob(value[:0])
		defer putBuffer(value)
		if err := txn.Set(seek[:], value); err != nil {
			return err
		}
		return txn.Commit()
	}
	return nil

}
