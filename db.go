package meter

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"golang.org/x/sync/errgroup"
)

type EventDB struct {
	*badger.DB
	event  string
	mu     sync.RWMutex
	ids    map[string]uint64
	fields map[uint64]Fields
	once   OnceNoError
}

func NewEventDB(event string, db *badger.DB) *EventDB {
	return &EventDB{
		event:  event,
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

func (db *EventDB) Event() string {
	return db.event
}

func (db *EventDB) EventKey(t time.Time) []byte {
	return db.appendEventKey(nil, t)
}

func (db *EventDB) Query(q *Query) ([]*ScanResult, error) {
	var s scanResults
	if len(q.Group) > 0 {
		s = newGroupEventScan(q)
	} else {
		s = newEventScan(q)
	}
	err := db.scan(q.Start, q.End, q.Step, s)
	if err != nil {
		return nil, err
	}
	return s.Results(), nil

}
func (db *EventDB) Labels() ([]string, error) {
	err := db.Sync()
	if err != nil {
		return nil, err
	}
	var labels []string
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, fields := range db.fields {
		for i := range fields {
			f := &fields[i]
			labels = appendDistinct(labels, f.Label)
		}
	}
	sort.Strings(labels)
	return labels, nil
}

func (db *EventDB) prefixSize() int {
	return keyPrefixSize + len(db.event)
}

func (db *EventDB) appendEventKey(dst []byte, t time.Time) []byte {
	dst = append(dst, keyPrefixEvent...)
	dst = append(dst, db.event...)
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(t.Unix()))
	dst = append(dst, buf[:]...)
	return dst
}

func (db *EventDB) appendValueKey(dst []byte, id uint64) []byte {
	dst = append(dst, keyPrefixValue...)
	dst = append(dst, db.event...)
	buf := [8]byte{}
	binary.BigEndian.PutUint64(buf[:], id)
	dst = append(dst, buf[:]...)
	return dst
}

func NormalizeStep(step time.Duration) time.Duration {
	switch {
	case step <= 0:
		return step
	case step < time.Second:
		return time.Second
	default:
		return step - step%time.Second
	}
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

func (db *EventDB) loadID(data []byte) (id uint64, err error) {
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

func (e *EventDB) getFields(id uint64) (fields Fields) {
	e.mu.RLock()
	fields = e.fields[id]
	e.mu.RUnlock()
	return
}

func (e *EventDB) loadFields(id uint64) (fields Fields, err error) {
	fields = e.getFields(id)
	if fields != nil {
		return fields, nil
	}
	buf := bufferPool.Get().([]byte)
	buf = e.appendValueKey(buf[:0], id)
	txn := e.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(buf)
	bufferPool.Put(buf)
	if err != nil {
		return
	}
	v, err := item.Value()
	if err != nil {
		return
	}
	raw := string(v)
	fields = FieldsFromString(raw)
	e.mu.Lock()
	e.fields[id] = fields
	e.ids[raw] = id
	e.mu.Unlock()
	return

}
func (e *EventDB) AppendFields(fields Fields, id uint64) (Fields, error) {
	f := e.getFields(id)
	if f != nil {
		return append(fields, f...), nil
	}
	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)
	buf = e.appendValueKey(buf[:0], id)
	err := e.View(func(txn *badger.Txn) (err error) {
		item, err := txn.Get(buf)
		if err == nil {
			buf, err = item.ValueCopy(buf)
		}
		return
	})
	if err == nil {
		e.set(buf, id)
		fields = fields.AppendRawString(string(buf))
	}
	return fields, err
}

func (e *EventDB) getID(data []byte) (id uint64, ok bool) {
	e.mu.RLock()
	id, ok = e.ids[string(data)]
	e.mu.RUnlock()
	return
}

func (e *EventDB) set(data []byte, id uint64) (fields Fields) {
	e.mu.Lock()
	fields, ok := e.fields[id]
	if ok {
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
	fields = FieldsFromString(s)
	e.fields[id] = fields
	e.mu.Unlock()
	return
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
			id, err = db.loadID(buf)
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

func (db *MultiEventDB) Labels(events ...string) (map[string][]string, error) {
	g := new(errgroup.Group)
	mu := sync.Mutex{}
	results := make(map[string][]string, len(events))
	for _, event := range events {
		event := event
		g.Go(func() error {
			db, err := db.Get(event)
			if err != nil {
				return err
			}
			labels, err := db.Labels()
			if err != nil {
				return err
			}
			mu.Lock()
			results[event] = labels
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (db *MultiEventDB) Query(q *Query, events ...string) (MultiScanResults, error) {
	g := new(errgroup.Group)
	mu := sync.Mutex{}
	m := make(map[string][]*ScanResult, len(events))
	for _, event := range events {
		event := event
		g.Go(func() error {
			db, err := db.Get(event)
			if err != nil {
				return err
			}
			results, err := db.Query(q)
			if err != nil {
				for _, r := range results {
					r.Close()
				}
				return err
			}
			mu.Lock()
			m[event] = results
			mu.Unlock()
			return nil
		})
	}
	return m, g.Wait()
}

func (db MultiEventDB) EventSummary(q *Query, events ...string) (*EventSummaries, error) {
	results, err := db.Query(q, events...)
	defer results.Close()
	if err != nil {
		return nil, err
	}
	return NewEventSummaries(q.EmptyValue, results.Results()...), nil
}

func (db MultiEventDB) FieldSummary(q *Query, events ...string) (FieldSummaries, error) {
	results, err := db.Query(q, events...)
	defer results.Close()
	if err != nil {
		return nil, err
	}
	sums := FieldSummaries{}
	sums = sums.Append(results.Results()...)
	return sums, nil
}

func (db MultiEventDB) Summary(q *Query, events ...string) (MultiScanResults, error) {
	q.Step = -1
	return db.Query(q, events...)
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

type Query struct {
	Match      Fields        `json:"match,omitempty"`
	Group      []string      `json:"group,omitempty"`
	Start      time.Time     `json:"start"`
	End        time.Time     `json:"end"`
	Step       time.Duration `json:"step"`
	EmptyValue string        `json:"empty,omitempty"`
}

func (q *Query) SetValues(values url.Values) {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			q.Step, _ = time.ParseDuration(step[0])
		} else {
			q.Step = 0
		}
	} else {
		q.Step = -1
	}
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	q.Start = time.Unix(start, 0)
	end, _ := strconv.ParseInt(values.Get("end"), 10, 64)
	q.End = time.Unix(end, 0)
	match := q.Match[:0]
	for key, values := range values {
		if strings.HasPrefix(key, "match.") {
			label := strings.TrimPrefix(key, "match.")
			for _, value := range values {
				match = append(match, Field{
					Label: label,
					Value: value,
				})
			}
		}
	}
	sort.Stable(match)
	q.Match = match
	group, ok := values["group"]
	if ok && len(group) == 0 {
		group = make([]string, 0, len(q.Match))
		group = q.Match.appendDistinctLabels(group)
	}
	q.Group = group
	q.EmptyValue = values.Get("empty")
}
