package meter

// type QStore interface {
// 	Query(event *Desc, step time.Duration, start, end time.Time, m Mapper, r Reducer) (Buckets, error)
// 	Store(event *Desc, t time.Time, snapshot Snapshot) error
// }

// type Timestamp int64

// func (fields Fields) Hash() uint32 {
// 	h := newFNVa32()
// 	addFNVa32(h, uint32(byte(len(fields))))
// 	for i := range fields {
// 		f := &fields[i]
// 		addFNVa32(h, uint32(byte(len(f.Label))))
// 		for j := 0; 0 <= j && j < len(f.Label); j++ {
// 			h = addFNVa32(h, uint32(f.Label[j]))
// 		}
// 		addFNVa32(h, uint32(byte(len(f.Value))))
// 		for j := 0; 0 <= j && j < len(f.Value); j++ {
// 			h = addFNVa32(h, uint32(f.Value[j]))
// 		}
// 	}
// 	return h
// }

// func (fields Fields) Match(q url.Values) (Fields, bool) {
// 	if len(q) == 0 {
// 		return fields, true
// 	}
// 	n := 0
// 	for i := range fields {
// 		f := &fields[i]
// 		values, ok := q[f.Label]
// 		if ok && (len(values) == 0 || indexOf(values, f.Value) != -1) {
// 			fields[n] = *f
// 			n++
// 		}
// 	}
// 	if n == len(q) {
// 		return fields[:n], true
// 	}
// 	return fields, false
// }

// type Row struct {
// 	Fields Fields
// 	Count  int64
// }
// type Chunk struct {
// 	Time Timestamp
// 	Rows []Row
// }

// type Bucket struct {
// 	Event  string
// 	Fields Fields
// 	Data   []DataPoint
// }

// func (b *Bucket) Add(t, n int64) {
// 	for i := range b.Data {
// 		d := &b.Data[i]
// 		if d.Timestamp == t {
// 			d.Value += n
// 			return
// 		}
// 	}
// 	b.Data = append(b.Data, DataPoint{t, n})
// }

// type Buckets []Bucket

// func (buckets Buckets) Merge(chunk Chunk) Buckets {
// 	for i := range chunk.Rows {
// 		row := &chunk.Rows[i]
// 		buckets = buckets.merge(chunk.Time, row)
// 	}
// 	return buckets
// }
// func (buckets Buckets) merge(t Timestamp, row *Row) Buckets {
// 	for i := range buckets {
// 		b := &buckets[i]
// 		if b.Fields.Equal(row.Fields) {
// 			b.Add(int64(t), row.Count)
// 			return buckets
// 		}
// 	}
// 	return append(buckets, Bucket{
// 		Fields: row.Fields,
// 		Data: []DataPoint{{
// 			Timestamp: int64(t),
// 			Value:     row.Count,
// 		}},
// 	})
// }

// type MapReducer interface {
// 	Mapper
// 	Reducer
// }

// type Reducer interface {
// 	Reduce(rows []Row, row Row) []Row
// }

// type Mapper interface {
// 	Map(row Row) (Row, bool)
// }

// type MapperFunc func(row Row) (Row, bool)

// func (f MapperFunc) Map(row Row) (Row, bool) {
// 	return f(row)
// }

// type Mappers []Mapper

// func (ms Mappers) Map(row Row) (Row, bool) {
// 	ok := true
// 	for _, m := range ms {
// 		if m != nil {
// 			if row, ok = m.Map(row); !ok {
// 				break
// 			}
// 		}
// 	}
// 	return row, ok
// }

// type GroupMapper []string

// func (g GroupMapper) Map(row Row) (Row, bool) {
// 	if len(g) == 0 {
// 		return row, true
// 	}
// 	row.Fields = row.Fields.Filter(g...)
// 	return row, len(row.Fields) == len(g)
// }

// type IdentityMapper struct{}

// func (IdentityMapper) Map(row Row) (Row, bool) {
// 	return row, true
// }

// type QueryMapper url.Values

// func (q QueryMapper) Map(row Row) (Row, bool) {
// 	if len(q) == 0 {
// 		return row, true
// 	}
// 	if fields, ok := row.Fields.Match(url.Values(q)); ok {
// 		row.Fields = fields
// 		return row, true
// 	}
// 	return row, false
// }

// type SumReducer struct{}

// func (SumReducer) Reduce(rows []Row, row Row) []Row {
// 	for i := range rows {
// 		r := &rows[i]
// 		if r.Fields.Equal(row.Fields) {
// 			r.Count += row.Count
// 			return rows
// 		}
// 	}
// 	return append(rows, row)
// }

// type ValueFrequencyReducer struct{}

// func (ValueFrequencyReducer) Reduce(rows []Row, row Row) []Row {
// 	sum := SumReducer{}
// 	for i := range row.Fields {
// 		if j := i + 1; 0 <= j && j < len(row.Fields) {
// 			rows = sum.Reduce(rows, Row{
// 				Fields: row.Fields[i:j],
// 				Count:  row.Count,
// 			})
// 		}
// 	}
// 	return rows
// }

// type labelMap map[string]uint64

// type Store struct {
// 	*badger.DB
// }

// const (
// 	fieldsKeySize = 10
// )

// var (
// 	keyNextID      = []byte(`__nextID`)
// 	keyPrefixField = []byte("f:")
// 	keyPrefixEvent = []byte("e:")
// )

// type fieldsKey [fieldsKeySize]byte
// type fieldsID uint64

// func (id fieldsID) Hash() uint32 {
// 	return uint32(id >> 32)
// }

// func prefixSize(name string) int {
// 	return len(name) + len(keyPrefixEvent)
// }

// func appendEventKey(key []byte, t time.Time, name string) []byte {
// 	key = append(key, keyPrefixEvent...)
// 	key = append(key, name...)
// 	buf := [8]byte{}
// 	binary.BigEndian.PutUint64(buf[:], uint64(t.UnixNano()))
// 	return append(key, buf[:]...)
// }

// func (s *Store) fieldsID(h uint32, value []byte) (id uint64, err error) {
// 	const maxRetries = 2
// 	retries := 0
// 	for retries < maxRetries {
// 		id = 0
// 		err = s.Update(func(txn *badger.Txn) error {
// 			key := newFieldsKey(uint64(h) << 32)
// 			seek := key[:]
// 			prefix := key[:6]
// 			iter := txn.NewIterator(badger.IteratorOptions{
// 				PrefetchValues: false,
// 			})
// 			n := uint32(0)
// 			defer iter.Close()
// 			for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
// 				item := iter.Item()
// 				v, err := item.Value()
// 				if err != nil {
// 					return err
// 				}
// 				if bytes.Equal(v, value) {
// 					if k := item.Key(); len(k) == fieldsKeySize {
// 						id = binary.BigEndian.Uint64(k[2:])
// 						return nil
// 					}
// 				}
// 				n++
// 			}
// 			id |= uint64(n)
// 			binary.BigEndian.PutUint32(key[6:], n)
// 			return txn.Set(key[:], value)
// 		})
// 		if err == nil || err != badger.ErrConflict {
// 			return
// 		}
// 		retries++
// 	}
// 	return
// }

// func (s *Store) Store(t time.Time, desc *Desc, counters Snapshot, ttl time.Duration) (err error) {
// 	data := make([]byte, 0, 4096)
// 	scratch := make([]byte, 0, 256)
// 	labels := desc.Labels()
// 	name := desc.Name()
// 	return s.Update(func(txn *badger.Txn) error {
// 		buf := [16]byte{}
// 		for _, c := range counters {
// 			scratch, h := ZipFields(scratch[:0], labels, c.Values)
// 			id, err := s.fieldsID(h, scratch)
// 			if err != nil {
// 				return err
// 			}
// 			binary.BigEndian.PutUint64(buf[:], id)
// 			binary.BigEndian.PutUint64(buf[8:], uint64(c.Count))
// 			data = append(data, buf[:]...)
// 		}
// 		scratch = appendEventKey(scratch[:0], t, name)
// 		if ttl > 0 {
// 			return txn.SetWithTTL(scratch, data, ttl)
// 		}
// 		return txn.Set(scratch, data)
// 	})
// }

// // func (s *Store) Fields(id fieldsID) (Fields, error) {
// // 	s.lock.RLock()
// // 	fields := s.cache[uint64(id)]
// // 	s.lock.RUnlock()
// // 	if fields != nil {
// // 		return fields, nil
// // 	}
// // 	s.lock.Lock()
// // 	if fields = s.cache[uint64(id)]; fields != nil {
// // 		s.lock.Unlock()
// // 		return fields, nil
// // 	}
// // 	defer s.lock.Unlock()
// // 	raw, err := s.loadFieldsRaw(id)
// // 	if err != nil {
// // 		return nil, err
// // 	}
// // 	fields = FieldsFromString(raw)
// // 	s.cache[uint64(id)] = fields
// // 	return fields, nil
// // }

// // func (s *Store) loadFieldsRaw(id fieldsID) (raw string, err error) {
// // 	err = s.View(func(txn *badger.Txn) error {
// // 		key := id.Key()
// // 		item, err := txn.Get(key[:])
// // 		if err != nil {
// // 			return err
// // 		}
// // 		data, err := item.Value()
// // 		if err != nil {
// // 			return err
// // 		}
// // 		raw = string(data)
// // 		return nil
// // 	})
// // 	return
// // }

// // func (fdb *Store) find(h uint32, fields Fields) (uint32, bool) {
// // 	ids := fdb.lookup[h]
// // 	for _, id := range ids {
// // 		if f, ok := fdb.index[id]; ok && f.Equal(fields) {
// // 			return id, true
// // 		}
// // 	}
// // 	return 0, false
// // }

// // func (fdb *Store) load() error {
// // 	fdb.lock.Lock()
// // 	defer fdb.lock.Unlock()
// // 	fdb.index = make(map[uint32]Fields)
// // 	fdb.lookup = make(map[uint32][]uint32)
// // 	return fdb.View(func(txn *badger.Txn) error {
// // 		i := txn.NewIterator(badger.DefaultIteratorOptions)
// // 		defer i.Close()
// // 		h := uint32(0)
// // 		id := fieldsID(0)
// // 		key := id.Key()
// // 		seek := key[:]
// // 		prefix := key[:2]
// // 		for i.Seek(seek); i.ValidForPrefix(prefix); i.Next() {
// // 			item := i.Item()
// // 			if k := item.Key(); len(k) == fieldsKeySize {
// // 				v, err := item.Value()
// // 				if err != nil {
// // 					return err
// // 				}
// // 				h = binary.BigEndian.Uint32(k[2:])
// // 				id = binary.BigEndian.Uint32(k[6:])
// // 				fdb.index[id] = FieldsFromBytes(v)
// // 				fdb.lookup[h] = append(fdb.lookup[h], id)
// // 			}
// // 		}
// // 		fdb.nextID = id + 1
// // 		return nil
// // 	})
// // }

// // func (fdb *Store) Fields(id uint64) (f Fields) {
// // 	fdb.lock.RLock()
// // 	f = fdb.index[id]
// // 	fdb.lock.RUnlock()
// // 	return
// // }

// // func (fdb *Store) ID(fields Fields) (uint64, error) {
// // 	h := fields.Hash()
// // 	fdb.lock.RLock()
// // 	id, ok := fdb.find(h, fields)
// // 	fdb.lock.RUnlock()
// // 	if ok {
// // 		return id, nil
// // 	}
// // 	fdb.lock.Lock()
// // 	defer fdb.lock.Unlock()
// // 	if id, ok = fdb.find(h, fields); ok {
// // 		return id, nil
// // 	}
// // 	err := fdb.Update(func(txn *badger.Txn) error {
// // 		key := newFieldKey(h, fdb.nextID)
// // 		value := fields.AppendTo(nil)
// // 		return txn.Set(key[:], value)
// // 	})
// // 	if err != nil {
// // 		return 0, err
// // 	}
// // 	fdb.nextID++
// // 	fdb.index[id] = fields
// // 	fdb.lookup[h] = append(fdb.lookup[h], id)
// // 	return id, nil
// // }

// // // func (db *Store) labelMap(event string, labels []string, s Snapshot) ([]labelMap, error) {
// // // 	keys := make([]string, len(labels))
// // // 	m := make([]labelMap, len(labels))
// // // 	for i, label := range labels {
// // // 		keys[i] = fmt.Sprintf("event:%s:label:%s:index", event, label)
// // // 		m[i] = labelMap{}
// // // 	}
// // // 	db.DB.View(func(txn *badger.Txn) error {
// // // 	})
// // // 	for i := range s {
// // // 		c := &s[i]
// // // 		for i := range labels {
// // // 			var v string
// // // 			if 0 <= i && i < len(c.values) {
// // // 				v = c.values[i]
// // // 			}
// // // 			if v == "" {
// // // 				continue
// // // 			}
// // // 			mm := m[i]
// // // 			if _, ok := mm[v]; !ok {
// // // 				cmd := cmdHADDNX.Eval(pipe, []string{keys[i]}, v)
// // // 				mm[v] = uint64(len(replies))
// // // 				replies = append(replies, cmd)
// // // 			}
// // // 			m[i] = mm
// // // 		}
// // // 	}
// // // 	_, err := pipe.Exec()
// // // }
