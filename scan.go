package meter

import (
	"encoding/binary"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
)

func (db *EventDB) scan(start, end time.Time, step time.Duration, scan scanResults) (err error) {
	var (
		seek       = db.EventKey(end)
		prefixSize = db.prefixSize()
		prefix     = seek[:prefixSize]
		minT       = start.Unix()
		ts         int64
		s          = int64(NormalizeStep(step) / time.Second)
		item       *badger.Item
		fields     Fields
		r          *ScanResult
		id         uint64
		ok         bool
		n          int64
		v          []byte
	)
	txn := db.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(badger.IteratorOptions{
		Reverse: true,
	})
	defer iter.Close()

	for iter.Seek(seek); iter.ValidForPrefix(prefix); iter.Next() {
		item = iter.Item()
		ts = readEventKeyTimestamp(prefixSize, item.Key())
		if ts < minT {
			return
		}
		v, err = item.Value()
		if err == nil {
			return
		}
		ts = stepTS(ts, s)
		for ; len(v) >= 16; v = v[16:] {
			id = binary.BigEndian.Uint64(v)
			r, ok = scan.Find(id)
			if !ok {
				fields, err = db.loadFields(id)
				if err != nil {
					return
				}
				r = scan.FindOrCreate(id, fields)
			}
			if r == nil {
				continue
			}
			n = int64(binary.BigEndian.Uint64(v[8:]))
			if step >= 0 {
				r.Add(ts, n)
			} else {
				r.Total += n
			}
		}
	}
	return
}

type scanResults interface {
	Find(id uint64) (*ScanResult, bool)
	FindOrCreate(id uint64, fields Fields) *ScanResult
	Results() []*ScanResult
}

type eventScan struct {
	*ScanResult
	index map[uint64]struct{}
	match Fields
}

func newEventScan(q *Query) *eventScan {
	s := new(eventScan)
	s.ScanResult = blankResult()
	s.index = make(map[uint64]struct{})
	s.match = q.Match.Sorted()
	return s
}

func (s *eventScan) Find(id uint64) (r *ScanResult, ok bool) {
	_, ok = s.index[id]
	return s.ScanResult, ok
}

func (s *eventScan) Results() []*ScanResult {
	if s.ScanResult == nil {
		return nil
	}
	return []*ScanResult{s.ScanResult}
}

func (s *eventScan) FindOrCreate(id uint64, fields Fields) *ScanResult {
	if !fields.MatchSorted(s.match) {
		return nil
	}
	s.index[id] = struct{}{}
	return s.ScanResult
}

type groupEventScan struct {
	event   string
	results []*ScanResult
	index   map[uint64]*ScanResult
	match   Fields
	group   []string
	empty   string
	grouped Fields
}

func newGroupEventScan(q *Query) *groupEventScan {
	g := new(groupEventScan)
	g.index = make(map[uint64]*ScanResult)
	g.match = q.Match.Sorted()
	g.empty = q.EmptyValue
	g.group = make([]string, 0, len(q.Group))
	g.group = appendDistinct(g.group, q.Group...)
	sort.Strings(g.group)
	g.grouped = Fields(make([]Field, len(g.group)))
	return g
}
func (g *groupEventScan) Find(id uint64) (r *ScanResult, ok bool) {
	r, ok = g.index[id]
	return
}

func (g *groupEventScan) groupFields(dst, src Fields) Fields {
	var (
		value, label string
		i, j         int
		f            *Field
	)

	for i, label = range g.group {
	seek:
		for j = range src {
			f = &src[j]
			switch strings.Compare(f.Label, label) {
			case 0:
				src = src[j:]
				value = f.Value
				break seek
			case -1:
				for _, label = range g.group[i:] {
					dst = append(dst, Field{
						Label: label,
						Value: g.empty,
					})
				}
				return dst
			}
		}
		if value == "" {
			value = g.empty
		}
		dst = append(dst, Field{
			Label: label,
			Value: value,
		})
	}
	return dst
}

func (g *groupEventScan) FindOrCreate(id uint64, fields Fields) (r *ScanResult) {
	if !fields.MatchSorted(g.match) {
		g.index[id] = nil
		return
	}
	fields = g.groupFields(g.grouped[:0], fields)
	for _, r = range g.results {
		if r.Fields.Equal(fields) {
			return r
		}
	}
	r = blankResult()
	r.Fields = fields
	r.Event = g.event
	g.results = append(g.results, r)
	g.index[id] = r
	return
}
func (g *groupEventScan) Results() []*ScanResult {
	return g.results
}

func readEventKeyTimestamp(prefix int, k []byte) int64 {
	if len(k) > prefix {
		if k = k[prefix:]; len(k) == 8 {
			return int64(binary.BigEndian.Uint64(k))
		}
	}
	return 0
}
