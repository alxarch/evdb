package meter

import (
	"encoding/binary"
	"sort"
	"sync"

	"github.com/dgraph-io/badger"
)

const (
	prefixByteValue = 0
	prefixByteEvent = 1
)

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

func compaction(db *badger.DB, start, end int64) error {
	txn := db.NewTransaction(true)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	iter := txn.NewIterator(opt)
	defer iter.Close()
	seek := appendKey(nil, prefixByteEvent, uint64(start))
	cc := getCompactionBuffer()
	defer putCompactionBuffer(cc)

	for iter.Seek(seek); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(key)
		if !ok || ts >= end {
			break
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		cc.Read(v)
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
		if err := txn.Set(seek, value); err != nil {
			return err
		}
		return txn.Commit(nil)
	}
	return nil
}

// type RetentionPolicy map[time.Duration]time.Duration

// func (p RetentionPolicy) Cascade(now time.Time) []TimeRange {
// 	rules := make([]RetentionRule, 0, len(p))
// 	for step, period := range p {
// 		rules = append(rules, RetentionRule{step, period})
// 	}
// 	return RetentionRules(rules).Normalize().Steps(now)
// }

// type RetentionRule struct {
// 	Step   time.Duration
// 	Period time.Duration
// }
// type RetentionRules []RetentionRule

// func (rules RetentionRules) Normalize() RetentionRules {
// 	sort.SliceStable(rules, func(i, j int) bool {
// 		a, b := &rules[i], &rules[j]
// 		switch {
// 		case a.Step < b.Step:
// 			return true
// 		case a.Step > b.Step:
// 			return false
// 		default:
// 			return a.Period > b.Period
// 		}
// 	})
// 	out := rules[:0]
// 	last := RetentionRule{
// 		Step: time.Second,
// 	}
// 	for _, r := range rules {
// 		r.Step = r.Step.Truncate(last.Step)
// 		r.Period = r.Period.Truncate(r.Step)
// 		if r.Step <= last.Step || r.Period <= last.Period {
// 			continue
// 		}
// 		if r.Step%last.Step == 0 {
// 			out = append(out, r)
// 		} else {
// 			out[len(out)-1].Period = r.Period
// 		}
// 		last = r
// 	}
// 	return out
// }

// func (rules RetentionRules) get(i int) *RetentionRule {
// 	if 0 <= i && i < len(rules) {
// 		return &rules[i]
// 	}
// 	return nil
// }

// func (rules RetentionRules) Steps(now time.Time) []TimeRange {
// 	result := make([]TimeRange, len(rules))
// 	for i := range result {
// 		rule := rules.get(i)
// 		tr := &result[i]
// 		tr.Start = now.Add(-rule.Period)
// 		tr.Step = rule.Step
// 		if i == 0 {
// 			tr.End = now
// 		} else {
// 			tr.End = result[i-1].Start
// 		}
// 	}
// 	return result
// }

// type Compaction struct {
// 	Rules      RetentionPolicy
// 	NumWorkers int
// }
