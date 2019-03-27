package meter

import (
	"sync"
)

type Counter struct {
	Count  int64    `json:"n"`
	Values []string `json:"v,omitempty"`
}

func (c *Counter) Match(values []string) bool {
	if len(c.Values) == len(values) {
		values = values[:len(c.Values)]
		for i := range c.Values {
			if c.Values[i] == values[i] {
				continue
			}
			return false
		}
		return true
	}
	return false
}

type Snapshot []Counter

func (s Snapshot) FilterZero() Snapshot {
	j := 0
	for i := range s {
		c := &s[i]
		if c.Count == 0 {
			continue
		}
		s[j] = *c
		j++
	}
	return s[:j]
}

func (s Snapshot) Reset() Snapshot {
	for i := range s {
		s[i] = Counter{}
	}
	return s[:0]
}

var snapshotPool sync.Pool

func getSnapshot() Snapshot {
	if x := snapshotPool.Get(); x != nil {
		return x.(Snapshot)
	}
	const minSnapshotSize = 64
	return make([]Counter, 0, minSnapshotSize)
}

func putSnapshot(s Snapshot) {
	snapshotPool.Put(s.Reset())
}
