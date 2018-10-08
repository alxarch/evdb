package meter

import (
	"strings"
	"sync/atomic"
)

type Counter interface {
	Count() int64
	Add(n int64) int64
	AppendValues([]string) []string
	Match(values []string) bool
	Set(n int64) int64
}
type CounterLocal struct {
	n      int64
	values string
}

func (c *CounterLocal) Add(n int64) int64 {
	c.n += n
	return c.n
}

func (c *CounterLocal) AppendValues(values []string) []string {
	return appendRawValues(values, c.values)
}

func (c *CounterLocal) Count() int64 {
	return c.n
}
func (c *CounterLocal) Set(n int64) {
	c.n = n
}

type LocalCounters struct {
	index    map[uint64][]int
	counters []CounterLocal
}

func (cc *LocalCounters) WithLabelValues(values ...string) *CounterLocal {
	h := hashNew()
	for i, v := range values {
		if i > 0 {
			h = hashAddByte(h, LabelSeparator)
		}
		for j := 0; 0 <= j && j < len(v); j++ {
			h = hashAddByte(h, v[j])
		}
	}
	for _, id := range cc.index[h] {
		if 0 <= id && id < len(cc.counters) {
			if c := &cc.counters[id]; matchRawValues(c.values, values) {
				return c
			}
		}
	}
	id := len(cc.counters)
	cc.counters = append(cc.counters, CounterLocal{
		values: joinValues(values),
	})
	if cc.index == nil {
		cc.index = make(map[uint64][]int)
	}
	cc.index[h] = append(cc.index[h], id)
	return cc.get(id)
}

func (cc *LocalCounters) Len() int {
	return len(cc.counters)
}

func (cc *LocalCounters) MergeInto(e *Event) {
	desc := e.Describe()
	values := make([]string, 0, len(desc.Labels()))
	for i := range cc.counters {
		c := &cc.counters[i]
		values = appendRawValues(values, c.values)
		e.WithLabelValues(values...).Add(c.n)
	}
}
func (cc *LocalCounters) get(i int) *CounterLocal {
	if 0 <= i && i < len(cc.counters) {
		return &cc.counters[i]
	}
	return nil
}

func (cc *LocalCounters) Reset() {
	for h, ids := range cc.index {
		cc.index[h] = ids[:0]
	}
	j := 0
	for i := range cc.counters {
		c := &cc.counters[i]
		if c.n == 0 {
			continue
		}
		c.n = 0
		if 0 <= j && j < len(cc.counters) {
			cc.counters[j].values = c.values
			h := rawValuesHash(c.values)
			cc.index[h] = append(cc.index[h], j)
			j++
		}
	}
	if 0 <= j && j < len(cc.counters) {
		tmp := cc.counters[j:]
		for i := range tmp {
			c := &tmp[i]
			c.values = ""
		}
		cc.counters = cc.counters[:j]
	}
	for h, ids := range cc.index {
		if len(ids) == 0 {
			delete(cc.index, h)
		}
	}
}

func valuesHash2(values []string) (h uint64) {
	h = hashNew()
	for i, v := range values {
		if i > 0 {
			h = hashAddByte(h, LabelSeparator)
		}
		for j := 0; 0 <= j && j < len(v); j++ {
			h = hashAddByte(h, v[j])
		}
	}
	return
}
func rawValuesHash(v string) (h uint64) {
	h = hashNew()
	for i := 0; 0 <= i && i < len(v); i++ {
		h = hashAddByte(h, v[i])
	}
	return
}

type CounterAtomic struct {
	n      int64
	values string
}

var _ Counter = &CounterAtomic{}

// func newSafeCounter(values []string) *safeCounter {
// 	return &safeCounter{0, joinValues(values)}
// }

func (c *CounterAtomic) AppendValues(values []string) []string {
	return appendRawValues(values, c.values)
}
func (c *CounterAtomic) Match(values []string) bool {
	return matchRawValues(c.values, values)
}

func (c *CounterAtomic) Count() int64 {
	return atomic.LoadInt64(&c.n)
}
func (c *CounterAtomic) Add(n int64) int64 {
	return atomic.AddInt64(&c.n, n)
}
func (c *CounterAtomic) Set(n int64) int64 {
	return atomic.SwapInt64(&c.n, n)
}

func SplitValues(s string) []string {
	return appendRawValues(nil, s)
}

func appendRawValues(v []string, s string) []string {
	for i := 0; 0 <= i && i < len(s); i++ {
		if s[i] == LabelSeparator {
			v = append(v, s[:i])
			if i++; 0 <= i && i < len(s) {
				s = s[i:]
			} else {
				break
			}
		}
	}
	if s != "" {
		return append(v, s)
	}
	return v
}

func joinValues(values []string) string {
	const valueSize = 64
	s := strings.Builder{}
	s.Grow(len(values) * valueSize)
	for i, v := range values {
		if i > 0 {
			s.WriteByte(LabelSeparator)
		}
		s.WriteString(v)
	}
	return s.String()
}

func matchRawValues(s string, values []string) bool {
	for _, v := range values {
		if len(s) > len(v) {
			if s[:len(v)] == v {
				s = s[len(v)+1:]
				continue
			}
		} else if s == v {
			return true
		}
		return false
	}
	return true
}
