package meter

import "sync/atomic"

type Counter interface {
	Count() int64
	Add(n int64) int64
	Values() []string
	Set(n int64) int64
}

type safeCounter struct {
	n      int64
	values []string
}

var _ Counter = &safeCounter{}

func newSafeCounter(values ...string) *safeCounter {
	return &safeCounter{0, values}
}

func (c *safeCounter) Count() int64 {
	return atomic.LoadInt64(&c.n)
}
func (c *safeCounter) Add(n int64) int64 {
	return atomic.AddInt64(&c.n, n)
}
func (c *safeCounter) Values() []string {
	return c.values
}
func (c *safeCounter) Set(n int64) int64 {
	return atomic.SwapInt64(&c.n, n)
}

type unsafeCounter struct {
	n      int64
	values []string
}

var _ Counter = &unsafeCounter{}

func newUnsafeCounter(values ...string) *unsafeCounter {
	return &unsafeCounter{0, values}
}

func (c *unsafeCounter) Count() int64 {
	return c.n
}
func (c *unsafeCounter) Add(n int64) int64 {
	c.n += n
	return c.n
}
func (c *unsafeCounter) Values() []string {
	return c.values
}
func (c *unsafeCounter) Set(n int64) (p int64) {
	p = c.n
	c.n = n
	return
}

type counterMetric struct {
	Counter
	*Desc
}

type Counters struct {
	counters map[uint64][]Counter
}

func NewCounters() *Counters {
	return &Counters{
		counters: make(map[uint64][]Counter),
	}
}

func (cc *Counters) MergeInto(e Event) {
	for _, counters := range cc.counters {
		for i := 0; i < len(counters); i++ {
			c := counters[i]
			e.WithLabelValues(c.Values()...).Add(c.Count())
		}
	}
}

func (cc *Counters) Reset() {
	for h, counters := range cc.counters {
		j := 0
		for i := 0; i < len(counters); i++ {
			c := counters[i]
			if n := c.Set(0); n == 0 {
				continue
			}
			counters[j] = c
			j++
		}
		if j == 0 {
			delete(cc.counters, h)
		} else {
			cc.counters[h] = counters[:j]
		}
	}
}

func (cs *Counters) WithLabelValues(values ...string) (c Counter) {
	h := valuesHash(values)
	if cs.counters == nil {
		cs.counters = make(map[uint64][]Counter)
	}
	counters := cs.counters[h]
	for i := 0; i < len(counters); i++ {
		if c = counters[i]; matchValues(c.Values(), values) {
			return
		}
	}
	c = newUnsafeCounter(values...)
	cs.counters[h] = append(cs.counters[h], c)
	return
}

func matchValues(a, b []string) bool {
	for i, val := range a {
		if b[i] != val {
			return false
		}
	}
	return true

}
