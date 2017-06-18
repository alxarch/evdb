package meter

import (
	"sync"
	"sync/atomic"
)

type Batch map[string]int64

type Counter struct {
	value int64
}

func (v *Counter) Set(n int64) int64 {
	return atomic.SwapInt64(&v.value, n)
}
func (v *Counter) Get() int64 {
	return atomic.LoadInt64(&v.value)
}

func (v *Counter) Inc(d int64) int64 {
	return atomic.AddInt64(&v.value, d)
}

type Counters struct {
	values map[string]*Counter
	mu     sync.RWMutex
}

func (c *Counters) increment(d string, n int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.values[d]; ok {
		return v.Inc(n)
	} else {
		v = &Counter{}
		total := v.Inc(n)
		c.values[d] = v
		return total
	}

}
func (c *Counters) Increment(key string, n int64) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.values[key]; ok {
		return v.Inc(n)
	} else {
		c.mu.RUnlock()
		n = c.increment(key, n)
		c.mu.RLock()
		return n
	}
}
func NewCounters() *Counters {
	return &Counters{
		values: make(map[string]*Counter),
	}
}

func (c *Counters) Batch() Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64)
	for desc, val := range c.values {
		out[desc] = val.Get()
	}
	return out
}

func (c *Counters) Flush() Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64)
	for desc, val := range c.values {
		out[desc] = val.Set(0.0)
	}
	return out
}

func (c *Counters) BatchIncrement(b Batch) {
	for key, val := range b {
		c.Increment(key, val)
	}
}
