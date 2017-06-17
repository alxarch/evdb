package meter

import (
	"math"
	"sync"
	"sync/atomic"
)

type Batch map[string]float64

type CounterValue struct {
	bits uint64
}

func (v *CounterValue) Set(f float64) float64 {
	oldBits := atomic.SwapUint64(&v.bits, math.Float64bits(f))
	return math.Float64frombits(oldBits)
}
func (v *CounterValue) Get() float64 {
	return math.Float64frombits(atomic.LoadUint64(&v.bits))
}

func (v *CounterValue) Inc(d float64) {
	// Optimistic increment from prometheus
	for {
		oldBits := atomic.LoadUint64(&v.bits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + d)
		if atomic.CompareAndSwapUint64(&v.bits, oldBits, newBits) {
			return
		}
	}
}

type Counters struct {
	values map[string]*CounterValue
	mu     sync.RWMutex
}

func (c *Counters) increment(d string, n float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.values[d]; ok {
		v.Inc(n)
	} else {
		v = &CounterValue{}
		v.Inc(n)
		c.values[d] = v
	}

}
func (c *Counters) Increment(key string, n float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.values[key]; ok {
		v.Inc(n)
	} else {
		c.mu.RUnlock()
		c.increment(key, n)
		c.mu.RLock()
	}
}
func NewCounters() *Counters {
	return &Counters{
		values: make(map[string]*CounterValue),
	}
}

func (c *Counters) Batch() Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]float64)
	for desc, val := range c.values {
		out[desc] = val.Get()
	}
	return out
}

func (c *Counters) Flush() Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]float64)
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
