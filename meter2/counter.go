package meter2

import "sync/atomic"

type Counter struct {
	n      int64
	values []string
	desc   *Desc
}

var _ Metric = &Counter{}

func NewCounter(d *Desc, values ...string) *Counter {
	return &Counter{desc: d, values: values}
}

func (v *Counter) matches(values []string) bool {
	for i, val := range values {
		if v.values[i] != val {
			return false
		}
	}
	return true
}

func (v *Counter) Add(n int64) int64 {
	n = atomic.AddInt64(&v.n, n)
	return n
}
func (v *Counter) Count() int64 {
	return atomic.LoadInt64(&v.n)
}
func (v *Counter) Set(n int64) int64 {
	return atomic.SwapInt64(&v.n, n)
}
func (v *Counter) Values() []string {
	return v.values
}
func (v *Counter) Describe() *Desc {
	return v.desc
}
