package meter2

import "sync/atomic"

type Counter struct {
	n      int64
	values []string
	t      MetricType
	desc   *Desc
}

var _ Metric = &Counter{}
var _ Collector = &Counter{}
var _ Descriptor = &Counter{}

func NewCounter(d *Desc, values ...string) *Counter {
	return &Counter{t: MetricTypeIncrement, desc: d, values: values}
}
func newCounter(d *Desc, t MetricType, values ...string) *Counter {
	return &Counter{t: t, desc: d, values: values}
}

func (v *Counter) matches(values []string) bool {
	if v == nil {
		return false
	}
	for i, val := range values {
		if v.values[i] != val {
			return false
		}
	}
	return true
}

func (v *Counter) Add(n int64) int64 {
	if v == nil {
		return 0
	}
	n = atomic.AddInt64(&v.n, n)
	return n
}
func (v *Counter) Count() int64 {
	if v == nil {
		return 0
	}
	return atomic.LoadInt64(&v.n)
}
func (v *Counter) Set(n int64) int64 {
	if v == nil {
		return 0
	}
	return atomic.SwapInt64(&v.n, n)
}
func (v *Counter) Values() []string {
	if v == nil {
		return nil
	}
	return v.values
}
func (v *Counter) Describe() *Desc {
	if v == nil {
		return nil
	}
	return v.desc
}
func (v *Counter) Type() MetricType {
	return v.t
}

func (v *Counter) Collect(ch chan<- Metric) {
	if v != nil {
		ch <- v
	}
}
