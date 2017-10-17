package meter

import "sync/atomic"

type Metric interface {
	Counter
	Descriptor
}

type MetricType uint8

const (
	_                    MetricType = iota
	MetricTypeIncrement             // Increment value on store
	MetricTypeUpdateOnce            // Update value once on store
	MetricTypeUpdate                // Update value on store
)

type metric struct {
	n      *int64
	desc   *Desc
	values []string
}

var _ Metric = &metric{}

func NewMetric(n int64, values []string, desc *Desc) Metric {
	return &metric{&n, desc, values}
}

func (m metric) Add(n int64) int64 {
	return atomic.AddInt64(m.n, n)
}
func (m metric) Count() int64 {
	return atomic.LoadInt64(m.n)
}

func (m metric) Set(n int64) int64 {
	return atomic.SwapInt64(m.n, n)
}
func (m metric) Values() []string {
	return m.values
}
func (m metric) Describe() *Desc {
	return m.desc
}

func (m metric) Collect(ch chan<- Metric) {
	ch <- m
}
