package meter2

type Metric interface {
	Values() []string
	Count() int64
	Add(n int64) int64
	Set(n int64) int64
	Type() MetricType
	Descriptor
}

type MetricType uint8

const (
	_ MetricType = iota
	MetricTypeIncrement
	MetricTypeUpdateOnce
	MetricTypeUpdate
)
