package meter2

type Metric interface {
	Values() []string
	Count() int64
	Add(n int64) int64
	Set(n int64) int64
	Descriptor
}
