package meter

type MetricType uint8

const (
	_                    MetricType = iota
	MetricTypeIncrement             // Increment value on store
	MetricTypeUpdateOnce            // Update value once on store
	MetricTypeUpdate                // Update value on store
)

// type Metric struct {
// 	Counter
// 	desc *Desc
// }

// func (m Metric) Describe() *Desc {
// 	return m.desc
// }

// func (m Metric) Collect(ch chan<- Metric) {
// 	ch <- m
// }
