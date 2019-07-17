package meter

import (
	"encoding/json"
	"math"
	"strconv"
)

// DataPoint is a time/value pair
type DataPoint struct {
	Timestamp int64
	Value     float64
}

// AppendJSON appends JSON to a buffer
func (p DataPoint) AppendJSON(data []byte) []byte {
	data = append(data, '[')
	data = strconv.AppendInt(data, p.Timestamp, 10)
	data = append(data, ',')
	data = strconv.AppendFloat(data, p.Value, 'f', -1, 64)
	data = append(data, ']')
	return data
}

// MarshalJSON implements json.Marshaler interface
func (p DataPoint) MarshalJSON() ([]byte, error) {
	return p.AppendJSON(nil), nil
}

// UnmarshalJSON implements json.Unmarshaler interface
func (p *DataPoint) UnmarshalJSON(data []byte) error {
	value := [2]json.Number{}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	p.Timestamp, err = value[0].Int64()
	if err != nil {
		return err
	}
	p.Value, err = value[1].Float64()
	if err != nil {
		return err
	}
	return nil
}

// DataPoints is a collection of DataPoints
type DataPoints []DataPoint

// Copy creates a copy of s
func (s DataPoints) Copy() DataPoints {
	if s == nil {
		return nil
	}
	cp := make([]DataPoint, len(s))
	copy(cp, s)
	return cp
}

// First returns a pointer to the first point
func (s DataPoints) First() *DataPoint {
	return s.Get(0)
}

// Get returns a pointer to the i-th point
func (s DataPoints) Get(i int) *DataPoint {
	if 0 <= i && i < len(s) {
		return &s[i]
	}
	return nil
}

// Last returns a pointer to the last point
func (s DataPoints) Last() *DataPoint {
	if n := len(s) - 1; 0 <= n && n < len(s) {
		return &s[n]
	}
	return nil
}

// MergePoint adds a point using Merger
func (s DataPoints) MergePoint(m Merger, t int64, v float64) DataPoints {
	for i := len(s) - 1; 0 <= i && i < len(s); i-- {
		d := &s[i]
		if d.Timestamp == t {
			d.Value = m.Merge(d.Value, v)
			return s
		}
	}
	return append(s, DataPoint{
		Timestamp: t,
		Value:     m.Merge(math.NaN(), v),
	})
}

// ValueAt searches for the value at a specific time
func (s DataPoints) ValueAt(ts int64) float64 {
	for i := range s {
		if d := &s[i]; d.Timestamp == ts {
			return d.Value
		}
	}
	return math.NaN()
}

// IndexOf returns the index of tm in the collection of data points
func (s DataPoints) IndexOf(ts int64) int {
	for i := range s {
		if d := &s[i]; d.Timestamp == ts {
			return i
		}
	}
	return -1
}

// Avg returns the average value of all points
func (s DataPoints) Avg() float64 {
	return s.Sum() / float64(len(s))
}

// Sum returns the sum of the values of all points
func (s DataPoints) Sum() (sum float64) {
	for i := range s {
		d := &s[i]
		sum += d.Value
	}
	return
}

// Len implements sort.Interface
func (s DataPoints) Len() int {
	return len(s)
}

// Swap implements sort.Interface
func (s DataPoints) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less implements sort.Interface
func (s DataPoints) Less(i, j int) bool {
	return s[i].Timestamp < s[j].Timestamp
}

// AppendJSON appends points as JSON to a buffer
func (s DataPoints) AppendJSON(data []byte) []byte {
	if s == nil {
		return append(data, `null`...)
	}
	data = append(data, '[')
	for i := range s {
		p := &s[i]
		if i != 0 {
			data = append(data, ',')
		}
		data = p.AppendJSON(data)
	}

	data = append(data, ']')
	return data
}

// MarshalJSON implements json.Marshal interface
func (s DataPoints) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte(`null`), nil
	}
	size := 32*len(s) + 4
	return s.AppendJSON(make([]byte, 0, size)), nil
}

// MergeScalar merges a value to all datapoints using m
func (s DataPoints) MergeScalar(m Merger, v float64) DataPoints {
	for i := range s {
		d := &s[i]
		d.Value = m.Merge(d.Value, v)
	}
	return s
}

// Merge merges other datapoints using m
func (s DataPoints) Merge(m Merger, data ...DataPoint) DataPoints {
	for i := range data {
		d := &data[i]
		s = s.MergePoint(m, d.Timestamp, d.Value)
	}
	return s
}

// MergeConsecutiveDuplicates merges sequential points with equal timestamps
func (s DataPoints) MergeConsecutiveDuplicates(m Merger) DataPoints {
	if m == nil {
		m = MergeSum{}
	}
	distinct := s[:0]
	for i := 0; 0 <= i && i < len(s); i++ {
		p := &s[i]
		for j := i; 0 <= j && j < len(s); j++ {
			next := &s[j]
			if next.Timestamp == p.Timestamp {
				p.Value = m.Merge(p.Value, next.Value)
				i++
			} else {
				break
			}
		}
		distinct = append(distinct, *p)
	}
	return distinct

}

// Merger provides a method to merge values
type Merger interface {
	Merge(a, b float64) float64
}

// MergeFunc is a closure Merger
type MergeFunc func(a, b float64) float64

// Merge implements Merger
func (f MergeFunc) Merge(a, b float64) float64 {
	return f(a, b)
}

// MergeSum adds values
type MergeSum struct{}

// Merge implements Merger
func (MergeSum) Merge(a, b float64) float64 {
	if math.IsNaN(a) {
		return b
	}
	if math.IsNaN(b) {
		return a
	}
	return a + b
}

// MergeDiff subtracts values
type MergeDiff struct{}

// Merge implements Merger
func (MergeDiff) Merge(a, b float64) float64 {
	if math.IsNaN(a) {
		return -b
	}
	if math.IsNaN(b) {
		return a
	}
	return a - b
}

// MergeDiv divides values
type MergeDiv struct{}

// Merge implements Merger
func (MergeDiv) Merge(a, b float64) float64 {
	if math.IsNaN(a) {
		return 0
	}
	return a / b
}

// MergeMul multiplies values
type MergeMul struct{}

// Merge implements Merger
func (MergeMul) Merge(a, b float64) float64 {
	if math.IsNaN(a) {
		return 0
	}
	if math.IsNaN(b) {
		return 0
	}
	return a * b
}

// MergeMax keeps max value
type MergeMax struct{}

// Merge implements Merger
func (MergeMax) Merge(a, b float64) float64 {
	if a > 0 {
		return a
	}
	return b
}

// MergeMin keeps min value
type MergeMin struct{}

// Merge implements Merger
func (MergeMin) Merge(a, b float64) float64 {
	if a < b {
		return a
	}
	return b

}

// MergeAvg merges values to their average
type MergeAvg struct {
	n     int
	total float64
}

// Merge implements Merger
func (avg *MergeAvg) Merge(_, b float64) float64 {
	avg.n++
	if !math.IsNaN(b) {
		avg.total += b
	}
	return avg.total / float64(avg.n)
}

var mergeSum = MergeSum{}
