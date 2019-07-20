package meter

import (
	"encoding/json"
	"errors"
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

func (s DataPoints) SeekRight(ts int64) DataPoints {
	for i := len(s) - 1; 0 <= i && i < len(s); i-- {
		if d := &s[i]; d.Timestamp <= ts {
			return s[:i]
		}
	}
	return nil

}
func (s DataPoints) SeekLeft(ts int64) DataPoints {
	for i := range s {
		if d := &s[i]; d.Timestamp >= ts {
			return s[i:]
		}
	}
	return nil
}

func (s DataPoints) MergeVectorR(m Merger, v DataPoints) error {
	if len(v) == len(s) {
		v = v[:len(s)]
		for i := range s {
			p0, p1 := &s[i], &v[i]
			p0.Value = m.Merge(p1.Value, p0.Value)
		}
		return nil
	}
	return errors.New("Invalid operand size")
}
func (s DataPoints) MergeVector(m Merger, v DataPoints) error {
	if len(v) == len(s) {
		v = v[:len(s)]
		for i := range s {
			p0, p1 := &s[i], &v[i]
			p0.Value = m.Merge(p0.Value, p1.Value)
		}
		return nil
	}
	return errors.New("Invalid operand size")
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

// MergeValue merges a value to all datapoints using s as base
func (s DataPoints) MergeValue(m Merger, v float64) DataPoints {
	for i := range s {
		d := &s[i]
		d.Value = m.Merge(d.Value, v)
	}
	return s
}

// MergeValueR merges a value to all datapoints using v as base
func (s DataPoints) MergeValueR(m Merger, v float64) DataPoints {
	for i := range s {
		d := &s[i]
		d.Value = m.Merge(v, d.Value)
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

var mergeSum = MergeSum{}

func (s DataPoints) Aggregate(m Merger, data ...DataPoint) DataPoints {
	for i := range data {
		d := &data[i]
		s = s.MergePoint(m, d.Timestamp, d.Value)
	}
	return s
}

func (s DataPoints) Pick(data DataPoints) DataPoints {
	for i := range data {
		d := &data[i]
		for j := range s {
			p := &s[j]
			if p.Timestamp == d.Timestamp {
				p.Value = d.Value
			}
		}
	}
	return s
}

func (s DataPoints) Slice(start, end int64) DataPoints {
	if s == nil {
		return nil
	}
	if s = s.SeekLeft(start); s == nil {
		return nil
	}
	if s = s.SeekRight(end); s == nil {
		return nil
	}
	return s

}
