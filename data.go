package evdb

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
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
	if math.IsNaN(p.Value) {
		data = append(data, "null"...)
	} else {
		data = strconv.AppendFloat(data, p.Value, 'f', -1, 64)
	}
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
			return s[:i+1]
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

// First returns a pointer to the first point
func (s DataPoints) First() *DataPoint {
	return s.Get(0)
}

// Last returns a pointer to the last point
func (s DataPoints) Last() *DataPoint {
	if n := len(s) - 1; 0 <= n && n < len(s) {
		return &s[n]
	}
	return nil
}

// Add adds a point
func (s DataPoints) Add(t int64, v float64) DataPoints {
	for i := len(s) - 1; 0 <= i && i < len(s); i-- {
		d := &s[i]
		if d.Timestamp == t {
			d.Value += v
			return s
		}
	}
	return append(s, DataPoint{
		Timestamp: t,
		Value:     v,
	})
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

func (s DataPoints) Reset() DataPoints {
	for i := range s {
		s[i] = DataPoint{}
	}
	return s[:0]
}

func (s DataPoints) Fill(v float64) {
	for i := range s {
		d := &s[i]
		d.Value = v
	}
}

func BlankData(t *TimeRange, v float64) DataPoints {
	start, end, step := t.Start.Unix(), t.End.Unix(), int64(t.Step/time.Second)
	return fillData(v, start, end, step)
}

func fillData(v float64, start, end, step int64) (data DataPoints) {
	if step < 1 {
		step = 1
	}
	n := (end - start) / step
	if n < 0 {
		return
	}
	data = make([]DataPoint, n+1)
	for i := range data {
		ts := start + int64(i)*step
		data[i] = DataPoint{
			Timestamp: ts,
			Value:     v,
		}
	}
	return
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
