package meter

import (
	"errors"
	"go/token"
	"math"
)

// Merger provides a method to merge values
type Merger interface {
	Merge(a, b float64) float64
}

type Value interface {
	Merge(m Merger, x interface{}, rev bool) (interface{}, error)
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

func mergeResults(m Merger, x, y interface{}) (interface{}, error) {
	if x, ok := x.(Value); ok {
		return x.Merge(m, y, false)
	}
	if y, ok := y.(Value); ok {
		return y.Merge(m, x, true)
	}
	if x, ok := x.([]interface{}); ok {
		var out []interface{}
		for _, x := range x {
			v, err := mergeResults(m, x, y)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	if y, ok := y.([]interface{}); ok {
		var out []interface{}
		for _, y := range y {
			v, err := mergeResults(m, x, y)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, errors.New("Invalid operands")

}

func mergeOp(op token.Token) Merger {
	switch op {
	case token.ADD:
		return MergeSum{}
	case token.SUB:
		return MergeDiff{}
	case token.MUL:
		return MergeMul{}
	case token.QUO:
		return MergeDiv{}
	default:
		return nil
	}

}
