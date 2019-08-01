package evql

import (
	"go/token"
	"math"

	"github.com/alxarch/evdb"
	errors "golang.org/x/xerrors"
)

type merger interface {
	Merge(a, b float64) float64
}

type mergeAdd struct{}

func (mergeAdd) Merge(a, b float64) float64 {
	if math.IsNaN(a) {
		return b
	}
	if math.IsNaN(b) {
		return a
	}
	return a + b
}

type mergeSub struct{}

// Merge implements Merger
func (mergeSub) Merge(a, b float64) float64 {
	// if math.IsNaN(a) {
	// 	return -b
	// }
	// if math.IsNaN(b) {
	// 	return a
	// }
	return a - b
}

type mergeDiv struct{}

// Merge implements Merger
func (mergeDiv) Merge(a, b float64) float64 {
	// if math.IsNaN(a) {
	// 	return 0
	// }
	return a / b
}

type mergeMul struct{}

// Merge implements Merger
func (mergeMul) Merge(a, b float64) float64 {
	// if math.IsNaN(a) {
	// 	return 0
	// }
	// if math.IsNaN(b) {
	// 	return 0
	// }
	return a * b
}

func newMerger(op token.Token) merger {
	switch op {
	case token.ADD:
		return mergeAdd{}
	case token.SUB:
		return mergeSub{}
	case token.MUL:
		return mergeMul{}
	case token.QUO:
		return mergeDiv{}
	default:
		return nil
	}

}

// // MergeFunc is a closure Merger
// type MergeFunc func(a, b float64) float64

// // Merge implements Merger
// func (f MergeFunc) Merge(a, b float64) float64 {
// 	return f(a, b)
// }

// func mergeResults(m Merger, x, y interface{}) (interface{}, error) {
// 	if x, ok := x.(Value); ok {
// 		return x.Merge(m, y, false)
// 	}
// 	if y, ok := y.(Value); ok {
// 		return y.Merge(m, x, true)
// 	}
// 	if x, ok := x.([]interface{}); ok {
// 		var out []interface{}
// 		for _, x := range x {
// 			v, err := mergeResults(m, x, y)
// 			if err != nil {
// 				return nil, err
// 			}
// 			out = append(out, v)
// 		}
// 		return out, nil
// 	}
// 	if y, ok := y.([]interface{}); ok {
// 		var out []interface{}
// 		for _, y := range y {
// 			v, err := mergeResults(m, x, y)
// 			if err != nil {
// 				return nil, err
// 			}
// 			out = append(out, v)
// 		}
// 		return out, nil
// 	}
// 	return nil, errors.New("Invalid operands")

// }

func mergeData(m merger, s, v evdb.DataPoints) error {
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
