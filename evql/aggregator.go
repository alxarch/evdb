package evql

import (
	"math"
	"strings"

	"github.com/alxarch/evdb"
)

// Aggregator aggregates values
type Aggregator interface {
	Aggregate(acc, v float64) float64
	Reset()
	Zero() float64
}

type aggMin struct{}

func (aggMin) Zero() float64 {
	return math.Inf(1)
}

func (aggMin) Reset() {}

func (aggMin) Aggregate(acc, v float64) float64 {
	if acc < v {
		return acc
	}
	return v
}

type aggMax struct{}

func (aggMax) Reset() {}

func (aggMax) Zero() float64 {
	return math.Inf(-1)
}
func (aggMax) Aggregate(acc, v float64) float64 {
	if acc > v {
		return acc
	}
	return v
}

type aggSum struct{}

func (aggSum) Reset() {}

func (aggSum) Zero() float64 {
	return 0
}
func (aggSum) Aggregate(acc, v float64) float64 {
	if math.IsNaN(v) {
		return acc
	}
	return acc + v
}

type aggCount struct{}

func (aggCount) Reset() {}

func (aggCount) Zero() float64 {
	return 0
}
func (aggCount) Aggregate(acc, _ float64) float64 {
	return acc + 1
}

type aggAvg struct {
	sum, count float64
}

func (a *aggAvg) Reset() {
	*a = aggAvg{}
}

func (*aggAvg) Zero() float64 {
	return math.NaN()
}

func (a *aggAvg) Aggregate(_, v float64) float64 {
	if !math.IsNaN(v) {
		a.sum += v
		a.count++
	}
	return a.sum / a.count
}

// NewAggregator creates a new Aggregator
func NewAggregator(name string) Aggregator {
	switch strings.ToLower(name) {
	case "count":
		return aggSum{}
	case "sum":
		return aggSum{}
	case "avg":
		return new(aggAvg)
	case "min":
		return aggMin{}
	case "max":
		return aggMax{}
	default:
		return nil
	}
}

// BlankAggregator returns a separate Aggregator instance
func BlankAggregator(agg Aggregator) Aggregator {
	if agg == nil {
		return aggSum{}
	}
	if _, isAvg := agg.(*aggAvg); isAvg {
		return new(aggAvg)
	}
	return agg
}

func AggregateData(s evdb.DataPoints, agg Aggregator) float64 {
	v := agg.Zero()
	for i := range s {
		d := &s[i]
		v = agg.Aggregate(v, d.Value)
	}
	return v
}
