package meter

import (
	"context"
	"math"
)

// func (q *Eval) Fatal(exp ast.Expr, err error) error {
// 	panic(q.Error(exp, err))
// }

// func (q *Eval) Fatalf(exp ast.Expr, msg string, args ...interface{}) {
// 	panic(q.Errorf(exp, msg, args...))
// }

// Evaler runs eval queries
type Evaler interface {
	Eval(ctx context.Context, q EvalQuery) ([]interface{}, error)
}

type EvalQuery struct {
	TimeRange
	Query string
}

type evaler struct {
	Scanner
}

type Aggregator interface {
	Aggregate(acc, v float64) float64
	Zero() float64
}

type aggMin struct{}

func (aggMin) Zero() float64 {
	return math.Inf(1)
}
func (aggMin) Aggregate(acc, v float64) float64 {
	if acc < v {
		return acc
	}
	return v
}

type aggMax struct{}

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

func (aggSum) Zero() float64 {
	return 0
}
func (aggSum) Aggregate(acc, v float64) float64 {
	return acc + v
}

type aggCount struct{}

func (aggCount) Zero() float64 {
	return 0
}
func (aggCount) Aggregate(acc, _ float64) float64 {
	return acc + 1
}

type aggAvg struct {
	sum, count float64
}

func (*aggAvg) Zero() float64 {
	return math.NaN()
}

func (a *aggAvg) Aggregate(acc, v float64) float64 {
	if math.IsNaN(acc) {
		*a = aggAvg{
			sum:   v,
			count: 1,
		}
		return v
	}
	a.sum += v
	a.count++
	return a.sum / a.count
}

// type scanKind int

// const (
// 	_ scanKind = iota
// 	kindRoot
// 	kindBlock
// 	kindResult
// 	kindEventScan
// 	kindCombine
// 	kindReduce
// 	kindBinaryOp
// )

// type scanExpr struct {
// 	ScanQuery
// 	Group    []string
// 	Kind     scanKind
// 	Parent   *scanExpr
// 	Agg      Aggregator
// 	Op       Merger
// 	Children []scanExpr
// 	node     ast.Node
// }

// func (s *scanExpr) Child(node ast.Node, kind scanKind) *scanExpr {
// 	s.Children = append(s.Children, scanExpr{
// 		Parent: s,
// 		node:   node,
// 		Agg:    s.Agg,
// 		Group:  s.Group,
// 		ScanQuery: ScanQuery{
// 			TimeRange: s.TimeRange,
// 			Match:     s.Match,
// 		},
// 	})
// 	return &s.Children[len(s.Children)-1]

// }

func aggResults(agg Aggregator, results ...*Result) (out *Result) {
	if _, ok := agg.(*aggAvg); ok {
		agg = new(aggAvg)
	}
	if len(results) > 0 {
		out, results = results[0], results[1:]
		if len(results) == 0 {
			return
		}
		for i := range out.Data {
			d := &out.Data[i]
			v := agg.Aggregate(agg.Zero(), d.Value)
			for _, r := range results {
				if 0 <= i && i < len(r.Data) {
					p := &r.Data[i]
					v = agg.Aggregate(v, p.Value)
				} else {
					v = agg.Aggregate(v, math.NaN())
				}
			}
			d.Value = v
		}
	}
	return
}

var _ = `func(){
	goals{goal: bar|baz|bar}
	*match{foo: bar|baz|buz}
	*group{foo, bar}
	{

	}
	<-sum{
		[...]avg{
			foo, bar, baz,
		}[-20:m],
		foo{foo: &bad}[-10:m]/bar,
	}
	<-raw{foo}
	{
		*match{foo: bar|baz|buz}
		*group{foo, bar}
		<-sum{[...]avg{foo, bar, baz * !avg{foo[-10:m]}}
	}
	<-avg{foo}
}
_sum{track/win}%match{

}
	_{foo: bar},
	track / win

}
_.match{

}|sum{
	foo / bar
}
[...]debug{ // Return raw result without eval
	max<<[...]avg{
		foo{bar: baz} / avg<<foo{bar: baz}[-10:m],

	}|group(foo, bar, baz)|gr,
	{

	}.group(foo, bar, baz)
	avg(a)
	[...]min{

	}[-10:m]
	>>avg
	[...]avg{
		// S1 [[t0, v0] ... [tN, vN]] -> [[t0, avg(v0, ..., vN)], [t1, avg(v0, ..., vN)]
		foo{bar: baz}[-10:m].avg(),
		// S2 = [[t0, v0] ... [tN, vN]] -> [[t0, avg(v0, ..., vN)], [t1, avg(v0, ..., vN)]
		(foo{bar: baz} + bar{baz: foo}).group(foo, bar, baz),
	}|min(), // avg(S1, S2) = [t0, avg(s1, s2)] -> float
}
[...]avg{
	foo{bar: baz}[-10:m]|group(foo, bar, baz)|avg() // [[t0, v0] ... [tN, vN]] -> [[t0, avg(v0, ..., vN)], [t1, avg(v0, ..., vN)]
}
`

type ResultOp interface {
	RewriteResult(x interface{}) (interface{}, error)
}
type ResultRewriter interface {
	RewriteResult(result interface{}) (interface{}, error)
}

type Rewriter interface {
	Rewrite(q *ScanQuery, args ...interface{}) (*ScanQuery, error)
}

// Eval implements Evaler interface
func (e *evaler) Eval(ctx context.Context, q EvalQuery) ([]interface{}, error) {
	p := new(Parser)
	if err := p.Reset(q.Query, q.TimeRange); err != nil {
		return nil, err
	}
	results, err := e.Scan(ctx, p.Queries()...)
	if err != nil {
		return nil, err
	}
	return p.Eval(nil, results), nil
}

// NewEvaler turns a Querier to an Evaler
func NewEvaler(scanner Scanner) Evaler {
	if scanner == nil {
		return nil
	}
	if e, ok := scanner.(*evaler); ok {
		return e
	}
	return &evaler{scanner}

}

type aggDebug struct{}

func (aggDebug) Reset() {}
func (aggDebug) Aggregate(_ float64) float64 {
	return math.NaN()

}

type aggRaw struct{}

func (aggRaw) Reset() {}
func (aggRaw) Aggregate(_ float64) float64 {
	return math.NaN()

}
