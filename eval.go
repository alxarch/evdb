package meter

import (
	"context"
)

// func (q *Eval) Fatal(exp ast.Expr, err error) error {
// 	panic(q.Error(exp, err))
// }

// func (q *Eval) Fatalf(exp ast.Expr, msg string, args ...interface{}) {
// 	panic(q.Errorf(exp, msg, args...))
// }

type querier struct {
	Scanner
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

// Query implements Querier interface
func (e *querier) Query(ctx context.Context, t TimeRange, q string) ([]interface{}, error) {
	p := new(Parser)
	if err := p.Reset(q); err != nil {
		return nil, err
	}
	r, err := e.Scan(ctx, p.Queries(t)...)
	if err != nil {
		return nil, err
	}
	return p.Eval(nil, t, r), nil
}

// NewQuerier turns a Querier to an Evaler
func NewQuerier(scanner Scanner) Querier {
	if scanner == nil {
		return nil
	}
	if e, ok := scanner.(*querier); ok {
		return e
	}
	return &querier{scanner}

}
