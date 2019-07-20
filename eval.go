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

// Evaler runs eval queries
type Evaler interface {
	Eval(ctx context.Context, query string) ([]interface{}, error)
}

type evaler struct {
	Scanner
}

// Eval implements Evaler interface
func (q *evaler) Eval(ctx context.Context, query string) ([]interface{}, error) {
	p := new(Parser)
	if err := p.Reset(query); err != nil {
		return nil, err
	}
	results, err := q.Scan(ctx, p.Queries()...)
	if err != nil {
		return nil, err
	}
	return p.Eval(results)
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
