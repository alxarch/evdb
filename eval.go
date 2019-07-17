package meter

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strings"
)

// Eval is a query expression of simple arthmetic between events
type Eval struct {
	fset   *token.FileSet
	exp    ast.Expr
	events []string
}

func (e *Eval) String() string {
	w := new(strings.Builder)
	printer.Fprint(w, e.fset, e.exp)
	return w.String()
}

func (e *Eval) sprint(exp ast.Expr) string {
	w := new(strings.Builder)
	printer.Fprint(w, e.fset, exp)
	return w.String()
}
func (e *Eval) parse(exp ast.Expr) error {
	switch exp := exp.(type) {
	case *ast.Ident:
		e.events = appendDistinct(e.events, exp.Name)
		return nil
	case *ast.BinaryExpr:
		if m := mergeOp(exp.Op); m == nil {
			return fmt.Errorf("Unsupported operator %q", exp.Op)
		}
		if err := e.parse(exp.X); err != nil {
			return err
		}
		if err := e.parse(exp.Y); err != nil {
			return nil
		}
		return nil
	case *ast.ParenExpr:
		return e.parse(exp.X)
	default:
		return fmt.Errorf("Invalid expression %q", e.sprint(exp))
	}
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

// Result calculates the result of an expression
func (e *Eval) Result(results map[string]ScanResult) (r Result, err error) {
	r.Event = e.String()
	r.ScanResult, err = e.eval(e.exp, results)
	return
}

func (e *Eval) eval(exp ast.Expr, results map[string]ScanResult) (ScanResult, error) {
	switch exp := exp.(type) {
	case *ast.Ident:
		return results[exp.Name], nil
	case *ast.ParenExpr:
		return e.eval(exp.X, results)
	case *ast.BinaryExpr:
		m := mergeOp(exp.Op)
		if m == nil {
			return ScanResult{}, fmt.Errorf("Unsupported operator %q", exp.Op)
		}
		x, err := e.eval(exp.X, results)
		if err != nil {
			return ScanResult{}, err
		}
		y, err := e.eval(exp.Y, results)
		if err != nil {
			return ScanResult{}, err
		}
		x.Merge(&y, m)
		return x, nil
	default:
		return ScanResult{}, fmt.Errorf("Invalid expression %q", exp)
	}
}

// Events returns the distinct event names referenced in the query expression
func (e *Eval) Events() []string {
	return e.events
}

// ParseEval parses a query expression
func ParseEval(exp string) (*Eval, error) {
	eval := Eval{
		fset: token.NewFileSet(),
	}
	e, err := parser.ParseExprFrom(eval.fset, exp, []byte(exp), 0)
	if err != nil {
		return nil, err
	}
	eval.exp = e
	if err := eval.parse(e); err != nil {
		return nil, err
	}
	return &eval, nil
}

// Evaler runs eval queries
type Evaler interface {
	Eval(ctx context.Context, q Query, exp ...string) (Results, error)
}

type evalQuerier struct {
	querier Querier
}

// Eval implements Evaler interface
func (eq *evalQuerier) Eval(ctx context.Context, q Query, expressions ...string) (Results, error) {
	var events []string
	var evals []*Eval
	for _, exp := range expressions {
		eval, err := ParseEval(exp)
		if err != nil {
			return nil, err
		}
		events = appendDistinct(events, eval.Events()...)
		evals = append(evals, eval)
	}
	scanResults, err := eq.querier.Query(ctx, q, events...)
	if err != nil {
		return nil, err
	}
	results := scanResults.ByEvent()
	evalResults := make([]Result, 0, len(evals))
	for _, eval := range evals {
		r, err := eval.Result(results)
		if err != nil {
			return nil, err
		}
		evalResults = append(evalResults, r)
	}
	return evalResults, nil
}

// QueryEvaler turns a Querier to an Evaler
func QueryEvaler(querier Querier) Evaler {
	if querier == nil {
		return nil
	}
	if e, ok := querier.(Evaler); ok {
		return e
	}
	return &evalQuerier{querier}

}
