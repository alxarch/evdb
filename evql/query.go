package evql

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"

	db "github.com/alxarch/evdb"
)

// Execer executes evql queries
type Execer interface {
	Exec(ctx context.Context, t db.TimeRange, q string) ([]db.Results, error)
}

type execer struct {
	db.Scanner
}

// Exec implements Execer interface
func (s *execer) Exec(ctx context.Context, t db.TimeRange, query string) ([]db.Results, error) {
	q, err := Parse(query)
	if err != nil {
		return nil, err
	}
	r, err := s.Scanner.Scan(ctx, q.Queries(t)...)
	if err != nil {
		return nil, err
	}
	return q.Eval(nil, t, r), nil
}

// NewExecer turns a Scanner to an Execer
func NewExecer(scanner db.Scanner) Execer {
	if scanner == nil {
		return nil
	}
	if e, ok := scanner.(*execer); ok {
		return e
	}
	return &execer{scanner}

}

type Query struct {
	root blockNode
}

func Parse(query string) (*Query, error) {
	fset := token.NewFileSet()
	// Wrap query body
	query = fmt.Sprintf(`func(){%s}`, query)
	exp, err := parser.ParseExprFrom(fset, "", []byte(query), 0)
	if err != nil {
		return nil, err
	}

	root, err := parseRoot(exp)
	if err != nil {
		if e, ok := err.(*nodeError); ok {
			return nil, e.ParseError(fset)
		}
		return nil, err
	}
	nameResults(fset, root)
	q := Query{
		root: root,
	}
	return &q, nil
}

func (q *Query) Queries(t db.TimeRange) []db.ScanQuery {
	return nodeQueries(nil, &t, q.root)
}

// Eval executes the query against some results
func (q *Query) Eval(out []db.Results, t db.TimeRange, results db.Results) []db.Results {
	if q.root != nil {
		out = q.root.Eval(out, &t, results)
	}
	return out
}
