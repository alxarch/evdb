package evql

import (
	"context"

	"github.com/alxarch/evdb"
)

// Querier executes meterql queries
type Querier interface {
	Query(ctx context.Context, t evdb.TimeRange, q string) ([]interface{}, error)
}

type querier struct {
	evdb.Scanner
}

// Query implements Querier interface
func (s *querier) Query(ctx context.Context, t evdb.TimeRange, q string) ([]interface{}, error) {
	p := new(Parser)
	if err := p.Reset(q); err != nil {
		return nil, err
	}
	r, err := s.Scanner.Scan(ctx, p.Queries(t)...)
	if err != nil {
		return nil, err
	}
	return p.Eval(nil, t, r), nil
}

// NewQuerier turns a Scanner to a Querier
func NewQuerier(scanner evdb.Scanner) Querier {
	if scanner == nil {
		return nil
	}
	if e, ok := scanner.(*querier); ok {
		return e
	}
	return &querier{scanner}

}
