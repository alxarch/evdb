package meter

import (
	"context"
)

type DB interface {
	Query(ctx context.Context, q *Query, events ...string) (Results, error)
	Storer
	Close() error
}
