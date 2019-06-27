package meter

import (
	"context"
	"errors"
	"net/url"
	"sync"
)

type DB interface {
	Query(ctx context.Context, q *Query, events ...string) (Results, error)
	Storer
	Close() error
}

type Opener interface {
	Open(url string, events ...string) (DB, error)
}

var (
	openMu  sync.Mutex
	openers = map[string]Opener{}
)

func Register(scheme string, op Opener) {
	openMu.Lock()
	defer openMu.Unlock()
	openers[scheme] = op
}
func Open(configURL string, events ...string) (DB, error) {
	c, err := url.Parse(configURL)
	if err != nil {
		return nil, err
	}
	openMu.Lock()
	o, ok := openers[c.Scheme]
	openMu.Unlock()
	if !ok {
		return nil, errors.New(`Unregistered storage type`)
	}
	return o.Open(configURL, events...)
}
