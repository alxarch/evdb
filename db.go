package meter

import (
	"fmt"
	"net/url"
	"sync"
)

type DB interface {
	Querier
	Storer(event string) Storer
	Close() error
}

type Opener interface {
	Open(configURL string) (DB, error)
}

var (
	openerMu sync.Mutex
	openers  = map[string]Opener{}
)

func Register(scheme string, op Opener) {
	openerMu.Lock()
	defer openerMu.Unlock()
	_, alreadyRegistered := openers[scheme]
	if alreadyRegistered {
		panic(fmt.Errorf(`Scheme %q already registered`, scheme))
	}
	openers[scheme] = op
}
func Open(configURL string) (DB, error) {
	u, err := url.Parse(configURL)
	if err != nil {
		return nil, err
	}
	scheme := u.Scheme
	openerMu.Lock()
	opener := openers[scheme]
	openerMu.Unlock()
	if opener == nil {
		return nil, fmt.Errorf("Scheme %q not registered", scheme)
	}
	return opener.Open(configURL)
}
