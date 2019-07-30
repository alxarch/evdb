package evdb

import (
	"fmt"
	"net/url"
	"sync"

	errors "golang.org/x/xerrors"
)

type DB interface {
	Scanner
	Store
	Close() error
}

type Opener interface {
	Open(configURL string, events ...string) (DB, error)
}

var (
	openerMu sync.Mutex
	openers  = map[string]Opener{}
)

func Register(scheme string, op Opener) error {
	openerMu.Lock()
	defer openerMu.Unlock()
	_, alreadyRegistered := openers[scheme]
	if alreadyRegistered {
		return errors.Errorf(`Scheme %q already registered`, scheme)
	}
	openers[scheme] = op
	return nil
}

func Open(configURL string, events ...string) (DB, error) {
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
	return opener.Open(configURL, events...)
}
