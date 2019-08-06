package evdb

import (
	"fmt"
	"net/url"
	"sync"

	errors "golang.org/x/xerrors"
)

// DB handles scan and store requests
type DB interface {
	Scanner
	Store
	Close() error
}

// Opener opens new DB instances from a URL configuration
type Opener interface {
	Open(configURL string) (DB, error)
}

var (
	openerMu sync.Mutex
	openers  = map[string]Opener{}
)

// Register registers a DB opener for a URL scheme
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

// Open opens a DB using a configURL and applies options
func Open(configURL string, options ...Option) (DB, error) {
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
	db, err := opener.Open(configURL)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		db, err = option.apply(db)
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

// Option is a DB option
type Option interface {
	apply(db DB) (DB, error)
}

type readOnlyDB struct {
	DB
}

func (ro *readOnlyDB) Storer(string) (Storer, error) {
	return nil, errors.Errorf("Readonly DB")
}

// ReadOnly disables the Store interface of a DB
func ReadOnly() Option {
	return fnOption(func(db DB) (DB, error) {
		return &readOnlyDB{db}, nil
	})
}
