// Package evbadger provides an evdb backend using dgraph.io/badger key-value store
package evbadger

import (
	"net/url"
	"strings"

	"github.com/alxarch/evdb"
	"github.com/dgraph-io/badger/v2"
	errors "golang.org/x/xerrors"
)

type opener struct{}

func (o opener) Open(configURL string, events ...string) (evdb.DB, error) {
	options, err := o.parseURL(configURL)
	if err != nil {
		return nil, err
	}
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	return Open(db, events...)
}

const urlScheme = "badger"

func init() {
	evdb.Register(urlScheme, opener{})
}

func (opener) parseURL(optionsURL string) (options badger.Options, err error) {
	u, err := url.Parse(optionsURL)
	if err != nil {
		return
	}
	if u.Scheme != urlScheme {
		err = errors.Errorf(`Invalid scheme %q != %q`, u.Scheme, urlScheme)
		return
	}
	q := u.Query()
	options = badger.DefaultOptions
	// options.Logger = nil
	options.Dir = u.Path
	if options.ValueDir = q.Get("value-dir"); options.ValueDir == "" {
		options.ValueDir = options.Dir
	}

	if _, ok := q["read-only"]; ok {
		switch strings.ToLower(q.Get("read-only")) {
		case "false", "off", "no", "0":
		default:
			options.ReadOnly = true
		}
	}
	return
}
