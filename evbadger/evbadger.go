// Package evbadger provides an evdb backend using dgraph.io/badger key-value store
package evbadger

import (
	"log"
	"net/url"
	"strings"

	"github.com/alxarch/evdb"
	"github.com/dgraph-io/badger/v2"
	errors "golang.org/x/xerrors"
)

type opener struct{}

func (opener) Open(configURL string, events ...string) (evdb.DB, error) {
	options, err := ParseURL(configURL)
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
	o := opener{}
	if err := evdb.Register(urlScheme, o); err != nil {
		log.Fatal("Failed to register db opener", err)
	}
}

// ParseURL parses config url from options
func ParseURL(optionsURL string) (options badger.Options, err error) {
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
	options.Logger = nil
	if _, ok := q["debug"]; ok {
		switch strings.ToLower(q.Get("debug")) {
		case "true", "on", "yes", "1", "":
			options.Logger = newDebugLogger()
		}
	}
	if options.Logger == nil {
		options.Logger = newLogger()
	}
	return
}
