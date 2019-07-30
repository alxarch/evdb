// Package evredis provides an evdb backend using Redis
package evredis

import (
	"net/url"
	"strconv"

	"github.com/alxarch/evdb"
)

type opener struct{}

var _ evdb.Opener = opener{}

// Open implements evdb.Opener interface
func (o opener) Open(configURL string, events ...string) (evdb.DB, error) {
	options, err := o.parseURL(configURL)
	if err != nil {
		return nil, err
	}

	return Open(options, events...)

}

const urlScheme = "badger"

func init() {
	evdb.Register(urlScheme, opener{})
}

// Options are options for a DB
type Options struct {
	Redis       string
	ScanSize    int64
	KeyPrefix   string
	Resolutions []Resolution
}

// ParseURL parses options from a URL
func (opener) parseURL(rawurl string) (o Options, err error) {
	u, _ := url.Parse(rawurl)
	q := u.Query()
	o.ScanSize, _ = strconv.ParseInt(q.Get("scan-size"), 10, 32)
	o.KeyPrefix = q.Get("key-prefix")
	// TODO: parse resolutions from URL
	o.Resolutions = []Resolution{
		ResolutionHourly.WithTTL(Weekly),
		ResolutionDaily.WithTTL(Yearly),
		ResolutionWeekly.WithTTL(10 * Yearly),
	}
	o.Redis = u.String()
	return
}
