// Package evredis provides an evdb backend using Redis
package evredis

import (
	"log"
	"net/url"
	"strconv"

	"github.com/alxarch/evdb"
)

type opener struct{}

var _ evdb.Opener = opener{}

// Open implements evdb.Opener interface
func (opener) Open(configURL string) (evdb.DB, error) {
	options, err := ParseURL(configURL)
	if err != nil {
		return nil, err
	}

	return Open(options)

}

const urlScheme = "redis"

func init() {
	o := opener{}
	if err := evdb.Register(urlScheme, o); err != nil {
		log.Fatal("Failed to register db opener", err)
	}
}

// Config is configuration for a Redis DB
type Config struct {
	Redis       string
	ScanSize    int64
	KeyPrefix   string
	Resolutions []Resolution
}

// ParseURL parses config from a URL
func ParseURL(configURL string) (o Config, err error) {
	u, _ := url.Parse(configURL)
	q := u.Query()
	o.ScanSize, _ = strconv.ParseInt(q.Get("scan-size"), 10, 32)
	delete(q, "scan-size")
	o.KeyPrefix = q.Get("key-prefix")
	delete(q, "key-prefix")
	// TODO: parse resolutions from URL
	o.Resolutions = []Resolution{
		ResolutionHourly.WithTTL(Weekly),
		ResolutionDaily.WithTTL(Yearly),
		ResolutionWeekly.WithTTL(10 * Yearly),
	}
	u.RawQuery = q.Encode()
	o.Redis = u.String()
	return
}
