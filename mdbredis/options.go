package mdbredis

import (
	"net/url"
	"strconv"

	redis "github.com/alxarch/fastredis"
)

type Options struct {
	Redis       redis.PoolOptions
	ScanSize    int64
	KeyPrefix   string
	Resolutions []Resolution
}

func ParseURL(rawurl string) (o Options, err error) {
	o.Redis, err = redis.ParseURL(rawurl)
	if err != nil {
		return
	}
	u, _ := url.Parse(rawurl)
	q := u.Query()
	o.ScanSize, _ = strconv.ParseInt(q.Get("scan-size"), 10, 64)
	o.KeyPrefix = q.Get("key-prefix")
	// TODO: parse resolutiuons from URL
	o.Resolutions = []Resolution{
		ResolutionHourly.WithTTL(Weekly),
		ResolutionDaily.WithTTL(Yearly),
		ResolutionWeekly.WithTTL(10 * Yearly),
	}
	return
}
