package mdbredis

import (
	"net/url"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type Options struct {
	Redis       *redis.Options
	ScanSize    int64
	KeyPrefix   string
	Resolutions []Resolution
}

func ParseURL(rawurl string) (o Options, err error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return
	}
	q := u.Query()
	u.RawQuery = ""
	o.Redis, err = redis.ParseURL(u.String())
	if err != nil {
		return
	}
	if v, ok := q["read-timeout"]; ok && len(v) > 0 {
		o.Redis.ReadTimeout, _ = time.ParseDuration(v[0])
	}
	if v, ok := q["write-timeout"]; ok && len(v) > 0 {
		o.Redis.WriteTimeout, _ = time.ParseDuration(v[0])
	}
	if v, ok := q["pool-size"]; ok && len(v) > 0 {
		o.Redis.PoolSize, _ = strconv.Atoi(v[0])
	}
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
