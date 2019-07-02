package mdbredis

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Options struct {
	RedisURL    string
	ScanSize    int64
	KeyPrefix   string
	Resolutions []Resolution
}

func redisPool(rawurl string) (*redis.Pool, error) {
	if rawurl == "" {
		pool := redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", ":6379")
			},
		}
		return &pool, nil
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf(`Invalid URL scheme %q`, u.Scheme)
	}
	q := u.Query()
	var dialOptions []redis.DialOption
	if v, ok := q["read-timeout"]; ok && len(v) > 0 {
		timeout, _ := time.ParseDuration(v[0])
		dialOptions = append(dialOptions, redis.DialReadTimeout(timeout))
	}
	if v, ok := q["write-timeout"]; ok && len(v) > 0 {
		d, _ := time.ParseDuration(v[0])
		dialOptions = append(dialOptions, redis.DialWriteTimeout(d))
	}
	u.RawQuery = ""
	rawurl = u.String()
	pool := redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(rawurl, dialOptions...)
		},
	}
	if _, ok := q["max-active"]; ok {
		v := q.Get("max-active")
		pool.MaxActive, _ = strconv.Atoi(v)
	}
	if _, ok := q["max-idle"]; ok {
		v := q.Get("max-idle")
		pool.MaxIdle, _ = strconv.Atoi(v)
	}
	if _, ok := q["idle-timeout"]; ok {
		v := q.Get("idle-timeout")
		pool.IdleTimeout, _ = time.ParseDuration(v)
	}
	if _, ok := q["max-conn-lifetime"]; ok {
		v := q.Get("max-conn-lifetime")
		pool.MaxConnLifetime, _ = time.ParseDuration(v)
	}
	return &pool, nil
}

func ParseURL(rawurl string) (o Options, err error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return
	}
	q := u.Query()
	o.ScanSize, _ = strconv.ParseInt(q.Get("scan-size"), 10, 64)
	o.KeyPrefix = q.Get("key-prefix")
	u.RawQuery = q.Encode()
	o.RedisURL = rawurl
	// TODO: parse resolutiuons from URL
	o.Resolutions = []Resolution{
		ResolutionHourly.WithTTL(Weekly),
		ResolutionDaily.WithTTL(Yearly),
		ResolutionWeekly.WithTTL(10 * Yearly),
	}
	return
}
