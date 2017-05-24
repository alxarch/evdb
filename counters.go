package meter

import (
	"time"

	"github.com/go-redis/redis"
)

type Counters map[string]int64
type KeyCounters map[string]Counters
type TTLCounters map[time.Duration]KeyCounters

func (c TTLCounters) Persist(r redis.UniversalClient) (t int64, err error) {
	pipeline := r.Pipeline()
	defer pipeline.Close()
	for ttl, keys := range c {
		px := ttl / time.Millisecond
		for key, fields := range keys {
			t += int64(len(fields))
			for field, n := range fields {
				pipeline.HIncrBy(key, field, n)
			}
			if px > 0 {
				pipeline.PExpire(key, px)
			}
		}
	}
	_, err = pipeline.Exec()
	return
}

func (c TTLCounters) Increment(key string, field string, n int64, ttl time.Duration) {
	if ttl < 0 {
		ttl = 0
	}
	if nil == c[ttl] {
		c[ttl] = KeyCounters{}
	}
	if nil == c[ttl][key] {
		c[ttl][key] = Counters{}
	}
	c[ttl][key][field] += n
}
