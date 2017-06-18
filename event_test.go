package meter_test

import (
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/go-redis/redis"
)

var redisOptions = &redis.Options{Addr: ":6379"}
var redisClient = redis.NewClient(redisOptions)

func Test_Persist(t *testing.T) {
	f := meter.NewFilter(meter.ResolutionDaily, meter.Daily)
	e := meter.NewEvent("foo", []string{}, f)
	e.Log(2)
	now := time.Now()
	e.Persist(now, redisClient)
	key := meter.ResolutionDaily.Key(e.EventNameLabels(nil), now)
	defer redisClient.Del(key)
	result, err := redisClient.HGetAll(key).Result()
	if err != nil {
		t.Error(err)
	}
	if n := result["*"]; n != "2" {
		t.Errorf("Invalid count %s", n)
	}

}
