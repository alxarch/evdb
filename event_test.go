package meter_test

import (
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

var redisOptions = &redis.Options{Addr: ":6379"}
var redisClient = redis.NewClient(redisOptions)

func Test_Labels(t *testing.T) {
	labels := []string{"bar", "baz"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e := meter.NewEvent("foo", labels, meter.ResolutionDaily)
	la := e.Labels()
	assert.Equal(t, la, []string{"bar", "*", "baz", "*"})
	la = e.Labels("bar", "foo")
	assert.Equal(t, la, []string{"bar", "foo", "baz", "*"})
}

func Test_Dim(t *testing.T) {
	dim := []string{"bar"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e := meter.NewEvent("foo", dim, meter.ResolutionDaily)
	f, ok := e.DimField([]string{"bar"}, map[string]string{"bar": "baz"})
	assert.True(t, ok, "Dim matches field single")
	assert.Equal(t, "bar:baz", f, "Invalid dim field %s single", f)
	labels := []string{"bar", "baz", "bar"}
	dim = []string{"bar", "baz"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e = meter.NewEvent("foo", labels, meter.ResolutionDaily)
	f, ok = e.DimField(dim, map[string]string{"bar": "baz", "baz": "foo"})
	assert.True(t, ok, "Dim matches field")
	assert.Equal(t, "bar:baz:baz:foo", f, "Invalid dim field %s", f)
}

func Test_Persist(t *testing.T) {
	dim := []string{"bar", "baz"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e := meter.NewEvent("foo", dim, meter.ResolutionDaily)
	assert.True(t, e.HasLabel("bar"))
	assert.True(t, e.HasLabel("baz"))
	assert.False(t, e.HasLabel("foz"))
	e.Log(1)
	e.Log(13, e.Labels("bar", "baz")...)
	now := time.Now()
	s := e.Snapshot()
	assert.Equal(t, map[string]int64{
		"": 1,
		"bar\x00baz\x00baz\x00*": 13,
	}, s)
	e.Persist(now, redisClient)
	s = e.Snapshot()
	assert.Equal(t, map[string]int64{
		"": 0,
		"bar\x00baz\x00baz\x00*": 0,
	}, s)
	key := meter.ResolutionDaily.Key(e.EventName(nil), now)
	assert.Equal(t, "stats:daily:"+meter.ResolutionDaily.MarshalTime(now)+":foo", key)
	defer redisClient.Del(key)
	result, err := redisClient.HGetAll(key).Result()
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{
		"*": "14",
		e.Field("bar", "baz"): "13",
	}, result)

}
func Test_Records(t *testing.T) {
	dim := []string{"bar", "baz"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e := meter.NewEvent("foo", dim, meter.ResolutionDaily)

	start := time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)

	res := meter.ResolutionDaily
	records := e.Records(res, start, end, []string{"bar", "foo"}, []string{"bar", "baz"})
	assert.Equal(t, 32, len(records), "Invalid records length %d", len(records))

}
