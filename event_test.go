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
	if !ok {
		t.Error("Dim field not ok")
	}
	if f != "bar:baz" {
		t.Error("Dim field invalid")
	}
}
func Test_Persist(t *testing.T) {
	dim := []string{"bar"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e := meter.NewEvent("foo", dim, meter.ResolutionDaily)
	assert.True(t, e.HasLabel("bar"))
	assert.False(t, e.HasLabel("baz"))
	e.Log(1)
	e.Log(1, e.Labels("bar", "baz")...)
	now := time.Now()
	e.Persist(now, redisClient)
	key := meter.ResolutionDaily.Key(e.EventName(), now)
	assert.Equal(t, "stats:daily:"+meter.ResolutionDaily.MarshalTime(now)+":foo", key)
	defer redisClient.Del(key)
	result, err := redisClient.HGetAll(key).Result()
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{
		e.AllField():                       "2",
		e.Field(e.Labels("bar", "baz")...): "1",
	}, result)

}
