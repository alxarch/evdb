package meter_test

import (
	"log"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/go-redis/redis"
)

var redisOptions = &redis.Options{Addr: ":6379"}
var redisClient = redis.NewClient(redisOptions)

func Test_Labels(t *testing.T) {
	dim := []string{"bar"}
	// f := meter.NewFilter(meter.ResolutionDaily, meter.Daily, dim)
	e := meter.NewEvent("foo", dim, meter.ResolutionDaily)
	if has := e.HasLabel("baz"); has {
		t.Error("Haslabel error")
	}
	if has := e.HasLabel("bar"); !has {
		t.Error("Haslabel error")
	}
	labels := e.Labels("bar", "baz")
	if len(labels) != 2 {
		t.Error("Invalid size")
	}
	if labels[0] != "bar" {
		t.Error("Invalid label")
	}
	if labels[1] != "baz" {
		t.Error("Invalid label")
	}
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
	if has := e.HasLabel("baz"); has {
		t.Error("Haslabel error")
	}
	if has := e.HasLabel("bar"); !has {
		t.Error("Haslabel error")
	}
	e.Log(1)
	e.Log(1, e.Labels("bar", "baz")...)
	now := time.Now()
	e.Persist(now, redisClient)
	key := meter.ResolutionDaily.Key(e.EventName(), now)
	defer redisClient.Del(key)
	result, err := redisClient.HGetAll(key).Result()
	if err != nil {
		t.Error(err)
	}
	if n := result[e.AllField()]; n != "2" {
		t.Errorf("Invalid count %s", n)
	}
	if n := result["bar:baz"]; n != "1" {
		t.Errorf("Invalid count %s", n)
	}
	log.Println(result)

}
