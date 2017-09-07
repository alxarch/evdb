package meter2_test

import (
	"log"
	"net/url"
	"testing"
	"time"

	"github.com/alxarch/go-meter/meter2"
	"github.com/go-redis/redis"
)

var reg = meter2.NewRegistry()
var desc = meter2.NewDesc("test", []string{"foo", "bar"}, meter2.ResolutionDaily)
var event = meter2.NewEvent(desc)
var rc = redis.NewClient(&redis.Options{
	Addr: ":6379",
	DB:   3,
})

func init() {
	reg.Register(event)
}

func Test_ReadWrite(t *testing.T) {
	db := meter2.NewDB(rc)
	db.Registry = reg
	n := event.WithLabelValues([]string{"bar", "baz"}).Add(1)
	event.WithLabelValues([]string{"bax"}).Add(1)
	log.Println("Counter", n)
	db.Gather(event)
	q := url.Values{}
	q.Set("foo", "bar")
	q.Set("bar", "baz")
	// data := []byte{}
	// field := meter2.AppendMatchField(data[:0], desc.Labels(), "", map[string]string{
	// 	"foo": "bar",
	// 	"bar": "baz",
	// })
	// result, err := db.Scan("meter:\x1fdaily\x1f2017-09-07\x1ftest", string(field))
	// log.Println("Counter", string(field), result, err)
	sq := meter2.ScanQuery{
		Event:      "test",
		Start:      time.Now().Add(-72 * 3 * time.Hour),
		End:        time.Now(),
		Query:      q,
		Resolution: meter2.ResolutionDaily,
	}
	results := make(chan meter2.ScanResult, 1)
	done := make(chan int)
	go func() {
		defer close(done)
		for r := range results {
			log.Println(r)
		}
	}()
	err := db.ScanQuery(sq, results)
	close(results)
	<-done
	log.Println(err)

}
