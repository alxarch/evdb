package meter2_test

import (
	"log"
	"net/http/httptest"
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
	// q.Set("foo", "bar")
	// q.Set("bar", "baz")
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
		Group:      "foo",
		Resolution: "daily",
	}
	results, err := db.Query(sq)
	log.Println(err, results)
	s := httptest.NewServer(db)
	// s.Start()
	defer s.Close()
	res, err := s.Client().Get(s.URL + "?event=foo&start=2017&end=2017&res=daily")
	log.Println(res.Status, err)

}
