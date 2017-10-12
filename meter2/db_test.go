package meter2_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/alxarch/go-meter/meter2"
	"github.com/alxarch/go-meter/meter2/tcodec"
	"github.com/go-redis/redis"
)

var reg = meter2.NewRegistry()
var desc = meter2.NewDesc(meter2.MetricTypeIncrement, "test", []string{"foo", "bar"}, meter2.ResolutionDaily)
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
	// db.Registry = reg
	n := event.WithLabelValues("bar", "baz").Add(1)
	event.WithLabelValues("bax").Add(1)
	if n != 1 {
		t.Errorf("Invalid counter %d", n)
	}
	db.Gather(event, time.Now())
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
	sq := meter2.QueryBuilder{
		Events:     []string{"test"},
		Start:      time.Now().Add(-72 * 3 * time.Hour),
		End:        time.Now(),
		Query:      q,
		Group:      []string{"foo"},
		Resolution: "daily",
	}
	qs := sq.Queries(reg)
	results, err := db.Query(qs...)
	if len(results) != 1 {
		t.Errorf("Invalid results %d", len(results))
	}
	if err != nil {
		t.Error(err)
	}

	c := meter2.Controller{DB: db, Registry: reg, TimeDecoder: tcodec.LayoutCodec("2006")}
	s := httptest.NewServer(&c)
	// s.Start()
	defer s.Close()
	res, err := s.Client().Get(s.URL + "?event=foo&start=2017&end=2017&res=daily")
	if res.StatusCode != http.StatusOK {
		t.Errorf("Invalid response status %d: %s", res.StatusCode, res.Status)
	}
	if err != nil {
		t.Error(err)
	}

}
