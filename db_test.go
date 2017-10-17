package meter_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/alxarch/go-meter/tcodec"
	"github.com/go-redis/redis"
)

var reg = meter.NewRegistry()
var resol = meter.ResolutionDaily.WithTTL(meter.Daily)
var desc = meter.NewDesc(meter.MetricTypeIncrement, "test", []string{"foo", "bar"}, resol)
var event = meter.NewEvent(desc)
var rc = redis.NewClient(&redis.Options{
	Addr: ":6379",
	DB:   3,
})

func init() {
	reg.Register(event)
}

func Test_ReadWrite(t *testing.T) {
	db := meter.NewDB(rc)
	// db.Registry = reg
	n := event.WithLabelValues("bar", "baz").Add(1)
	if n != 1 {
		t.Errorf("Invalid counter %d", n)
	}
	event.WithLabelValues("bax").Add(1)
	n, err := db.Gather(event, time.Now())
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if n != 3 {
		t.Errorf("Wrong pipeline size %d", n)
	}
	q := url.Values{}
	sq := meter.QueryBuilder{
		Events:     []string{"test"},
		Start:      time.Now().Add(-72 * 3 * time.Hour),
		End:        time.Now(),
		Query:      q,
		Group:      []string{"foo"},
		Resolution: "daily",
	}
	qs := sq.Queries(reg)
	results, err := db.Query(qs...)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if len(results) != 1 {
		t.Errorf("Invalid results %d", len(results))
	}

	c := meter.Controller{DB: db, Registry: reg, TimeDecoder: tcodec.LayoutCodec("2006")}
	s := httptest.NewServer(&c)
	// s.Start()
	defer s.Close()
	res, err := s.Client().Get(s.URL + "?event=foo&start=2017&end=2017&res=daily")
	if res.StatusCode != http.StatusOK {
		t.Errorf("Invalid response status %d: %s", res.StatusCode, res.Status)
	}
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}

	values := db.ValueScan(event, resol, time.Now(), time.Now())
	log.Println(values)
}
