package meter_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
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
	defer rc.FlushDB()
	db := meter.NewDB(rc)
	n := event.WithLabelValues("bar", "baz").Add(1)
	if n != 1 {
		t.Errorf("Invalid counter %d", n)
	}
	event.WithLabelValues("bax").Add(1)
	now := time.Now().In(time.UTC)
	n, err := db.Gather(event, now)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if n != 3 {
		t.Errorf("Wrong pipeline size %d", n)
	}
	q := url.Values{}
	sq := meter.QueryBuilder{
		Events:     []string{"test"},
		Start:      now.Add(-72 * 3 * time.Hour),
		End:        now,
		Query:      q,
		Group:      []string{"foo"},
		Resolution: "daily",
	}
	qs := sq.Queries(reg)
	results, err := db.Query(qs...)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if len(results) != 2 {
		t.Errorf("Invalid results len %d", len(results))
	} else if n := results[0].Data[0].Value; n != 1 {
		t.Errorf("Invalid group results %d", n)
	}

	c := meter.Controller{Store: db, Registry: reg, TimeDecoder: resol}
	s := httptest.NewServer(&c)
	// s.Start()
	defer s.Close()
	dt := now.Format(meter.DailyDateFormat)
	res, err := s.Client().Get(s.URL + "?event=test&start=" + dt + "&end=" + dt + "&res=daily&foo=bar")
	if res.StatusCode != http.StatusOK {
		t.Errorf("Invalid response status %d: %s", res.StatusCode, res.Status)
	}
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	data, _ := ioutil.ReadAll(res.Body)
	res.Body.Close()
	results = meter.Results{}
	json.Unmarshal(data, &results)
	r := results.Find("test", meter.LabelValues{"foo": "bar"})
	if r == nil {
		t.Errorf("Result not found %s", results)
	}

	values := db.Values(event, resol, now, now)
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	// log.Println(values)
}
