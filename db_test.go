package meter_test

import (
	"encoding/json"
	"fmt"
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
	err := db.Gather(event, now)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if n := db.Redis.HLen(db.Key(meter.ResolutionDaily, "test", now)).Val(); n != 2 {
		t.Errorf("invalid gather %d", n)
	}
	if n := event.Len(); n != 2 {
		t.Errorf("Wrong collector size %d", n)
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
	println("Run q")
	qs := sq.Queries(reg)
	results, err := db.Query(qs...)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if len(results) != 2 {
		t.Errorf("Invalid results len %d", len(results))
	}
	for _, r := range results {
		println(fmt.Sprintf("result\n%+v\n", r))
	}

	c := meter.Controller{Q: db, Events: reg, TimeDecoder: resol}
	s := httptest.NewServer(&c)
	// s.Start()
	defer s.Close()
	dt := now.Format(meter.DailyDateFormat)
	res, err := s.Client().Get(s.URL + "?event=test&start=" + dt + "&end=" + dt + "&res=daily&foo=bar")
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	} else if res.StatusCode != http.StatusOK {
		t.Errorf("Invalid response status %d: %s", res.StatusCode, res.Status)
		data, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		results = meter.Results{}
		json.Unmarshal(data, &results)
		r := results.Find("test", meter.LabelValues{"foo": "bar"})
		if r == nil {
			t.Errorf("Result not found %v", results)
		}
	}

	results, _ = db.Query(meter.Query{
		Mode:       meter.ModeValues,
		Event:      event,
		Start:      now,
		End:        now,
		Resolution: meter.ResolutionDaily,
	})
	values := results.FrequencyMap()
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	results, _ = db.Query(meter.Query{
		Mode:  meter.ModeValues,
		Event: event,
		Start: now,
		Values: []map[string]string{
			map[string]string{
				"foo": "bar",
			},
		},
		End:        now,
		Resolution: meter.ResolutionDaily,
	})
	values = results.FrequencyMap()
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	// log.Println(values)
}
