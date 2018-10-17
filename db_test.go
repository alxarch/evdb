package meter

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var reg = NewRegistry()
var resol = ResolutionDaily.WithTTL(Daily)
var desc = NewDesc(MetricTypeIncrement, "test", []string{"foo", "bar"}, resol)
var event = NewEvent(desc)
var rc = &redis.Pool{
	Dial: func() (redis.Conn, error) {
		return redis.DialURL("redis://127.0.0.1/9")
	},
}

func init() {
	reg.Register(event)
}

func Test_ReadWrite(t *testing.T) {
	conn := rc.Get()
	defer func() {
		conn.Do("FLUSHDB")
		conn.Close()
	}()
	db := NewDB(rc)
	n := event.Add(1, "bar", "baz")
	if n != 1 {
		t.Errorf("Invalid counter %d", n)
	}
	n = event.Add(1, "bar", "baz")
	if n != 2 {
		t.Errorf("Invalid counter %d", n)
	}
	event.Add(1, "bax")
	now := time.Now().In(time.UTC)
	err := db.Gather(now, event)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if n, _ := redis.Int64(conn.Do("HLEN", db.Key(ResolutionDaily, "test", now))); n != 2 {
		t.Errorf("invalid gather %d", n)
	}
	if n := event.Len(); n != 2 {
		t.Errorf("Wrong collector size %d", n)
	}
	q := url.Values{}
	sq := QueryBuilder{
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
	}
	for i, r := range results {
		t.Logf("result %d\n%+v\n", i, r)
	}

	c := Controller{DB: db, Registry: reg, TimeDecoder: resol}
	s := httptest.NewServer(&c)
	// s.Start()
	defer s.Close()
	dt := now.Format(DailyDateFormat)
	res, err := s.Client().Get(s.URL + "?event=test&start=" + dt + "&end=" + dt + "&res=daily&foo=bar")
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	} else if res.StatusCode != http.StatusOK {
		t.Errorf("Invalid response status %d: %s", res.StatusCode, res.Status)
		data, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		results = Results{}
		json.Unmarshal(data, &results)
		r := results.Find("test", map[string]string{"foo": "bar"})
		if r == nil {
			t.Errorf("Result not found %v", results)
		}
	}

	results, _ = db.Query(Query{
		Mode:       ModeValues,
		Event:      event,
		Start:      now,
		End:        now,
		Resolution: ResolutionDaily,
	})
	values := results.FrequencyMap()
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	results, _ = db.Query(Query{
		Mode:  ModeValues,
		Event: event,
		Start: now,
		End:   now,
		Values: []map[string]string{
			map[string]string{
				"foo": "bar",
			},
		},
		Resolution: ResolutionDaily,
	})
	values = results.FrequencyMap()
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	// for k, v := range values {
	// 	t.Errorf("%s: %v", k, v)

	// }
}
