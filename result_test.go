package meter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// AssertEqual checks if values are equal
func AssertEqual(t *testing.T, a interface{}, b interface{}) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		t.Errorf("a != b %v %v", a, b)
	}
}
func Assert(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}

func AssertNil(t *testing.T, a interface{}) {
	if a != nil {
		t.Errorf("a != nil %v", a)
	}
}

var ts = TimeSequence(time.Now().Add(-Daily), time.Now(), Hourly)

var start = time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
var end = time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)

func Test_TimeSequence(t *testing.T) {

	ts := TimeSequence(start, end, Daily)
	AssertEqual(t, len(ts), 16)
	AssertEqual(t, ts[0], start)
	AssertEqual(t, ts[15], end)
	AssertEqual(t, []time.Time{}, TimeSequence(start, end, 0))
	ts = TimeSequence(start, start, Daily)
	AssertEqual(t, len(ts), 1)
	AssertEqual(t, start, ts[0])
}

var results Results
var resultFoo = Result{
	Event:  "foo",
	Labels: map[string]string{"foo": "bar", "bar": "baz"},
	Data: DataPoints{
		DataPoint{},
	},
}

func Test_DataPoints(t *testing.T) {
	ps := DataPoints{}
	data := []int64{
		12, 15, 17, 20,
		30, 21, 92, 34,
		34, 37, 23, 45,
		74, 21, 92, 103,
	}
	ts := TimeSequence(start, end, Daily)

	for i, t := range ts {
		ps = append(ps, DataPoint{t.Unix(), data[i]})
	}
	ps.Sort()
	n, ok := ps.Find(ts[4])
	AssertEqual(t, n, data[4])
	AssertEqual(t, ok, true)
	n, ok = ps.Find(time.Now())
	AssertEqual(t, int64(0), n)
	AssertEqual(t, ok, false)
	actualJSON, err := json.Marshal(ps)
	AssertNil(t, err)
	actual := DataPoints{}
	err = json.Unmarshal(actualJSON, &actual)
	AssertNil(t, err)
	AssertEqual(t, ps, actual)

}
func Test_ResultsFind(t *testing.T) {
	now := time.Now()
	lvs := map[string]string{
		"foo": "bar",
	}
	results := Results{
		Result{
			Event:  "foo",
			Labels: lvs,
			Data: DataPoints{
				DataPoint{Timestamp: now.Unix(), Value: 42},
			},
		},
	}
	fmap := results.FrequencyMap()
	AssertEqual(t, fmap, FrequencyMap{
		"foo": map[string]int64{"bar": 42},
	})
	r := results.Find("foo", lvs)
	if r == nil {
		t.Errorf("Result not found")
	}
	if answer, ok := r.Data.Find(now); !ok || answer != 42 {
		t.Errorf("Invalid answer %d", answer)
	}
	if data, err := r.Data[0].MarshalJSON(); err != nil {
		t.Errorf("Marshal err %s", err)
	} else if string(data) != fmt.Sprintf(`[%d,42]`, now.Unix()) {
		t.Errorf("Invalid marshal %s", data)
	}
	if answer, ok := r.Data.Find(now.Add(time.Hour)); ok {
		t.Errorf("Invalid answer %d", answer)
	}
	r = results.Find("bar", lvs)
	if r != nil {
		t.Errorf("Result not found")
	}
	var nilR Results
	r = nilR.Find("bar", lvs)
	if r != nil {
		t.Errorf("Result not found")
	}

}
