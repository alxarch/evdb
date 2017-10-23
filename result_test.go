package meter_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

var ts = meter.TimeSequence(time.Now().Add(-meter.Daily), time.Now(), meter.Hourly)

var start = time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
var end = time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)

func Test_TimeSequence(t *testing.T) {

	ts := meter.TimeSequence(start, end, meter.Daily)
	assert.Equal(t, len(ts), 16)
	assert.Equal(t, ts[0], start)
	assert.Equal(t, ts[15], end)
	assert.Equal(t, []time.Time{}, meter.TimeSequence(start, end, 0))
	ts = meter.TimeSequence(start, start, meter.Daily)
	assert.Equal(t, len(ts), 1)
	assert.Equal(t, start, ts[0])
}

var results meter.Results
var resultFoo = meter.Result{
	Event:  "foo",
	Labels: meter.LabelValues{"foo": "bar", "bar": "baz"},
	Data: meter.DataPoints{
		meter.DataPoint{},
	},
}

func Test_DataPoints(t *testing.T) {
	ps := meter.DataPoints{}
	data := []int64{
		12, 15, 17, 20,
		30, 21, 92, 34,
		34, 37, 23, 45,
		74, 21, 92, 103,
	}
	ts := meter.TimeSequence(start, end, meter.Daily)

	for i, t := range ts {
		ps = append(ps, meter.DataPoint{t.Unix(), data[i]})
	}
	ps.Sort()
	n, ok := ps.Find(ts[4])
	assert.Equal(t, n, data[4])
	assert.True(t, ok)
	n, ok = ps.Find(time.Now())
	assert.Equal(t, int64(0), n)
	assert.False(t, ok)
	actualJSON, err := json.Marshal(ps)
	assert.NoError(t, err)
	actual := meter.DataPoints{}
	err = json.Unmarshal(actualJSON, &actual)
	assert.NoError(t, err)
	assert.Equal(t, ps, actual)

}
func Test_ResultsFind(t *testing.T) {
	now := time.Now()
	lvs := map[string]string{
		"foo": "bar",
	}
	results := meter.Results{
		meter.Result{
			Event:  "foo",
			Labels: lvs,
			Data: meter.DataPoints{
				meter.DataPoint{Timestamp: now.Unix(), Value: 42},
			},
		},
	}
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
	var nilR meter.Results
	r = nilR.Find("bar", lvs)
	if r != nil {
		t.Errorf("Result not found")
	}

}
