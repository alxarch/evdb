package meter_test

import (
	"net/url"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/alxarch/go-meter/tcodec"
	"github.com/stretchr/testify/assert"
)

func Test_ParseQuery(t *testing.T) {
	q := url.Values{}
	q.Set("event", "foo")
	q.Set("res", "daily")
	qb, err := meter.ParseQuery(q, tcodec.LayoutCodec(meter.DailyDateFormat))
	assert.Error(t, err)
	q.Set("event", "foo")
	q.Set("res", "daily")
	q.Set("start", "2017-10-30")
	q.Set("end", "2017-11-05")
	qb, err = meter.ParseQuery(q, tcodec.LayoutCodec(meter.DailyDateFormat))
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo"}, qb.Events)
	events := meter.NewRegistry()
	fooDesc := meter.NewCounterDesc("foo", []string{"bar", "baz"}, meter.ResolutionDaily.WithTTL(time.Hour))
	fooEvent := meter.NewEvent(fooDesc)
	events.Register(fooEvent)
	qs := qb.Queries(events)
	assert.Equal(t, 1, len(qs))
	// fmt.Printf("%+v", qs[0])

}
