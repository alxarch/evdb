package meter_test

import (
	"net/url"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

func Test_DBQuery(t *testing.T) {
	db := meter.NewDB(redisClient)
	reg := meter.NewRegistry()
	db.Registry = reg
	res := meter.ResolutionDaily
	e := meter.NewEvent("foo", []string{"goo", "bar"}, res)
	reg.Register("foo", e)
	start := time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)

	q := meter.Query{
		Events:     []string{"foo"},
		Start:      start,
		End:        end,
		Labels:     url.Values{"goo": []string{"foo"}},
		Resolution: res,
	}
	recs, err := db.Records(q)
	assert.Nil(t, err)
	assert.Equal(t, 16, len(recs))
	e.LogWithLabelValues(12, "foo")
	e.Persist(start, redisClient)
	key := e.Key(res, start, nil)
	defer redisClient.Del(key)
	err = meter.ReadRecords(redisClient, recs)
	assert.Nil(t, err)
	n := recs[0].Value()
	assert.Equal(t, int64(12), n, "Invalid rec value %d", n)
	n = recs[1].Value()
	assert.Equal(t, int64(0), n, "Invalid rec value %d", n)

}
