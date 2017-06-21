package meter_test

import (
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

func Test_ParseDateRange(t *testing.T) {
	s := "2017-01-01"
	e := "2017-01-16"

	start, end, err := meter.ResolutionDaily.ParseDateRange(s, e)
	assert.Nil(t, err)
	expect := time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expect, start)
	expect = time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expect, end)

	_, _, err = (*meter.Resolution)(nil).ParseDateRange(s, e)
	assert.Nil(t, err)

}

func Test_ResTimeSequence(t *testing.T) {
	start := time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)
	res := meter.ResolutionDaily
	ts := res.TimeSequence(start, end)
	assert.Equal(t, len(ts), 16)
	assert.Equal(t, ts[0], start)
	assert.Equal(t, ts[15], end)
}
