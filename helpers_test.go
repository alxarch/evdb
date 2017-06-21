package meter_test

import (
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

func Test_DateRangeParser(t *testing.T) {
	p := meter.DateRangeParser(meter.ResolutionDaily)
	start, end, err := p("2017-01-01", "2017-01-16", meter.Monthly)
	assert.Nil(t, err)
	expect := time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expect, start, "Invalid start %s", start)
	assert.Equal(t, expect, start)
	expect = time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expect, end, "Invalid end %s", end)
}

func Test_TimeSequence(t *testing.T) {
	start := time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2017, time.January, 16, 0, 0, 0, 0, time.UTC)
	ts := meter.TimeSequence(start, end, meter.Daily)
	assert.Equal(t, len(ts), 16)
	assert.Equal(t, ts[0], start)
	assert.Equal(t, ts[15], end)
	assert.Equal(t, []time.Time{}, meter.TimeSequence(start, end, 0))
}
