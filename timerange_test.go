package evdb_test

import (
	"testing"
	"time"

	db "github.com/alxarch/evdb"
	"github.com/alxarch/evdb/internal/assert"
)

func Test_TimeRange(t *testing.T) {
	var tr db.TimeRange
	assert.Equal(t, tr.NumSteps(), -1)
	now := time.Now()
	tr.End = now
	tr.Start = now.Add(-10 * time.Hour)
	tr.Step = time.Hour
	assert.Equal(t, tr.NumSteps(), 10)
	var ts []time.Time
	var ns []int
	tr.Each(func(tm time.Time, i int) {
		ts = append(ts, tm)
		ns = append(ns, i)
	})
	assert.Equal(t, len(ts), 11)
	var want []time.Time
	start := tr.Start.Truncate(time.Hour)
	for i := 0; i < 11; i++ {
		want = append(want, start.Add(time.Duration(i)*time.Hour))
	}
	assert.Equal(t, ts, want)
	assert.Equal(t, ns, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	ot := tr.Offset(time.Hour)
	assert.Equal(t, ot.NumSteps(), 10)
	assert.Equal(t, ot.Start, tr.Start.Add(time.Hour))
	assert.Equal(t, ot.End, tr.End.Add(time.Hour))
	assert.Equal(t, tr.Rel(&ot), db.TimeRelOverlapsAfter)
	assert.Equal(t, ot.Rel(&tr), db.TimeRelOverlapsBefore)
	assert.Equal(t, ot.Rel(&ot), db.TimeRelEqual)

}
