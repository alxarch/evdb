package events_test

import (
	"testing"

	meter "github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/internal/assert"
)

func BenchmarkEvent_Add(b *testing.B) {
	b.ReportAllocs()
	e := meter.New("foo", "bar", "baz")
	for i := 0; i <= b.N; i++ {
		e.Add(1, "BAR", "BAZ")
	}
}
func Test_Event(t *testing.T) {
	e := meter.New("foo", "bar", "baz")
	e.Add(1, "BAR", "BAZ")

	s := e.Flush(nil)
	assert.Equal(t, len(s), 1)
	assert.Equal(t, s[0], meter.Counter{
		Values: []string{"BAR", "BAZ"},
		Count:  1,
	})
	// assert.Equal(t, e.get(0).Count, int64(0))
	e.Merge(s)
	e.Add(1, "BAR", "BAZ")
	// AssertEqual(t, e.get(0).Count, int64(2))
	e.Flush(nil)
	e.Pack()
	assert.Equal(t, e.Len(), 0)
	// AssertEqual(t, e.index, map[uint64][]int{})
}
