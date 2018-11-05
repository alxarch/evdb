package meter

import (
	"testing"
)

func BenchmarkEvent_Add(b *testing.B) {
	b.ReportAllocs()
	e := NewEvent("foo", "bar", "baz")
	for i := 0; i <= b.N; i++ {
		e.Add(1, "BAR", "BAZ")
	}
}
func Test_Event(t *testing.T) {
	e := NewEvent("foo", "bar", "baz")
	e.Add(1, "BAR", "BAZ")

	s := e.Flush(nil)
	AssertEqual(t, s, Snapshot{{
		Values: []string{"BAR", "BAZ"},
		Count:  1,
	}})
	AssertEqual(t, e.get(0).Count, int64(0))
	e.Merge(s)
	e.Add(1, "BAR", "BAZ")
	AssertEqual(t, e.get(0).Count, int64(2))
	e.Flush(nil)
	e.Pack()
	AssertEqual(t, e.Len(), 0)
	AssertEqual(t, e.index, map[uint64][]int{})
}
