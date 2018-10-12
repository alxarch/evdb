package meter

import (
	"testing"
)

func BenchmarkLocalCounters_Add(b *testing.B) {
	e := Counters{}
	b.ReportAllocs()
	for i := 0; i <= b.N; i++ {
		e.Add(1, "BAR", "BAZ")
	}

}

func Test_Counters(t *testing.T) {
	cs := Counters{}
	cs.Add(1, "foo", "bar")
	c := cs.counters[0]
	if c.n != 1 {
		t.Errorf("Invalid counter value %d", c.n)
	}
	// cs.Reset()
}
