package meter

import (
	"testing"
)

func BenchmarkEvent_Add(b *testing.B) {
	b.ReportAllocs()
	desc := NewCounterDesc("foo", []string{"bar", "baz"})
	e := NewEvent(desc)
	for i := 0; i <= b.N; i++ {
		e.Add(1, "BAR", "BAZ")
	}

}
func Test_Event(t *testing.T) {
	desc := NewCounterDesc("foo", []string{"bar", "baz"})
	e := NewEvent(desc)
	e.Add(1, "BAR", "BAZ")
	// ch := make(chan meter.Metric, 1)
	// done := make(chan struct{})
	// var values []string
	// go func() {
	// 	for m := range ch {
	// 		values = m.AppendValues(values[:0])
	// 		if len(values) == 2 {
	// 			if values[0] != "BAR" {
	// 				t.Errorf("Invalid bar value %s", values[0])
	// 			}
	// 			if values[1] != "BAZ" {
	// 				t.Errorf("Invalid baz value %s", values[0])
	// 			}
	// 		} else {
	// 			t.Errorf("Invalid values %s", values)
	// 		}
	// 	}
	// 	close(done)

	// }()
	// e.Collect(ch)
	// close(ch)
	// <-done

	// m := e.cou("bar")
	// values = m.AppendValues(values[:0])
	// if len(values) != 2 {
	// 	t.Errorf("Invalid values %s", values)
	// }
}
