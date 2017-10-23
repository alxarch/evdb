package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_Event(t *testing.T) {
	desc := meter.NewCounterDesc("foo", []string{"bar", "baz"})
	e := meter.NewEvent(desc)
	e.WithLabelValues("BAR", "BAZ").Add(1)
	ch := make(chan meter.Metric, 1)
	done := make(chan struct{})
	go func() {
		for m := range ch {
			values := m.Values()
			if len(values) == 2 {
				if values[0] != "BAR" {
					t.Errorf("Invalid bar value %s", values[0])
				}
				if values[1] != "BAZ" {
					t.Errorf("Invalid baz value %s", values[0])
				}
			} else {
				t.Errorf("Invalid values %s", values)
			}
		}
		close(done)

	}()
	e.Collect(ch)
	close(ch)
	<-done

	m := e.WithLabels(meter.LabelValues{"foo": "bar"})
	values := m.Values()
	if len(values) != 2 {
		t.Errorf("Invalid values %s", values)
	}
}
