package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_Counters(t *testing.T) {
	cs := meter.NewCounters()
	cs.WithLabelValues("foo", "bar").Add(1)
	if n := cs.WithLabelValues("foo", "bar").Count(); n != 1 {
		t.Errorf("Invalid counter value %d", n)
	}
	cs.Reset()
	if n := cs.WithLabelValues("foo", "bar").Count(); n != 0 {
		t.Errorf("Invalid counter value %d", n)
	}

}
