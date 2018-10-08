package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_SplitValues(t *testing.T) {
	if values := meter.SplitValues(""); len(values) != 0 {
		t.Errorf("Invalid empty string split: %s", values)
	}
	if values := meter.SplitValues("foo\x1fbar"); len(values) != 2 {
		t.Errorf("Invalid empty string split: %s", values)
	}
}
func Test_Counters(t *testing.T) {
	cs := meter.LocalCounters{}
	cs.WithLabelValues("foo", "bar").Add(1)
	if n := cs.WithLabelValues("foo", "bar").Count(); n != 1 {
		t.Errorf("Invalid counter value %d", n)
	}
	cs.Reset()
	if n := cs.WithLabelValues("foo", "bar").Count(); n != 0 {
		t.Errorf("Invalid counter value %d", n)
	}
}
