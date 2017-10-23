package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_LabelValues(t *testing.T) {
	lvs := meter.FieldLabels([]string{"foo", "bar"})
	values := lvs.Values([]string{"foo"})
	if len(values) != 1 {
		t.Errorf("Invalid values %s", values)
	} else if values[0] != "bar" {
		t.Errorf("Invalid value %s", values[0])
	}
	if lvs.Equal(meter.LabelValues{}) {
		t.Errorf("Invalid equal")
	}
	if !lvs.Equal(meter.LabelValues{"foo": "bar"}) {
		t.Errorf("Invalid equal")
	}
	if lvs.Equal(meter.LabelValues{"foo": "bar", "bar": "baz"}) {
		t.Errorf("Invalid equal")
	}
	if lvs.Equal(meter.LabelValues{"bar": "baz"}) {
		t.Errorf("Invalid equal")
	}

}
