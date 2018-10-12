package meter

import (
	"testing"
)

func Test_Desc(t *testing.T) {
	desc := NewDesc(MetricTypeIncrement, "foo", []string{})
	if desc.Name() != "foo" {
		t.Errorf("Invalid desc name")
	}
	if r := desc.Resolutions(); r == nil {
		t.Errorf("Invalid (nil) desc resolutions")
	} else if len(r) != 0 {
		t.Errorf("Invalid (non-empty) desc resolutions")
	}
	_, ok := desc.Resolution("daily")
	if ok {
		t.Errorf("Invalid daily resolution")
	}
	// values := desc.LabelValues([]string{"foo", "bar"})
	// if len(values) != 0 {
	// 	t.Errorf("Invalid empty values")

	// }

}
