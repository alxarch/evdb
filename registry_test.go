package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_Registry(t *testing.T) {
	r := meter.NewRegistry()
	desc := meter.NewCounterDesc("foo", []string{})
	e := meter.NewEvent(desc)
	err := r.Register(e)
	if err != nil {
		t.Errorf("Non nil error %s", err)
	}
	err = r.Register(e)
	if err == nil {
		t.Errorf("Nil error")
	}
	events := r.Events()
	if len(events) != 1 {
		t.Errorf("Invalid events %s", events)
	} else if events[0].Describe().Name() != "foo" {
		t.Errorf("Invalid event %s", events[0])
	}
	e = r.Get("foo")
	if e == nil {
		t.Errorf("Nil event")
	}

}
