package meter

import (
	"testing"
)

func Test_Registry(t *testing.T) {
	r := NewRegistry()
	desc := NewCounterDesc("foo", []string{})
	e := NewEvent(desc)
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
		t.Errorf("Invalid events %v", events)
	} else if events[0].Describe().Name() != "foo" {
		t.Errorf("Invalid event %v", events[0])
	}
	e = r.Get("foo")
	if e == nil {
		t.Errorf("Nil event")
	}

}
