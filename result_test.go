package meter

import (
	"reflect"
	"sort"
	"testing"
)

var _ sort.Interface = DataPoints{}

// AssertEqual checks if values are equal
func AssertEqual(t *testing.T, a interface{}, b interface{}) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		t.Errorf("a != b %v %v", a, b)
	}
}
func Assert(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}

func AssertNil(t *testing.T, a interface{}) {
	if a != nil {
		t.Errorf("a != nil %v", a)
	}
}
