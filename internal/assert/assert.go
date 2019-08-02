package assert

import (
	"reflect"
	"testing"
)

// Equal checks if values are equal
func Equal(t *testing.T, a interface{}, b interface{}) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		t.Errorf("a != b\n%v\n%v\n", a, b)
	}
}

func OK(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}

func Nil(t *testing.T, a interface{}) {
	if a != nil {
		v := reflect.ValueOf(a)
		switch k := v.Type().Kind(); k {
		case reflect.Ptr, reflect.Slice, reflect.Func, reflect.Chan, reflect.Map:
			if !v.IsNil() {
				t.Errorf("a != nil %v", a)
			}
		default:
			t.Errorf("Not a pointer %q", v.Type())
		}
	}
}
func NoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
}
