package meter

import (
	"sort"
	"testing"
)

func TestFields_MatchSorted(t *testing.T) {
	fields := Fields{
		{Label: "foo", Value: "bar"},
	}
	match := Fields{
		{Label: "foo", Value: "bar"},
		{Label: "foo", Value: "baz"},
		{Label: "bar", Value: "baz"},
	}
	sort.Sort(match)
	if fields.MatchSorted(match) {
		t.Errorf("Invalid match")
	}
	fields = append(fields, Field{Label: "bar", Value: "baz"})
	sort.Sort(fields)
	if !fields.MatchSorted(match) {
		t.Errorf("No match")
	}

}

// func TestFields_MatchValues(t *testing.T) {
// 	fields := Fields{
// 		{Label: "foo", Value: "bar"},
// 	}
// 	match := url.Values{
// 		"foo": []string{"bar", "baz"},
// 		"bar": []string{"baz"},
// 	}
// 	if fields.MatchValues(match) {
// 		t.Errorf("Invalid match")
// 	}
// 	delete(match, "bar")
// 	if !fields.MatchValues(match) {
// 		t.Errorf("No match")
// 	}
// 	if !fields.MatchValues(nil) {
// 		t.Errorf("No match")
// 	}

// }

// func TestSubValues(t *testing.T) {
// 	v := SubValues(':', "foo:bar", "bar:baz")
// 	AssertEqual(t, v, url.Values{
// 		"foo": []string{"bar"},
// 		"bar": []string{"baz"},
// 	})
// }
