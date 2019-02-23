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
