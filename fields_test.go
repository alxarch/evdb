package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter/v2"
)

func TestFields_MatchSorted(t *testing.T) {
	{
		match := meter.Fields{
			{Label: "color", Value: "blue"},
		}
		fields := meter.Fields{
			{Label: "color", Value: "blue"},
			{Label: "taste", Value: "sour"},
		}
		ok := fields.MatchSorted(match)
		if !ok {
			t.Errorf("No match")
		}
	}
	{
		match := meter.Fields{
			{Label: "color", Value: "blue"},
			{Label: "color", Value: "green"},
			{Label: "taste", Value: "bitter"},
		}
		fields := meter.Fields{
			{Label: "color", Value: "green"},
			{Label: "shape", Value: "round"},
			{Label: "taste", Value: "bitter"},
		}
		if !fields.MatchSorted(match) {
			t.Errorf("No match")
		}

	}
	{
		match := meter.Fields{
			{Label: "color", Value: "blue"},
			{Label: "color", Value: "green"},
			{Label: "taste", Value: "bitter"},
		}
		fields := meter.Fields{
			{Label: "color", Value: "green"},
			{Label: "taste", Value: "sweet"},
		}
		if fields.MatchSorted(match) {
			t.Errorf("Invalid match")
		}

	}

}
