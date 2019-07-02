package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter/v2"
)

func TestFields_Reset(t *testing.T) {
	{
		var fields meter.Fields
		fields = fields.Reset()
		if fields != nil {
			t.Errorf("Invalid reset")
		}
	}
	{
		fields := meter.Fields{
			{Label: "foo", Value: "bar"},
		}
		fields = fields.Reset()
		if fields == nil {
			t.Errorf("Invalid reset")
		}
		if len(fields) != 0 {
			t.Errorf("Invalid reset")
		}
		fields = fields[:cap(fields)]
		if len(fields) != 1 {
			t.Errorf("Invalid reset")
		}
		if fields[0].Label != "" {
			t.Errorf("Invalid reset")
		}
		if fields[0].Value != "" {
			t.Errorf("Invalid reset")
		}
	}
}
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

func TestFields_Get(t *testing.T) {
	{
		var fields meter.Fields
		v, ok := fields.Get("foo")
		if ok {
			t.Errorf("Invalid get")
		}
		if v != "" {
			t.Errorf("Invalid get")
		}
	}
	{
		fields := meter.Fields{
			{Label: "foo", Value: "bar"},
		}
		v, ok := fields.Get("foo")
		if !ok {
			t.Errorf("Invalid get")
		}
		if v != "bar" {
			t.Errorf("Invalid get")
		}
		vv, ok := fields.Get("bar")
		if ok {
			t.Errorf("Invalid get")
		}
		if vv != "" {
			t.Errorf("Invalid get")
		}
	}
}

func TestFields_FromBlob(t *testing.T) {
	tests := []struct {
		name   string
		fields meter.Fields
	}{
		{"nil", nil},
		{"blank", meter.Fields{}},
		{"some", meter.Fields{{"a", "b"}}},
		{"more", meter.Fields{{"a", "b"}, {"foo", "bar"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob, _ := tt.fields.AppendBlob(nil)
			{
				fields, tail := meter.Fields(nil).FromBlob(blob)
				if !fields.Equal(tt.fields) {
					t.Errorf("Fields.FromBlob() got = %v, want %v", fields, tt.fields)
				}
				if len(tail) != 0 {
					t.Errorf("Fields.FromBlob() invalid tail = %v", tail)
				}

			}
			{
				var fields meter.Fields
				tail, _ := fields.ShiftBlob(blob)
				if !fields.Equal(tt.fields) {
					t.Errorf("Fields.ShiftBlob() got = %v, want %v", fields, tt.fields)
				}
				if len(tail) != 0 {
					t.Errorf("Fields.ShiftBlob() invalid tail = %v", tail)
				}

			}
			{
				var fields meter.Fields
				fields.UnmarshalBinary(blob)
				if !fields.Equal(tt.fields) {
					t.Errorf("Fields.UnmarshalBinaty() got = %v, want %v", fields, tt.fields)
				}
			}
		})
	}
}

func TestFieldsJSON(t *testing.T) {
	tests := []struct {
		name   string
		fields meter.Fields
		json   string
	}{
		{"nil", nil, `null`},
		{"blank", meter.Fields{}, `{}`},
		{"some", meter.Fields{{"a", "b"}}, `{"a":"b"}`},
		{"more", meter.Fields{{"a", "b"}, {"foo", "bar"}}, `{"a":"b","foo":"bar"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.fields.MarshalJSON()
			AssertNoError(t, err)
			AssertEqual(t, string(data), tt.json)
			var fields meter.Fields
			err = fields.UnmarshalJSON(data)
			AssertNoError(t, err)
			Assert(t, fields.Sorted().Equal(tt.fields), "%s Fields.UnmarshalJSON invalid fields %v != %v", tt.name, fields, tt.fields)
		})
	}
}
