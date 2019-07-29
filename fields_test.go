package evdb_test

import (
	"testing"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/internal/assert"
)

func TestFields_Reset(t *testing.T) {
	{
		var fields evdb.Fields
		fields = fields.Reset()
		if fields != nil {
			t.Errorf("Invalid reset")
		}
	}
	{
		fields := evdb.Fields{
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

func TestFields_Get(t *testing.T) {
	{
		var fields evdb.Fields
		v, ok := fields.Get("foo")
		if ok {
			t.Errorf("Invalid get")
		}
		if v != "" {
			t.Errorf("Invalid get")
		}
	}
	{
		fields := evdb.Fields{
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
		fields evdb.Fields
	}{
		{"nil", nil},
		{"blank", evdb.Fields{}},
		{"some", evdb.Fields{{"a", "b"}}},
		{"more", evdb.Fields{{"a", "b"}, {"foo", "bar"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob, _ := tt.fields.AppendBlob(nil)
			{
				fields, tail := evdb.Fields(nil).FromBlob(blob)
				if !fields.Equal(tt.fields) {
					t.Errorf("Fields.FromBlob() got = %v, want %v", fields, tt.fields)
				}
				if len(tail) != 0 {
					t.Errorf("Fields.FromBlob() invalid tail = %v", tail)
				}

			}
			{
				var fields evdb.Fields
				tail, _ := fields.ShiftBlob(blob)
				if !fields.Equal(tt.fields) {
					t.Errorf("Fields.ShiftBlob() got = %v, want %v", fields, tt.fields)
				}
				if len(tail) != 0 {
					t.Errorf("Fields.ShiftBlob() invalid tail = %v", tail)
				}

			}
			{
				var fields evdb.Fields
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
		fields evdb.Fields
		json   string
	}{
		{"nil", nil, `null`},
		{"blank", evdb.Fields{}, `{}`},
		{"some", evdb.Fields{{"a", "b"}}, `{"a":"b"}`},
		{"more", evdb.Fields{{"a", "b"}, {"foo", "bar"}}, `{"a":"b","foo":"bar"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.fields.MarshalJSON()
			assert.NoError(t, err)
			assert.Equal(t, string(data), tt.json)
			var fields evdb.Fields
			err = fields.UnmarshalJSON(data)
			assert.NoError(t, err)
			assert.OK(t, fields.Sorted().Equal(tt.fields), "%s Fields.UnmarshalJSON invalid fields %v != %v", tt.name, fields, tt.fields)
		})
	}
}

// func TestFields_MatchSorted(t *testing.T) {
// 	{
// 		match := evdb.Fields{
// 			{Label: "color", Value: "blue"},
// 		}
// 		fields := evdb.Fields{
// 			{Label: "color", Value: "blue"},
// 			{Label: "taste", Value: "sour"},
// 		}
// 		ok := fields.MatchSorted(match)
// 		if !ok {
// 			t.Errorf("No match")
// 		}
// 	}
// 	{
// 		match := evdb.Fields{
// 			{Label: "color", Value: "blue"},
// 			{Label: "color", Value: "green"},
// 			{Label: "taste", Value: "bitter"},
// 		}
// 		fields := evdb.Fields{
// 			{Label: "color", Value: "green"},
// 			{Label: "shape", Value: "round"},
// 			{Label: "taste", Value: "bitter"},
// 		}
// 		if !fields.MatchSorted(match) {
// 			t.Errorf("No match")
// 		}

// 	}
// 	{
// 		match := evdb.Fields{
// 			{Label: "color", Value: "blue"},
// 			{Label: "color", Value: "green"},
// 			{Label: "taste", Value: "bitter"},
// 		}
// 		fields := evdb.Fields{
// 			{Label: "color", Value: "green"},
// 			{Label: "taste", Value: "sweet"},
// 		}
// 		if fields.MatchSorted(match) {
// 			t.Errorf("Invalid match")
// 		}

// 	}

// }
