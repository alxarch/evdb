package mdbredis

import "testing"

func TestParseFields(t *testing.T) {
	fields := parseFields(nil, "")
	if len(fields) != 0 {
		t.Errorf("Invalid fields")
	}
	{
		fields := parseFields(nil, "bar\x1ffoo")
		if len(fields) != 1 {
			t.Errorf("Invalid fields")
		}
		f := fields[0]
		if f.Label != "bar" || f.Value != "foo" {
			t.Errorf("Invalid field %s %s", f.Label, f.Value)
		}

	}

}
