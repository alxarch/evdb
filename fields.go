package meter

import (
	"encoding/json"
	"sort"
)

// Field is a Label/Value pair
type Field struct {
	Label string
	Value string
}

// Fields is a collection of Label/Value pairs
type Fields []Field

// Reset resets Fields to empty
func (fields Fields) Reset() Fields {
	for i := range fields {
		fields[i] = Field{}
	}
	return fields[:0]
}

// Get returns the value of label
func (fields Fields) Get(label string) (string, bool) {
	for i := range fields {
		f := &fields[i]
		if f.Label == label {
			return f.Value, true
		}
	}
	return "", false
}

// AppendTo serializes Fields to binary data by appending
func (fields Fields) AppendTo(dst []byte) []byte {
	dst = appendUint32(dst, uint32(len(fields)))
	for i := range fields {
		f := &fields[i]
		dst = appendUint32(dst, uint32(len(f.Label)))
		dst = append(dst, f.Label...)
		dst = appendUint32(dst, uint32(len(f.Value)))
		dst = append(dst, f.Value...)
	}
	return dst
}

// ShiftFrom deserializes Fields from binary data
func (fields Fields) ShiftFrom(data []byte) (Fields, []byte) {
	_, data = shiftUint32(data)
	var label, value string
	for len(data) > 0 {
		label, data = shiftString(data)
		value, data = shiftString(data)
		fields = append(fields, Field{Label: label, Value: value})
	}
	return fields, data
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interfacee
func (fields *Fields) UnmarshalBinary(data []byte) error {
	*fields, _ = (*fields).ShiftFrom(data)
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler interfacee
func (fields Fields) MarshalBinary() ([]byte, error) {
	return fields.AppendTo(nil), nil
}

// MarshalJSON implements json.Marshaler interface
func (fields Fields) MarshalJSON() ([]byte, error) {
	return json.Marshal(fields.Map())
}

// UnmarshalJSON implements json.Marshaler interface
func (fields *Fields) UnmarshalJSON(data []byte) error {
	values := make(map[string]string)
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	fs := (*fields)[:0]
	for label := range values {
		fs = append(fs, Field{Label: label, Value: values[label]})
	}
	*fields = fs
	return nil
}

// GroupBy groups fields by labels
func (fields Fields) GroupBy(empty string, groups []string) Fields {
	grouped := make([]Field, len(groups))
	for i, label := range groups {
		value, ok := fields.Get(label)
		if !ok {
			value = empty
		}
		grouped[i] = Field{
			Label: label,
			Value: value,
		}
	}
	return grouped
}

// AppendValues appends the values for the labels in order
func (fields Fields) AppendValues(dst []string, empty string, labels ...string) []string {
	for _, label := range labels {
		v, ok := fields.Get(label)
		if !ok {
			v = empty
		}
		dst = append(dst, v)
	}
	return dst
}

// Equal checks if two Fields are equal
func (fields Fields) Equal(other Fields) bool {
	if len(fields) == len(other) {
		other = other[:len(fields)]
		for i := range fields {
			f := &fields[i]
			o := &other[i]
			if o.Label == f.Label && o.Value == f.Value {
				continue
			}
			return false
		}
		return true
	}
	return false
}

func (fields Fields) Len() int {
	return len(fields)
}

func (fields Fields) Swap(i, j int) {
	fields[i], fields[j] = fields[j], fields[i]
}

func (fields Fields) Less(i, j int) bool {
	return fields[i].Label < fields[j].Label
}

// Map converts a Fields collection to a map
func (fields Fields) Map() map[string]string {
	if fields == nil {
		return nil
	}
	m := make(map[string]string, len(fields))
	for i := range fields {
		f := &fields[i]
		m[f.Label] = f.Value
	}
	return m
}

// Copy clones a collection of fields
func (fields Fields) Copy() Fields {
	if fields == nil {
		return nil
	}
	cp := make([]Field, len(fields))
	copy(cp, fields)
	return cp
}

// Sorted returns a copy of fields sorted by label
func (fields Fields) Sorted() Fields {
	fields = fields.Copy()
	sort.Stable(fields)
	return fields
}

// ZipFields creates a field collection zipping labels and values
func ZipFields(labels []string, values []string) (fields Fields) {
	for i, label := range labels {
		if 0 <= i && i < len(values) {
			fields = append(fields, Field{
				Label: label,
				Value: values[i],
			})
		} else {
			fields = append(fields, Field{
				Label: label,
			})
		}
	}
	return

}

// MatchSorted matches fields agaist a sorted collection of Fields
func (fields Fields) MatchSorted(match Fields) bool {
next:
	for i := range fields {
		f := &fields[i]
		for j := range match {
			m := &match[j]
			if m.Label != f.Label {
				if j == 0 {
					// The match fields do not contain f.Label at all
					continue next
				}
				return false
			}
			if m.Value == f.Value {
				match = match[j:]
				// Skip label
				for j = range match {
					m = &match[j]
					if m.Label != f.Label {
						match = match[j:]
						continue next
					}
				}
				match = nil
				continue next
			}
		}
		// return len(match) == 0
	}
	return len(match) == 0

}
