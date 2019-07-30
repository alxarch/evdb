package evdb

import (
	"encoding/json"
	"sort"

	"github.com/alxarch/evdb/blob"
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

// UnmarshalJSON implements json.Unmarshaler interface
func (fields *Fields) UnmarshalJSON(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	f := (*fields)[:0]
	for label, value := range m {
		f = f.Set(label, value)
	}
	*fields = f
	return nil
}

// MarshalJSON implements json.Marshaler interface
func (fields Fields) MarshalJSON() ([]byte, error) {
	return json.Marshal(fields.Map())
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

// Set sets a label to a value
func (fields Fields) Set(label, value string) Fields {
	for i := range fields {
		f := &fields[i]
		if f.Label == label {
			f.Value = value
			return fields
		}
	}
	return append(fields, Field{label, value})
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

// AppendBlob implements blob.Appender interface
func (fields Fields) AppendBlob(b []byte) ([]byte, error) {
	b = blob.WriteU32BE(b, uint32(len(fields)))
	for i := range fields {
		f := &fields[i]
		b = blob.WriteString(b, f.Label)
		b = blob.WriteString(b, f.Value)
	}
	return b, nil
}

// ShiftBlob implements blob.Shifter interface
func (fields Fields) FromBlob(b []byte) (Fields, []byte) {
	n, b := blob.ReadU32BE(b)
	var label, value string
	for ; len(b) > 0 && n > 0; n-- {
		label, b = blob.ReadString(b)
		value, b = blob.ReadString(b)
		fields = append(fields, Field{
			Label: label,
			Value: value,
		})
	}
	return fields, b
}

// ShiftBlob implements blob.Shifter interface
func (fields *Fields) ShiftBlob(b []byte) ([]byte, error) {
	*fields, b = (*fields).FromBlob(b)
	return b, nil
}

func (fields *Fields) UnmarshalBinary(b []byte) error {
	*fields, _ = (*fields).FromBlob(b)
	return nil
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

func (fields Fields) AppendGrouped(grouped Fields, empty string, groups []string) Fields {
	for _, label := range groups {
		value, ok := fields.Get(label)
		if !ok {
			value = empty
		}
		grouped = append(grouped, Field{
			Label: label,
			Value: value,
		})
	}
	return grouped
}

func (fields Fields) Sorted() Fields {
	sorted := fields.Copy()
	sort.Sort(sorted)
	return sorted
}
