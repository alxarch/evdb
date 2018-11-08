package meter

import (
	"encoding/json"
	"sort"
)

type Field struct {
	Label string
	Value string
}
type Fields []Field

func (fields Fields) Get(label string) string {
	for i := range fields {
		f := &fields[i]
		if f.Label == label {
			return f.Value
		}
	}
	return ""
}

func (fields Fields) Grow(size int) Fields {
	if size > 0 {
		size += len(fields)
		if 0 <= size && size < cap(fields) {
			return fields[:size]
		}
		tmp := make([]Field, size)
		copy(tmp, fields)
		return tmp
	}
	return fields
}

func (fields Fields) AppendRawString(s string) Fields {
	if len(s) > 0 {
		i := len(fields)
		fields = fields.Grow(int(s[0]))
		s = string(s[1:])
		var f *Field
		for len(s) > 0 {
			n := int(s[0])
			s = s[1:]
			if 0 <= n && n <= len(s) {
				if f != nil {
					f.Value = s[:n]
					f = nil
				} else if 0 <= i && i < len(fields) {
					f = &fields[i]
					f.Label = s[:n]
					i++
				} else {
					break
				}
				s = s[n:]
			}
		}
		if 0 <= i && i <= len(fields) {
			return fields[:i]
		}

	}
	return fields

}

func FieldsFromString(s string) (fields Fields) {
	return fields.AppendRawString(s)
}

// MatchSorted matches 2 sets of sorted fields.
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
		return false
	}
	return len(match) == 0
}

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
func (fields Fields) MarshalJSON() ([]byte, error) {
	return json.Marshal(fields.Map())
}

func (fields Fields) Copy() Fields {
	if fields == nil {
		return nil
	}
	cp := make([]Field, len(fields))
	copy(cp, fields)
	return cp
}
func (fields Fields) Sorted() Fields {
	fields = fields.Copy()
	sort.Stable(fields)
	return fields
}
