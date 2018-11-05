package meter

import (
	"encoding/json"
	"net/url"
	"strings"
)

type Field struct {
	Label string
	Value string
}
type Fields []Field

func (fields Fields) Values() (values url.Values) {
	values = make(map[string][]string, len(fields))
	for i := range fields {
		f := &fields[i]
		values[f.Label] = append(values[f.Label], f.Value)
	}
	return
}

func (fields Fields) AppendValues(values url.Values) Fields {
	for label := range values {
		for _, value := range values[label] {
			fields = append(fields, Field{Label: label, Value: value})
		}
	}
	return fields
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

func (fields Fields) AppendTo(dst []byte) []byte {
	dst = append(dst, byte(len(fields)))
	for i := range fields {
		f := &fields[i]
		dst = append(dst, byte(len(f.Label)))
		dst = append(dst, f.Label...)
		dst = append(dst, byte(len(f.Value)))
		dst = append(dst, f.Value...)
	}
	return dst
}

func (fields Fields) IndexOf(key string) int {
	for i := range fields {
		f := &fields[i]
		if f.Label == key {
			return i
		}
	}
	return -1
}
func (fields Fields) SkipLabel(label string) Fields {
	for i := range fields {
		f := &fields[i]
		if f.Label != label {
			return fields[i:]
		}
	}
	return nil
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

func (fields Fields) Filter(dst Fields, labels ...string) Fields {
	if len(labels) == 0 {
		return append(dst, fields...)
	}
	for i := range fields {
		f := &fields[i]
		for _, label := range labels {
			if label == f.Label {
				dst = append(dst, *f)
				break
			}
		}
	}
	return dst
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

func (fields Fields) AppendLabelsDistinct(labels []string) []string {
	for i := range fields {
		f := &fields[i]
		if fields[:i].IndexOf(f.Label) == -1 {
			labels = append(labels, f.Label)
		}
	}
	return labels
}
func (fields Fields) AppendLabels(labels []string) []string {
	for i := range fields {
		f := &fields[i]
		labels = append(labels, f.Label)
	}
	return labels
}

func (fields Fields) Seek(label string) Fields {
	for i := range fields {
		f := &fields[i]
		if f.Label < label {
			continue
		}
		return fields[i:]
	}
	return nil
}

func (fields Fields) MatchValues(values url.Values) bool {
	n := 0
	for i := range fields {
		f := &fields[i]
		if want, ok := values[f.Label]; ok {
			if len(want) > 0 && indexOf(want, f.Value) == -1 {
				return false
			}
			n++
		}
	}
	return n == len(values)
}

func (fields Fields) MarshalJSON() ([]byte, error) {
	return json.Marshal(fields.Map())
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

func SubValues(separator byte, values ...string) (q url.Values) {
	q = make(map[string][]string, len(values))
	for _, kv := range values {
		if pos := strings.IndexByte(kv, separator); 0 <= pos && pos < len(kv) {
			if k, v := kv[:pos], kv[pos+1:]; k != "" && v != "" {
				q[k] = append(q[k], v)
			}
		}
	}
	return

}

func SplitAppendFields(fields Fields, separator byte, values ...string) Fields {
	for _, kv := range values {
		if pos := strings.IndexByte(kv, separator); 0 <= pos && pos < len(kv) {
			if k, v := kv[:pos], kv[pos+1:]; k != "" && v != "" {
				fields = append(fields, Field{
					Label: k,
					Value: v,
				})
			}
		}
	}
	return fields

}
