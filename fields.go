package meter

import (
	"encoding/json"
	"sort"
	"sync"
)

type Field struct {
	Label string
	Value string
}
type Fields []Field

func (fields Fields) Reset() Fields {
	for i := range fields {
		fields[i] = Field{}
	}
	return fields[:0]
}

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

func (fields Fields) AppendTo(dst []byte) []byte {
	for i := range fields {
		f := &fields[i]
		dst = append(dst, byte(len(f.Label)))
		dst = append(dst, f.Label...)
		dst = append(dst, byte(len(f.Value)))
		dst = append(dst, f.Value...)
	}
	return dst
}

func FieldsFromString(s string) (fields Fields) {
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

// MatchSorted matches 2 sets of sorted fields.

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

type FieldMatcher interface {
	MatchFields(fields Fields) bool
}

type matchAllFields struct{}

func (matchAllFields) MatchFields(_ Fields) bool { return true }

type FieldMatcherFunc func(Fields) bool

func (f FieldMatcherFunc) MatchFields(fields Fields) bool {
	return f(fields)
}

func (fields Fields) SortedMatcher() FieldMatcherFunc {
	return fields.Sorted().MatchSorted
}

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

type FieldCache struct {
	mu     sync.RWMutex
	ids    map[string]uint64
	fields map[uint64]Fields
}

func (c *FieldCache) SetString(id uint64, s string) (fields Fields) {
	c.mu.Lock()
	if fields = c.fields[id]; fields != nil {
		c.mu.Unlock()
		return
	}
	if c.ids == nil {
		c.ids = make(map[string]uint64)
	}
	c.ids[s] = id
	fields = FieldsFromString(s)
	if c.fields == nil {
		c.fields = make(map[uint64]Fields)
	}
	c.fields[id] = fields
	c.mu.Unlock()
	return
}

func (c *FieldCache) ID(fields Fields) (uint64, bool) {
	raw := fields.AppendTo(nil)
	return c.RawID(raw)
}

func (c *FieldCache) RawID(raw []byte) (id uint64, ok bool) {
	c.mu.RLock()
	id, ok = c.ids[string(raw)]
	c.mu.RUnlock()
	return
}

func (cache *FieldCache) Fields(id uint64) (fields Fields) {
	cache.mu.RLock()
	fields = cache.fields[id]
	cache.mu.RUnlock()
	return
}

func (cache *FieldCache) Labels() (labels []string) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	for _, fields := range cache.fields {
		for i := range fields {
			f := &fields[i]
			labels = append(labels, f.Label)
		}
	}
	sort.Strings(labels)
	return distinctSorted(labels)
}

func (fields Fields) GroupBy(empty string, groups []string) Fields {
	grouped := make([]Field, len(groups))
	for i, label := range groups {
		value := fields.Get(label)
		if value == "" {
			value = empty
		}
		grouped[i] = Field{
			Label: label,
			Value: value,
		}
	}
	return grouped
}

func (fields Fields) AppendValues(dst []string, empty string, labels ...string) []string {
	for _, label := range labels {
		v := fields.Get(label)
		if v == "" {
			v = empty
		}
		dst = append(dst, v)
	}
	return dst
}

type iLabel struct {
	Label string
	Index int
}

type labelIndex []iLabel

func (index labelIndex) Len() int {
	return len(index)
}

func (index labelIndex) Less(i, j int) bool {
	return index[i].Label < index[j].Label
}

func (index labelIndex) Swap(i, j int) {
	index[i], index[j] = index[j], index[i]
}

func newLabeLIndex(labels ...string) labelIndex {
	index := labelIndex(make([]iLabel, len(labels)))
	for i, label := range labels {
		index[i] = iLabel{
			Label: label,
			Index: i,
		}
	}
	sort.Stable(index)
	return index
}

func (index labelIndex) AppendFields(dst []byte, values []string) []byte {
	dst = append(dst, byte(len(index)))
	for i := range index {
		idx := &index[i]
		dst = append(dst, byte(len(idx.Label)))
		dst = append(dst, idx.Label...)
		if 0 <= idx.Index && idx.Index < len(values) {
			v := values[idx.Index]
			dst = append(dst, byte(len(v)))
			dst = append(dst, v...)
		} else {
			dst = append(dst, 0)
		}
	}
	return dst
}
