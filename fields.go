package evdb

import (
	"encoding/json"
	"sort"
	"sync"

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

// Copy clones a collection of fields
func (fields Fields) Copy() Fields {
	if fields == nil {
		return nil
	}
	cp := make([]Field, len(fields))
	copy(cp, fields)
	return cp
}

// FieldCache is an in memory cache of field ids
type FieldCache struct {
	mu     sync.RWMutex
	ids    map[string]uint64
	fields map[uint64]Fields
}

// Set set a field to an id
func (c *FieldCache) Set(id uint64, fields Fields) Fields {
	c.mu.Lock()
	if fields := c.fields[id]; fields != nil {
		c.mu.Unlock()
		return fields
	}
	if c.ids == nil {
		c.ids = make(map[string]uint64)
	}
	raw, _ := fields.AppendBlob(nil)
	c.ids[string(raw)] = id
	if c.fields == nil {
		c.fields = make(map[uint64]Fields)
	}
	c.fields[id] = fields
	c.mu.Unlock()
	return fields
}

// SetRaw sets a raw field value to an id
func (c *FieldCache) SetBlob(id uint64, blob []byte) Fields {
	c.mu.Lock()
	fields := c.fields[id]
	if fields != nil {
		c.mu.Unlock()
		return fields
	}
	if c.ids == nil {
		c.ids = make(map[string]uint64)
	}
	fields, _ = fields.FromBlob(blob)
	c.ids[string(blob)] = id
	if c.fields == nil {
		c.fields = make(map[uint64]Fields)
	}
	c.fields[id] = fields
	c.mu.Unlock()
	return fields
}

// ID gets the id of fields
func (c *FieldCache) ID(fields Fields) (uint64, bool) {
	raw, _ := fields.AppendBlob(nil)
	return c.BlobID(raw)
}

// BlobID returns the id of raw fields
func (c *FieldCache) BlobID(blob []byte) (id uint64, ok bool) {
	c.mu.RLock()
	id, ok = c.ids[string(blob)]
	c.mu.RUnlock()
	return
}

// Fields gets fields by id
func (c *FieldCache) Fields(id uint64) (fields Fields) {
	c.mu.RLock()
	fields = c.fields[id]
	c.mu.RUnlock()
	return
}

// Labels returns the distinct cached labels
func (c *FieldCache) Labels() (labels []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, fields := range c.fields {
		for i := range fields {
			f := &fields[i]
			labels = append(labels, f.Label)
		}
	}
	sort.Strings(labels)
	return distinctSorted(labels)
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
