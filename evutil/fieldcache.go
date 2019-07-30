package evutil

import (
	"sort"
	"sync"

	db "github.com/alxarch/evdb"
)

// FieldCache is an in memory cache of field ids
type FieldCache struct {
	mu     sync.RWMutex
	ids    map[string]uint64
	fields map[uint64]db.Fields
}

// Set set a field to an id
func (c *FieldCache) Set(id uint64, fields db.Fields) db.Fields {
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
		c.fields = make(map[uint64]db.Fields)
	}
	c.fields[id] = fields
	c.mu.Unlock()
	return fields
}

// SetBlob sets a raw field value to an id
func (c *FieldCache) SetBlob(id uint64, blob []byte) db.Fields {
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
		c.fields = make(map[uint64]db.Fields)
	}
	c.fields[id] = fields
	c.mu.Unlock()
	return fields
}

// ID gets the id of fields
func (c *FieldCache) ID(fields db.Fields) (uint64, bool) {
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
func (c *FieldCache) Fields(id uint64) (fields db.Fields) {
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

func distinctSorted(ss []string) []string {
	var (
		i    int
		last string
	)
	for _, s := range ss {
		if i == 0 || s != last {
			last = s
			ss[i] = s
			i++
		}
	}
	return ss[:i]
}
