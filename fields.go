package meter

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
)

type Field struct {
	Label string
	Value string
}
type Fields []Field

func FieldsFromString(s string) (fields Fields) {
	if len(s) > 0 {
		fields = make([]Field, s[0])
		s = string(s[1:])
		var f *Field
		i := 0
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
		return fields[:i]
	}
	return
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

func (fields Fields) Filter(labels ...string) Fields {
	if len(labels) == 0 {
		return fields
	}
	n := 0
	for i := range fields {
		f := &fields[i]
		if indexOf(labels, f.Label) == -1 {
			continue
		}
		fields[n] = *f
		n++
	}
	return fields[:n]
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

func ZipFields(dst []byte, labels, values []string) []byte {
	b := byte(len(labels))
	dst = append(dst, b)
	for i, label := range labels {
		b = byte(len(label))
		dst = append(dst, b)
		dst = append(dst, label...)
		if 0 <= i && i < len(values) {
			label = values[i]
			b = byte(len(label))
			dst = append(dst, b)
			dst = append(dst, label...)
		}
	}
	return dst
}

var rawMatcherPool sync.Pool

func getRawMatcher() *rawFieldMatcher {
	if x := rawMatcherPool.Get(); x != nil {
		return x.(*rawFieldMatcher)
	}
	return new(rawFieldMatcher)
}
func putRawMatcher(m *rawFieldMatcher) {
	if m != nil {
		m.n = -1
		rawMatcherPool.Put(m)
	}
}

type rawFieldMatcher struct {
	buffer []byte
	kvs    [][]byte
	n      int
}

func (raw *rawFieldMatcher) indexOf(kv []byte) int {
	for i, match := range raw.kvs {
		if bytes.Equal(kv, match) {
			return i
		}
	}
	return -1
}

func (raw *rawFieldMatcher) Reset(fields Fields) {
	b := raw.buffer[:0]
	kvs := raw.kvs[:0]
	n := 0
	for i := range fields {
		f := &fields[i]
		if fields[:i].IndexOf(f.Label) == -1 {
			n++
		}
		offset := len(b)
		b = append(b, byte(len(f.Label)))
		b = append(b, f.Label...)
		b = append(b, byte(len(f.Value)))
		b = append(b, f.Value...)
		kvs = append(kvs, b[offset:])
	}
	if len(kvs) < len(raw.kvs) {
		tmp := raw.kvs[len(kvs):]
		for i := range tmp {
			tmp[i] = nil
		}
	}
	raw.kvs = kvs
	raw.buffer = b
	raw.n = n
}

func (raw *rawFieldMatcher) MatchRawFields(fields []byte) bool {
	if raw.n == 0 {
		return true
	}
	if len(fields) > 0 {
		numFields := fields[0]
		fields = fields[1:]
		total := 0
		for ; len(fields) > 0 && numFields > 0; numFields-- {
			n := uint(fields[0])
			tail := fields[1:]
			if n <= uint(len(tail)) {
				n += uint(tail[n]) + 2
				if n <= uint(len(fields)) {
					tail = fields[:n]
					fields = fields[n:]
					for _, kv := range raw.kvs {
						if bytes.Equal(kv, tail) {
							total++
							break
						}
					}
				}
			}
		}
		return total == raw.n
	}
	return false
}

type RawFields []byte

func (raw RawFields) IndexOf(k, v []byte) int {
	if len(raw) > 0 {
		n := int(raw[0])
		raw = raw[1:]
		var p []byte
		for i := 0; i < n; i++ {
			p, raw = raw.read()
			if p == nil {
				return -1
			}
			if bytes.Equal(k, p) {
				p, raw = raw.read()
				if bytes.Equal(v, p) {
					return i
				}
			} else {
				_, raw = raw.read()
			}
		}
	}
	return -1
}

func (raw RawFields) read() ([]byte, RawFields) {
	if len(raw) > 0 {
		n := int(raw[0])
		raw = raw[1:]
		if 0 <= n && n < len(raw) {
			return []byte(raw[:n]), raw[n:]
		}
	}
	return nil, raw
}

func (raw RawFields) Len() int {
	if len(raw) > 0 {
		return int(raw[0])
	}
	return -1
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
