package meter

import (
	"bytes"
	"sync"
)

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

type RawFieldMatcher interface {
	MatchRawFields(raw []byte) bool
}

type identityFieldMatcher struct{}

func (identityFieldMatcher) MatchRawFields(_ []byte) bool {
	return true
}
func (identityFieldMatcher) MatchFields(Fields) bool {
	return true
}

type FieldMatcher interface {
	MatchFields(fields Fields) bool
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
		} else {
			dst = append(dst, 0)
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
