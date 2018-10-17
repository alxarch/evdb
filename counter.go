package meter

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
)

type Counter struct {
	n      int64
	values []string
}

type Snapshot []Counter

func (c *Counter) Values() []string {
	return c.values
}

func (c *Counter) Count() int64 {
	return c.n
}

func (c *Counter) Match(values []string) bool {
	if len(c.values) == len(values) {
		values = values[:len(c.values)]
		for i := range c.values {
			if c.values[i] == values[i] {
				continue
			}
			return false
		}
		return true
	}
	return false
}

func (c *Counter) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 {
		if data[0] == '[' && data[len(data)-1] == ']' {
			data = bytes.TrimSpace(data[1 : len(data)-1])
			pos := bytes.IndexByte(data, ',')
			if pos == -1 {
				return nil
			}
			n, err := strconv.ParseInt(string(data[:pos]), 10, 64)
			if err != nil {
				return err
			}
			c.n = n
			return json.Unmarshal(data[pos+1:], &c.values)
		}
		if data[0] == 'n' {
			if string(data) == "null" {
				return nil
			}
		}
	}
	return errors.New("Invalid JSON data")

}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	return s.AppendJSON(nil), nil
}

func (c *Counter) MarshalJSON() ([]byte, error) {
	return c.AppendJSON(nil), nil
}

func (c *Counter) AppendJSON(dst []byte) []byte {
	dst = append(dst, '[')
	dst = strconv.AppendInt(dst, c.Count(), 10)
	dst = append(dst, ',', '[')
	for i, v := range c.Values() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, '"')
		dst = append(dst, v...)
		dst = append(dst, '"')
	}
	dst = append(dst, ']', ']')
	return dst
}

func (s Snapshot) AppendJSON(dst []byte) []byte {
	dst = append(dst, '[')
	for i := range s {
		if i > 0 {
			dst = append(dst, ',')
		}
		c := &s[i]
		dst = c.AppendJSON(dst)
	}
	dst = append(dst, ']')
	return dst
}

// type Counters struct {
// 	index    map[uint64][]int
// 	counters []Counter
// }

// func (cc *Counters) add(n int64, h uint64, values []string) int64 {
// 	id := len(cc.counters)
// 	cc.counters = append(cc.counters, Counter{
// 		values: values,
// 		n:      n,
// 	})
// 	if cc.index == nil {
// 		cc.index = make(map[uint64][]int, 64)
// 	}
// 	cc.index[h] = append(cc.index[h], id)
// 	return n
// }

// func (cc *Counters) Add(n int64, values ...string) int64 {
// 	h := valuesHash(values)
// 	for _, i := range cc.index[h] {
// 		if 0 <= i && i < len(cc.counters) {
// 			c := &cc.counters[i]
// 			if c.Match(values) {
// 				return c.Add(n)
// 			}
// 		}
// 	}
// 	return cc.add(n, h, vcopy(values))
// }

// func (cc *Counters) Len() int {
// 	return len(cc.counters)
// }

// func (cc *Counters) Flush(s Snapshot) Snapshot {
// 	s = append(s, cc.counters...)
// 	for i := range cc.counters {
// 		c := &cc.counters[i]
// 		c.n = 0
// 	}
// 	return s
// }

// func (cc *Counters) Merge(s Snapshot) {
// 	for i := range s {
// 		c := &s[i]
// 		cc.Add(c.n, c.values...)
// 	}
// }

// type CounterAtomic struct {
// 	n      int64
// 	values []string
// }

// func (c *CounterAtomic) Match(values []string) bool {
// 	if len(values) == len(c.values) {
// 		values = values[:len(c.values)]
// 		for i, v := range c.values {
// 			if values[i] == v {
// 				continue
// 			}
// 			return false
// 		}
// 		return true
// 	}
// 	return false
// }

// func (c *CounterAtomic) Count() int64 {
// 	return atomic.LoadInt64(&c.n)
// }
// func (c *CounterAtomic) Add(n int64) int64 {
// 	return atomic.AddInt64(&c.n, n)
// }
// func (c *CounterAtomic) Set(n int64) int64 {
// 	return atomic.SwapInt64(&c.n, n)
// }

// func matchRawValues(tag string, values []string) bool {
// 	for _, v := range values {
// 		if len(tag) > 0 {
// 			if int(tag[0]) == len(v) {
// 				if tag = tag[1:]; len(tag) >= len(v) && tag[0:len(v)] == v {
// 					tag = tag[len(v):]
// 					continue
// 				}
// 			}
// 		}
// 		return false
// 	}
// 	return true

// }

// func joinValues(values []string) string {
// 	s := strings.Builder{}
// 	s.Grow(len(values) * 64)
// 	for _, v := range values {
// 		if len(v) < maxValueSize {
// 			s.WriteByte(byte(len(v)))
// 			s.WriteString(v)
// 		} else {
// 			s.WriteByte(maxValueSize)
// 			s.WriteString(v[:maxValueSize])
// 		}
// 	}
// 	return s.String()
// }

// func SplitValues(s string) []string {
// 	return appendRawValues(nil, s)
// }

// func appendRawValues(v []string, s string) []string {
// 	for len(s) > 0 {
// 		size := int(s[0])
// 		s = s[1:]
// 		if 0 <= size && size <= len(s) {
// 			v = append(v, s[:size])
// 			s = s[size:]
// 		}
// 	}
// 	return v
// }
