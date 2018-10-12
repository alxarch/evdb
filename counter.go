package meter

type Counter struct {
	n      int64
	values []string
}

func (c *Counter) Add(n int64) int64 {
	c.n += n
	return c.n
}

func (c *Counter) Values() []string {
	return c.values
}

func (c *Counter) Count() int64 {
	return c.n
}
func (c *Counter) Set(n int64) int64 {
	c.n, n = n, c.n
	return n
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

type Counters struct {
	index    map[uint64][]int
	counters []Counter
}

func vcopy(values []string) []string {
	cp := make([]string, len(values))
	for i, v := range values {
		cp[i] = v
	}
	return cp
}

func (cc *Counters) add(n int64, h uint64, values []string) int64 {
	id := len(cc.counters)
	cc.counters = append(cc.counters, Counter{
		values: values,
		n:      n,
	})
	if cc.index == nil {
		cc.index = make(map[uint64][]int, 64)
	}
	cc.index[h] = append(cc.index[h], id)
	return n
}

func (cc *Counters) Add(n int64, values ...string) int64 {
	h := valuesHash(values)
	for _, i := range cc.index[h] {
		if 0 <= i && i < len(cc.counters) {
			c := &cc.counters[i]
			if c.Match(values) {
				return c.Add(n)
			}
		}
	}
	return cc.add(n, h, vcopy(values))
}

func (cc *Counters) Len() int {
	return len(cc.counters)
}

func (cc *Counters) Flush(s Snapshot) Snapshot {
	s = append(s, cc.counters...)
	cc.Reset()
	return s
}

func (cc *Counters) Merge(s Snapshot) {
	for i := range s {
		c := &s[i]
		cc.Add(c.n, c.values...)
	}
}

func (cc *Counters) Reset() {
	for h, idx := range cc.index {
		ok := false
		for _, i := range idx {
			if 0 <= i && i < len(cc.counters) {
				if c := &cc.counters[i]; c.n != 0 {
					ok = true
					break
				}
			}
		}
		if ok {
			cc.index[h] = idx[:0]
		} else {
			delete(cc.index, h)
		}
	}
	cc.counters = cc.counters[:0]
}

func valuesHash(values []string) (h uint64) {
	h = hashNew()
	for _, v := range values {
		// if len(v) > maxValueSize {
		// 	v = v[:maxValueSize]
		// }
		// hashAddByte(h, byte(len(v)))
		for i := 0; 0 <= i && i < len(v); i++ {
			h = hashAddByte(h, v[i])
		}

	}
	return
}

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

func (cc *Counters) get(i int) *Counter {
	if 0 <= i && i < len(cc.counters) {
		return &cc.counters[i]
	}
	return nil
}

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
