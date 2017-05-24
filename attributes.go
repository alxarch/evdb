package meter

import "sync"

type Attributes []string

func (attr Attributes) Set(pairs ...string) Attributes {
	if n := len(pairs); 1 == n%2 {
		pairs = pairs[:n-1]
	}
	return append(attr, pairs...)
}

func (attr Attributes) Get(key string) string {
	n := len(attr)
	n -= n % 2
	for i := n - 2; i >= 0; i -= 2 {
		if attr[i] == key {
			return attr[i+1]
		}
	}
	return ""
}

func (a Attributes) Copy() Attributes {
	cp := fullAttributes(len(a))
	copy(cp, a)
	return cp
}

const DefaultAttributesSize = 64

var pool = &sync.Pool{
	New: func() interface{} {
		return Attributes(make([]string, 0, DefaultAttributesSize))
	},
}

func PoolAttributes(a Attributes) {
	pool.Put(a[:0])
}

func fullAttributes(size int) Attributes {
	blank := pool.Get().(Attributes)
	if size > cap(blank) {
		return Attributes(make([]string, size))
	}
	return blank[:size]
}

func BlankAttributes(size int) Attributes {
	blank := pool.Get().(Attributes)
	if size > cap(blank) {
		blank = Attributes(make([]string, 0, size))
	}
	return blank
}

func (attr Attributes) Map() map[string]string {
	m := make(map[string]string)
	for i := 0; i < len(attr); i += 2 {
		m[attr[i]] = attr[i+1]
	}
	return m
}
