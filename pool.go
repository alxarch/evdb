package meter

import "sync"

type AttributesPool struct {
	MinSize int
	pool    *sync.Pool
}

func NewPool(minsize int) *AttributesPool {
	if minsize <= 0 {
		minsize = DefaultMinSize
	}
	return &AttributesPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return Attributes(make([]string, minsize))
			},
		},
		MinSize: minsize,
	}
}

func (p *AttributesPool) Get(size int) Attributes {
	if p == nil {
		return Attributes(make([]string, size))
	}
	if size <= 0 {
		return nil
	} else if size > p.MinSize {
		return Attributes(make([]string, size))
	} else {
		r := p.pool.Get().(Attributes)
		return r[:size]
	}
}
func (p *AttributesPool) Blank(size int) Attributes {
	return p.Get(size)[:0]
}

func (p *AttributesPool) Put(r Attributes) {
	if p == nil {
		return
	}
	if cap(r) < p.MinSize {
		return
	}
	p.pool.Put(r)
}

const DefaultMinSize = 256

var defaultPool = NewPool(DefaultMinSize)
