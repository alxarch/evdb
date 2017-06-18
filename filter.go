package meter

import "time"

type Filter struct {
	maxage time.Duration
	res    *Resolution
	dims   [][]string // Dimensions for this filter

	maxdimsize int
	attrmask   map[string]bool // Needed attributes
}

// NewFilter creates and initializes a new Filter
func NewFilter(res *Resolution, maxage time.Duration, dims ...[]string) *Filter {
	f := &Filter{
		maxage:     maxage,
		attrmask:   make(map[string]bool),
		res:        res,
		dims:       dims,
		maxdimsize: 0,
	}
	for _, dim := range f.dims {
		if n := len(dim); n > f.maxdimsize {
			f.maxdimsize = n
		}
		for _, d := range dim {
			f.attrmask[d] = true
		}
	}

	return f
}

func (f *Filter) MaxDimSize() int {
	return f.maxdimsize
}
func (f *Filter) MaxAge() time.Duration {
	return f.maxage
}

func (f *Filter) Dimensions() [][]string {
	return f.dims
}
func (f *Filter) Resolution() *Resolution {
	return f.res
}

func (t *Filter) NeedsAttr(a string) bool {
	return t.attrmask[a]
}
