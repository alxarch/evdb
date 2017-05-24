package meter

import "time"

type Filter struct {
	maxage time.Duration
	res    *Resolution
	dims   [][]string

	maxdimsize int
	attrmask   map[string]bool
}

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

// MatchQuery finds the first matching dimension for a query and returns an Attributes slice
func (t *Filter) MatchQuery(q map[string]string) Attributes {
	if q == nil {
		return nil
	} else if len(q) == 0 {
		return Attributes{}
	}
	pairs := fullAttributes(2*t.MaxDimSize() + 10)
dim_loop:
	for _, dim := range t.dims {
		i := 0
		for _, d := range dim {
			pairs[i] = d
			if value := q[d]; value != "" {
				i++
				pairs[i] = value
				i++
			} else {
				continue dim_loop
			}
		}
		return pairs[:i]
	}
	PoolAttributes(pairs)
	return nil
}
