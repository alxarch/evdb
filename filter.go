package meter

import "time"

type Filter struct {
	maxage time.Duration
	res    *Resolution
	dims   [][]string // Dimensions for this filter

}

// NewFilter creates and initializes a new Filter
func NewFilter(res *Resolution, maxage time.Duration, dims ...[]string) *Filter {
	f := &Filter{
		maxage: maxage,
		res:    res,
		dims:   make([][]string, 0, len(dims)),
	}
	for _, dim := range dims {
		if len(dim) > 0 {
			fdim := make([]string, 0, len(dim))
			for _, d := range dim {
				if d != "" {
					fdim = append(fdim, d)
				}
			}
			if len(fdim) > 0 {
				f.dims = append(f.dims, fdim)
			}
		}
	}

	return f
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
