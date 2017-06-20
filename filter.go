package meter

import "time"

type Filter struct {
	maxage time.Duration
	res    *Resolution
	dims   Dimensions // Dimensions for this filter

}

// NewFilter creates and initializes a new Filter
func NewFilter(res *Resolution, maxage time.Duration, dims ...[]string) *Filter {
	f := &Filter{
		maxage: maxage,
		res:    res,
	}
	f.dims = Dims(dims...)

	return f
}

func (f *Filter) MaxAge() time.Duration {
	return f.maxage
}

func (f *Filter) Dimensions() Dimensions {
	return f.dims
}
func (f *Filter) Resolution() *Resolution {
	return f.res
}
