package meter2

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type Dimension []string

// LabelDimensions creates all possible label combinations for a set of labels
func LabelDimensions(labels ...string) Dimensions {
	size := (1 << uint(len(labels))) - 1 // len ** 2 - 1
	if size < 0 {
		return Dimensions{}
	}
	result := make([]Dimension, 0, size)
	for _, v := range labels {
		for _, r := range result {
			rr := make([]string, len(r), len(r)+1)
			copy(rr, r)
			rr = append(rr, v)
			result = append(result, rr)
		}
		result = append(result, []string{v})
	}
	return result
}

func QueryDimension(q map[string]string, labels []string) Dimension {
	if q == nil {
		return nil
	}
	dim := Dimension(make([]string, len(q)))
	j := 0
	for i := 0; i < len(labels); i++ {
		if _, ok := q[labels[i]]; ok {
			dim[j] = labels[i]
			j++
		}
	}
	return dim[:j]
}

func (dim Dimension) Contains(label string) bool {
	if dim == nil {
		return false
	}
	for _, d := range dim {
		if d == label {
			return true
		}
	}
	return false
}

func (dim Dimension) SubsetOf(other Dimension) bool {
dimloop:
	for _, d := range dim {
		for _, ok := range other {
			if d == ok {
				continue dimloop
			}
		}
		return false
	}
	return true
}

func (d Dimension) Copy() Dimension {
	if d == nil {
		return nil
	}
	cp := make([]string, len(d))
	copy(cp, d)
	return cp
}

type Dimensions []Dimension

func (dims Dimensions) SubsetOf(labels []string) bool {
	for i := 0; i < len(dims); i++ {
		if !dims[i].SubsetOf(Dimension(labels)) {
			return false
		}
	}
	return true
}

func (dims Dimensions) Sort(labels []string) {
	if len(dims) == 0 {
		return
	}
	index := PositionIndex(labels)
	dims.SortBy(index)
}
func (dim Dimension) SortBy(index map[string]int) {
	if index == nil {
		return
	}
	sort.Slice(dim, func(i int, j int) bool {
		return index[dim[i]] < index[dim[j]]
	})
}
func (dims Dimensions) SortBy(index map[string]int) {
	if len(dims) == 0 {
		return
	}
	if index != nil {
		for _, dim := range dims {
			dim.SortBy(index)
		}
	}
	sort.Slice(dims, func(i, j int) bool {
		return len(dims[i]) < len(dims[j])
	})
}
func DistinctDimensions(dims ...Dimension) Dimensions {
	index := make(map[string]Dimension)
	sep := string([]byte{LabelSeparator})
	i := 0
	for _, dim := range dims {
		key := strings.Join(dim, sep)
		if index[key] == nil {
			index[key] = dim
			i++
			dims[i] = dim
		}
	}
	return dims[:i]
}

func (dims Dimensions) Copy() (cp Dimensions) {
	if dims == nil {
		return
	}
	cp = make([]Dimension, len(dims))
	for i, dim := range dims {
		cp[i] = dim.Copy()
	}
	return
}

type Layout struct {
	Resolution
	Dimensions
}

var (
	ErrNilLayout = errors.New("Layout is nil")
)

func PositionIndex(values []string) map[string]int {
	if values == nil {
		return nil
	}
	index := make(map[string]int, len(values))
	for i, s := range values {
		index[s] = i
	}
	return index
}

func (g Layout) Copy() (cp Layout) {
	cp.Resolution = g.Resolution
	cp.Dimensions = g.Dimensions.Copy()
	return
}

func (g Layout) String() string {
	return fmt.Sprintf("%s %s", g.Resolution.Name(), g.Dimensions)
}
