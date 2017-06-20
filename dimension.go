package meter

import (
	"sort"
)

type Dimension []string

func LabelDimensions(labels ...string) []Dimension {
	size := (1 << uint(len(labels))) - 1 // len ** 2 - 1
	if size < 0 {
		return []Dimension{}
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

func Dim(labels ...string) Dimension {
	lmap := make(map[string]bool)
	for _, label := range labels {
		if label = Label(label); label != "" {
			lmap[label] = true
		}
	}
	dim := make([]string, 0, len(labels))
	for label, _ := range lmap {
		dim = append(dim, label)
	}
	sort.Strings(dim)
	return dim
}

type Dimensions []Dimension

func Dims(dims ...[]string) Dimensions {
	if len(dims) == 0 {
		return Dimensions{}
	}
	out := Dimensions(make([]Dimension, 0, len(dims)))
	for _, labels := range dims {
		if dim := Dim(labels...); len(dim) > 0 {
			out = append(out, dim)
		}
	}
	// Sort dimensions on length in descending order
	sort.Slice(dims, func(i int, j int) bool {
		return len(dims[i]) > len(dims[j])
	})
	return out
}
