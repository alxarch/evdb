package meter

import (
	"sort"
)

type Dimension []string

func LabelDimensions(labels ...string) []Dimension {
	result := []Dimension{}
	m := Labels(labels).Map()
	for k, v := range m {
		first := len(result) == 0

		if first {
			result = append(result, []string{k, v})

		} else {
			for _, r := range result {
				rr := make([]string, len(r), len(r)+2)
				copy(rr, r)
				rr = append(rr, k, v)
				result = append(result, rr)
			}
		}

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
