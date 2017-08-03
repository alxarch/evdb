package meter

import (
	"net/url"
	"strings"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/alxarch/go-timecodec"
)

type DateRangeParserFunc func(string, string, time.Duration) (time.Time, time.Time, error)

func DateRangeParser(dec tc.TimeDecoder) DateRangeParserFunc {
	return func(s, e string, max time.Duration) (start, end time.Time, err error) {
		now := time.Now()
		if e != "" {
			if end, err = dec.UnmarshalTime(e); err != nil {
				return
			}
		}
		if end.IsZero() || end.After(now) {
			end = now
		}
		if s != "" {
			if start, err = dec.UnmarshalTime(s); err != nil {
				return
			}
		}
		if max > 0 {
			min := end.Add(-max)
			if start.IsZero() || start.After(end) || start.Before(min) {
				start = min
			}
		}
		return
	}
}

func TimeSequence(start time.Time, end time.Time, unit time.Duration) []time.Time {
	if unit == 0 {
		return []time.Time{}
	}
	start = start.Round(unit)
	end = end.Round(unit)
	n := end.Sub(start) / unit

	results := make([]time.Time, 0, n)

	for s := start; end.Sub(s) >= 0; s = s.Add(unit) {
		results = append(results, s)
	}
	return results
}

func Join(sep string, parts ...string) string {
	return strings.Join(parts, sep)
}

func QueryPermutations(input url.Values) []map[string]string {
	vcount := []int{}
	keys := []string{}
	combinations := [][]int{}
	for k, v := range input {
		if c := len(v); c > 0 {
			keys = append(keys, k)
			vcount = append(vcount, c)
		}
	}
	var generate func([]int)
	generate = func(comb []int) {
		if i := len(comb); i == len(vcount) {
			combinations = append(combinations, comb)
			return
		} else {
			for j := 0; j < vcount[i]; j++ {
				next := make([]int, i+1)
				if i > 0 {
					copy(next[:i], comb)
				}
				next[i] = j
				generate(next)
			}
		}
	}
	generate([]int{})
	results := make([]map[string]string, 0, len(combinations))
	for _, comb := range combinations {
		result := make(map[string]string, len(comb))
		for i, j := range comb {
			key := keys[i]
			result[key] = input[key][j]
		}
		if len(result) > 0 {
			results = append(results, result)
		}
	}
	return results
}

// func PermutationPairs(input url.Values) [][]string {
// 	vcount := []int{}
// 	keys := []string{}
// 	combinations := [][]int{}
// 	for k, v := range input {
// 		if c := len(v); c > 0 {
// 			keys = append(keys, k)
// 			vcount = append(vcount, c)
// 		}
// 	}
// 	var generate func([]int)
// 	generate = func(comb []int) {
// 		if i := len(comb); i == len(vcount) {
// 			combinations = append(combinations, comb)
// 			return
// 		} else {
// 			for j := 0; j < vcount[i]; j++ {
// 				next := make([]int, i+1)
// 				if i > 0 {
// 					copy(next[:i], comb)
// 				}
// 				next[i] = j
// 				generate(next)
// 			}
// 		}
// 	}
// 	generate([]int{})
// 	results := [][]string{}
// 	for _, comb := range combinations {
// 		result := []string{}
// 		for i, j := range comb {
// 			key := keys[i]
// 			result = append(result, key, input[key][j])
// 		}
// 		if len(result) > 0 {
// 			results = append(results, result)
// 		}
// 	}
// 	return results
// }

const LeftDelim = "{{"
const RightDelim = "}}"

func NameTemplate(name string) (tpl *template.Template, err error) {
	var trees map[string]*parse.Tree
	if trees, err = parse.Parse(name, name, LeftDelim, RightDelim); err != nil {
		return
	}
	tree := trees[name]
	if len(tree.Root.Nodes) == 1 && tree.Root.Nodes[0].Type() == parse.NodeText {
		return
	}
	tpl, err = template.New(name).AddParseTree(name, tree)
	tpl = tpl.Option("missingkey=zero")
	return

}

// const DefaultBufferSize = 4096

// var pool = &sync.Pool{
// 	New: func() interface{} {
// 		return make([]byte, DefaultBufferSize)
// 	},
// }
//
// func fieldBuffer() []byte {
// 	return pool.Get().([]byte)
// }
// func poolBuffer(b []byte) {
// 	c := cap(b)
// 	if c < DefaultBufferSize {
// 		return
// 	}
// 	pool.Put(b[:c])
// }
//
// func (e *Event) capLabelSize(n int) int {
// 	if max := len(e.labels); n > max {
// 		return max
// 	}
// 	return n
//
// }
// func ValueField(values []string) string {
// 	switch len(values) {
// 	case 0:
// 		return ""
// 	case 1:
// 		return values[0]
// 	case 2:
// 		// Special case for common small values.
// 		// Remove if golang.org/issue/6714 is fixed
// 		return values[0] + sLabelSeparator + values[1]
// 	case 3:
// 		// Special case for common small values.
// 		// Remove if golang.org/issue/6714 is fixed
// 		return values[0] + sLabelSeparator + values[1] + sLabelSeparator + values[2]
// 	}
//
// 	b := fieldBuffer()
// 	defer poolBuffer(b)
// 	bp := copy(b, values[0])
// 	for _, s := range values[1:] {
// 		b[bp] = LabelSeparator
// 		bp++
// 		bp += copy(b[bp:], s)
// 	}
// 	return string(b[:bp])
// }

// func (e *Event) bField(labels Labels) string {
// 	if len(labels) == 0 || len(e.labels) == 0 {
// 		return sFieldTerminator
// 	}
//
// 	b := fieldBuffer()
// 	defer poolBuffer(b)
// 	bp := 0
// 	for _, label := range e.labels {
// 		if v, ok := labels[label]; ok && v != "" {
// 			if bp != 0 {
// 				b[bp] = LabelSeparator
// 				bp++
// 			}
// 			bp += copy(b[bp:], label)
// 			b[bp] = LabelSeparator
// 			bp++
// 			bp += copy(b[bp:], v)
// 		}
// 	}
// 	b[bp] = FieldTerminator
// 	bp++
// 	return string(b[:bp])
// }
