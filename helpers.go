package meter

import (
	"net/url"
	"strings"
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

func PermutationPairs(input url.Values) [][]string {
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
	results := [][]string{}
	for _, comb := range combinations {
		result := []string{}
		for i, j := range comb {
			key := keys[i]
			result = append(result, key, input[key][j])
		}
		if len(result) > 0 {
			results = append(results, result)
		}
	}
	return results
}

// func PermutationPairs(input url.Values) [][]string {
// 	qs := input.Encode()
// 	parts := strings.Split(qs, "&")
// 	if len(parts) > 4 {
// 		parts = parts[:4]
// 	}
//
// 	// done := map[string]bool{}
// 	results := [][]string{}
// 	// qch := make(chan string)
// 	// wg := sync.WaitGroup{}
// 	// output := func(pp []string) {
// 	// 	// qs := strings.Join(pp, "&")
// 	// 	// q, _ := url.ParseQuery(qs)
// 	// 	// for k, v := range q {
// 	// 	// 	q.Set(k, v[0])
// 	// 	// }
// 	// 	// log.Println(q.Encode())
// 	// 	// qch <- q.Encode()
// 	// 	wg.Done()
// 	// }
// 	var generate func(int, []string)
// 	t := 0
// 	generate = func(n int, pairs []string) {
// 		if n == 1 {
// 			results = append(results, pairs)
// 			// wg.Add(1)
// 			// log.Println(t)
// 			t++
// 			// go output(pairs)
// 		} else {
// 			for i := 0; i < n-1; i++ {
// 				generate(n-1, pairs)
// 				tmp := pairs[n-1]
// 				if (n % 2) == 0 {
// 					pairs[n-1] = pairs[i]
// 					pairs[i] = tmp
// 				} else {
// 					pairs[n-1] = pairs[0]
// 					pairs[0] = tmp
// 				}
// 			}
// 			generate(n-1, pairs)
// 		}
// 	}
// 	generate(len(parts), parts)
// 	log.Println(len(results))
// 	// go func() {
// 	// 	for qs := range qch {
// 	// 		log.Println("A")
// 	// 		q, _ := url.ParseQuery(qs)
// 	//
// 	// 		if !done[qs] {
// 	// 			p := make([]string, 0, 2*len(q))
// 	// 			for k, values := range q {
// 	// 				p = append(p, k, values[0])
// 	// 			}
// 	// 			done[qs] = true
// 	// 			results = append(results, p)
// 	// 		}
// 	// 	}
// 	// }()
// 	// wg.Wait()
// 	// close(qch)
//
// 	return results
// }
