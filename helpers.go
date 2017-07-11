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
	qs := input.Encode()
	parts := strings.Split(qs, "&")

	done := map[string]bool{}
	results := [][]string{}
	output := func(pp []string) {
		qs := strings.Join(pp, "&")
		q, _ := url.ParseQuery(qs)
		for k, v := range q {
			q.Set(k, v[0])
		}
		qs = q.Encode()
		if !done[qs] {
			p := make([]string, 0, 2*len(q))
			for k, values := range q {
				p = append(p, k, values[0])
			}
			done[qs] = true
			results = append(results, p)
		}

	}
	var generate func(int, []string)
	generate = func(n int, pairs []string) {
		if n == 1 {
			output(pairs)
		} else {
			for i := 0; i < n-1; i++ {
				generate(n-1, pairs)
				tmp := pairs[n-1]
				if (n % 2) == 0 {
					pairs[n-1] = pairs[i]
					pairs[i] = tmp
				} else {
					pairs[n-1] = pairs[0]
					pairs[0] = tmp
				}
			}
			generate(n-1, pairs)
		}
	}
	generate(len(parts), parts)

	return results
}
