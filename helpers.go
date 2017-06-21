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
	result := [][]string{}
	for k, vv := range input {
		first := len(result) == 0
		for i, v := range vv {
			if first {
				result = append(result, []string{k, v})
			} else if i == 0 {
				for i, r := range result {
					result[i] = append(r, k, v)
				}
			} else {
				for _, r := range result {
					rr := make([]string, len(r), len(r)+2)
					copy(rr, r)
					rr = append(rr, k, v)
					result = append(result, rr)
				}
			}
		}
	}
	return result
}

func SubQuery(q url.Values, prefix string) url.Values {
	prefix = strings.Trim(prefix, " :")
	if prefix == "" {
		return q
	}
	prefix = prefix + ":"
	labels := url.Values{}
	for name, values := range q {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		label := name[len(prefix):]
		for _, p := range values {
			labels.Add(label, p)
		}
	}
	return labels
}
