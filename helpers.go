package meter

import (
	"context"
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
		min := end.Add(-max)
		if start.IsZero() || start.After(end) || start.Before(min) {
			start = min
		}
		return
	}
}

func TimeSequence(start time.Time, end time.Time, unit time.Duration) []time.Time {
	start = start.Round(unit)
	end = end.Round(unit)
	if unit == 0 {
		return []time.Time{}
	}
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

func SetInterval(d time.Duration, callback func(tm time.Time)) (cancel func()) {
	done := make(chan struct{})
	cancel = func() {
		close(done)
	}
	go RunInterval(d, callback, done)
	return
}

func RunInterval(d time.Duration, callback func(tm time.Time), done <-chan struct{}) {
	tick := time.NewTicker(d)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case t := <-tick.C:
			callback(t)
		}
	}
}

func SetIntervalContext(parent context.Context, d time.Duration, callback func(tm time.Time)) (ctx context.Context, cancel context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel = context.WithCancel(parent)
	go RunInterval(d, callback, ctx.Done())
	return

}

// type Interval struct {
// 	cancel   context.CancelFunc
// 	interval time.Duration
// }
//
// func NewInterval(dt time.Duration, callback func(t time.Time)) *Interval {
// 	return &Interval{
// 		cancel:   SetInterval(dt, callback),
// 		interval: dt,
// 	}
// }
//
// func (i *Interval) Close() {
// 	i.cancel()
// }
// func (i *Interval) Interval() time.Duration {
// 	return i.interval
// }
