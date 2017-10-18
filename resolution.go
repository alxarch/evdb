package meter

import (
	"time"

	"github.com/alxarch/go-meter/tcodec"
)

const (
	// DailyDateFormat is the date format used by the start/end query parameters
	DailyDateFormat  string = "2006-01-02"
	HourlyDateFormat string = "2006-01-02-15"
	MinlyDateFormat  string = "2006-01-02-15-04"
)

// var NoResolutionCodec = tcodec.NewTimeCodec(func(t time.Time) string {
// 	return "*"
// }, func(s string) (t time.Time, e error) {
// 	return
// })

type Resolution struct {
	name string
	// step time.Duration
	ttl  time.Duration
	step time.Duration

	codec tcodec.TimeCodec
}

const (
	Minly   = time.Minute
	Hourly  = time.Hour
	Daily   = 24 * time.Hour
	Weekly  = 7 * Daily
	Monthly = 30 * Daily
	Yearly  = 365 * Daily
)

var (
	// NoResolution     = Resolution{"totals", 0, 0, NoResolutionCodec}
	ResolutionHourly = Resolution{"hourly", 0, Hourly, tcodec.LayoutCodec(HourlyDateFormat)}
	ResolutionDaily  = Resolution{"daily", 0, Daily, tcodec.LayoutCodec(DailyDateFormat)}
	ResolutionMinly  = Resolution{"minly", 0, Minly, tcodec.LayoutCodec(MinlyDateFormat)}
	ResolutionWeekly = Resolution{"weekly", 0, Weekly, tcodec.ISOWeekCodec}
)

func NewResolution(name string, step, ttl time.Duration) Resolution {
	return Resolution{name, ttl, step, tcodec.UnixTimeCodec(step)}
}

var zeroResolution = Resolution{}

func (r Resolution) IsZero() bool {
	return r == zeroResolution
}
func (r Resolution) Name() string {
	return r.name
}

func (r Resolution) WithName(name string) Resolution {
	r.name = name
	return r
}

func (r Resolution) TTL() time.Duration {
	return r.ttl
}

func (r Resolution) WithTTL(ttl time.Duration) Resolution {
	r.ttl = ttl
	return r
}

func (r Resolution) WithLayout(layout string) Resolution {
	r.codec = tcodec.LayoutCodec(layout)
	return r
}

func (r Resolution) WithCodec(codec tcodec.TimeCodec) Resolution {
	r.codec = codec
	return r
}

func (r Resolution) Round(t time.Time) time.Time {
	return t.Truncate(r.step)
}
func (r Resolution) AddSteps(t time.Time, n int) time.Time {
	return t.Truncate(r.step).Add(time.Duration(n) * r.Step())
}

func (r Resolution) TimeSequence(s, e time.Time) []time.Time {
	return TimeSequence(s, e, r.step)
}

// func (r Resolution) ParseDateRange(s, e string) (start, end time.Time, err error) {
// 	parser := DateRangeParser(r)
// 	return parser(s, e, r.ttl)
// }

func (r Resolution) UnmarshalTime(s string) (t time.Time, err error) {
	return r.codec.UnmarshalTime(s)
}

func (r Resolution) MarshalTime(t time.Time) string {
	if r.codec == nil {
		return t.Truncate(r.step).In(t.Location()).String()
	}
	return r.codec.MarshalTime(t)
}

func (r Resolution) Step() time.Duration {
	return r.step
}

func (r Resolution) WithStep(step time.Duration) Resolution {
	r.step = step
	return r
}

func TimeSequence(start time.Time, end time.Time, unit time.Duration) []time.Time {
	if unit == 0 {
		return []time.Time{}
	}
	start = start.Truncate(unit).In(start.Location())
	end = end.Truncate(unit).In(end.Location())
	n := end.Sub(start) / unit

	results := make([]time.Time, 0, n)

	for s := start; end.Sub(s) >= 0; s = s.Add(unit) {
		results = append(results, s)
	}
	return results
}
