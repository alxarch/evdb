package meter2

import (
	"time"

	tc "github.com/alxarch/go-timecodec"
	"github.com/araddon/dateparse"
)

const (
	// DailyDateFormat is the date format used by the start/end query parameters
	DailyDateFormat  string = "2006-01-02"
	HourlyDateFormat string = "2006-01-02-15"
	MinlyDateFormat  string = "2006-01-02-15-04"
)

var NoResolutionCodec = tc.NewTimeCodec(func(t time.Time) string {
	return "*"
}, func(s string) (t time.Time, e error) {
	return
})

type Resolution struct {
	name string
	// step time.Duration
	ttl  time.Duration
	step time.Duration

	codec tc.TimeCodec
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
	NoResolution     = Resolution{"totals", 0, 0, NoResolutionCodec}
	ResolutionHourly = Resolution{"hourly", 0, Hourly, tc.LayoutCodec(HourlyDateFormat)}
	ResolutionDaily  = Resolution{"daily", 0, Daily, tc.LayoutCodec(DailyDateFormat)}
	ResolutionMinly  = Resolution{"minly", 0, Minly, tc.LayoutCodec(MinlyDateFormat)}
	ResolutionWeekly = Resolution{"weekly", 0, Weekly, tc.ISOWeekCodec}
)

func NewResolution(name string, step, ttl time.Duration) Resolution {
	return Resolution{name, ttl, step, tc.UnixTimeCodec(step)}
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
	r.codec = tc.LayoutCodec(layout)
	return r
}

func (r Resolution) WithCodec(codec tc.TimeCodec) Resolution {
	r.codec = codec
	return r
}

func (r Resolution) Round(t time.Time) time.Time {
	return tc.Round(t, r.step).In(t.Location())
}

func (r Resolution) TimeSequence(s, e time.Time) []time.Time {
	return TimeSequence(s, e, r.step)
}

// func (r Resolution) ParseDateRange(s, e string) (start, end time.Time, err error) {
// 	parser := DateRangeParser(r)
// 	return parser(s, e, r.ttl)
// }

func (r Resolution) UnmarshalTime(s string) (t time.Time, err error) {
	if r.codec == nil {
		return dateparse.ParseAny(s)
	}
	return r.codec.UnmarshalTime(s)
}

func (r Resolution) MarshalTime(t time.Time) string {
	if r.codec == nil {
		return tc.Round(t, r.step).String()
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
