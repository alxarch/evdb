package meter

import (
	"strconv"
	"time"

	tc "github.com/alxarch/go-timecodec"
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
	Name  string
	codec tc.TimeCodec
	unit  time.Duration

	parseRange DateRangeParserFunc
}

const (
	Minly   = 60 * time.Second
	Hourly  = time.Hour
	Daily   = 24 * time.Hour
	Weekly  = 7 * Daily
	Monthly = 30 * Daily
	Yearly  = 365 * Daily
)

var (
	NoResolution     = NewResolution("totals", 0, NoResolutionCodec)
	ResolutionHourly = NewResolution("hourly", Hourly, tc.LayoutCodec(HourlyDateFormat))
	ResolutionDaily  = NewResolution("daily", Daily, tc.LayoutCodec(DailyDateFormat))
	ResolutionMinly  = NewResolution("minly", Minly, tc.LayoutCodec(MinlyDateFormat))
	ResolutionWeekly = NewResolution("weekly", Weekly, tc.ISOWeekCodec)
)

func NewResolution(name string, unit time.Duration, codec tc.TimeCodec) *Resolution {
	if codec == nil {
		codec = tc.NewTimeCodec(func(t time.Time) string {
			return strconv.FormatInt(t.UnixNano()/int64(unit), 10)
		}, func(s string) (t time.Time, err error) {
			var n int64
			if n, err = strconv.ParseInt(s, 10, 64); err != nil {
				return
			}
			return time.Time{}.Add(time.Duration(n) * time.Duration(unit)), nil

		})
	}
	parser := DateRangeParser(codec)
	return &Resolution{name, codec, unit, parser}
}

func (r *Resolution) ParseRange(s, e string, max time.Duration) (start, end time.Time, err error) {
	return r.parseRange(s, e, max)
}

func (r *Resolution) Round(t time.Time) time.Time {
	t, _ = r.UnmarshalTime(r.MarshalTime(t))
	return t
}

func (r *Resolution) Duration() time.Duration {
	return r.unit
}

func (r *Resolution) TimeSequence(s, e time.Time) []time.Time {
	return TimeSequence(r.Round(s), r.Round(e), r.unit)
}

func (r *Resolution) UnmarshalTime(s string) (t time.Time, err error) {
	return r.codec.UnmarshalTime(s)
}

func (r *Resolution) MarshalTime(t time.Time) string {
	return r.codec.MarshalTime(t)
}

func (r *Resolution) Key(event string, t time.Time) string {
	return Join(":", "stats", r.Name, r.MarshalTime(t), event)
}
