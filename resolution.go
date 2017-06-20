package meter

import (
	"time"

	tc "github.com/alxarch/go-timecodec"
)

// const (
// 	// DailyDateFormat is the date format used by the start/end query parameters
// 	DailyDateFormat  string = "2006-01-02"
// 	HourlyDateFormat string = "2006-01-02-15"
// 	MinlyDateFormat  string = "2006-01-02-15-04"
// )

var NoResolutionCodec = tc.NewTimeCodec(func(t time.Time) string {
	return "*"
}, func(s string) (t time.Time, e error) {
	return
})

type Resolution struct {
	Name string
	// step time.Duration
	ttl  time.Duration
	step time.Duration

	codec tc.TimeCodec
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
	NoResolution = &Resolution{
		codec: NoResolutionCodec,
	}
	ResolutionHourly = NewResolution("hourly", Hourly, 0)
	ResolutionDaily  = NewResolution("daily", Daily, 0)
	ResolutionMinly  = NewResolution("minly", Minly, 0)
	ResolutionWeekly = NewResolution("weekly", Weekly, 0)
)

func NewResolution(name string, step, ttl time.Duration) *Resolution {
	codec := tc.UnixTimeCodec(step)
	return &Resolution{name, ttl, step, codec}
}

func (r *Resolution) WithTTL(ttl time.Duration) *Resolution {
	return &Resolution{
		Name:  r.Name,
		ttl:   ttl,
		codec: r.codec,
	}
}

func (r *Resolution) Round(t time.Time) time.Time {
	t, _ = r.codec.UnmarshalTime(r.codec.MarshalTime(t))
	return t
}

func (r *Resolution) TimeSequence(s, e time.Time) []time.Time {
	return TimeSequence(r.Round(s), r.Round(e), r.step)
}
func (r *Resolution) TTL() time.Duration {
	return r.ttl
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
