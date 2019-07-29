package evredis

import (
	"fmt"
	"time"

	"github.com/alxarch/evdb/tcodec"
)

// Date formats
const (
	DailyDateFormat  string = "2006-01-02"
	HourlyDateFormat string = "2006-01-02-15"
)

// Resolution describes time dimensions
type Resolution struct {
	name string
	// step time.Duration
	ttl  time.Duration
	step time.Duration

	codec tcodec.TimeCodec
}

// Common durations
const (
	Hourly  = time.Hour
	Daily   = 24 * time.Hour
	Weekly  = 7 * Daily
	Monthly = 30 * Daily
	Yearly  = 365 * Daily
)

// Common resolutions
var (
	// NoResolution     = Resolution{"totals", 0, 0, NoResolutionCodec}
	ResolutionHourly = Resolution{"hourly", 0, Hourly, tcodec.LayoutCodec(HourlyDateFormat)}
	ResolutionDaily  = Resolution{"daily", 0, Daily, tcodec.LayoutCodec(DailyDateFormat)}
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

func (r Resolution) Truncate(t time.Time) time.Time {
	return t.Truncate(r.step).In(t.Location())
}
func (r Resolution) AddSteps(t time.Time, n int) time.Time {
	return t.Truncate(r.step).In(t.Location()).Add(time.Duration(n) * r.Step())
}

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

func resolutionsByDuration(resolutions ...Resolution) (map[time.Duration]Resolution, error) {
	m := make(map[time.Duration]Resolution, len(resolutions))
	for _, res := range resolutions {
		step := res.Step()
		if _, duplicate := m[step]; duplicate {
			return nil, fmt.Errorf(`Duplicate resolution %s`, step)
		}
		m[step] = res
	}
	return m, nil
}
