package tcodec

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

type TimeEncoder interface {
	MarshalTime(time.Time) string
}
type TimeEncoderFunc func(time.Time) string

func (enc TimeEncoderFunc) MarshalTime(t time.Time) string {
	return enc(t)
}

type TimeDecoderFunc func(string) (time.Time, error)

func (dec TimeDecoderFunc) UnmarshalTime(s string) (time.Time, error) {
	return dec(s)
}

type TimeDecoder interface {
	UnmarshalTime(string) (time.Time, error)
}

type TimeCodec interface {
	TimeEncoder
	TimeDecoder
}

func NewTimeCodec(enc TimeEncoderFunc, dec TimeDecoderFunc) TimeCodec {
	if enc == nil {
		panic("Invalid TimeEncoder")
	}
	if dec == nil {
		panic("Invalid TimeDecoder")
	}
	return &timeCodecFunc{enc, dec}
}

type timeCodecFunc struct {
	enc TimeEncoderFunc
	dec TimeDecoderFunc
}

func (c timeCodecFunc) MarshalTime(t time.Time) string {
	return c.enc(t)
}
func (c timeCodecFunc) UnmarshalTime(value string) (time.Time, error) {
	return c.dec(value)
}

type LayoutCodec string

func (layout LayoutCodec) UnmarshalTime(value string) (time.Time, error) {
	return time.Parse(string(layout), value)
}

func (layout LayoutCodec) MarshalTime(t time.Time) string {
	return t.Format(string(layout))
}

var isoweekRx = regexp.MustCompile("^(\\d{4})-(\\d{2})$")

type Error string

func (e Error) Error() string {
	return string(e)
}

const ErrInvalidTimeString Error = "Invalid ISOWeek string"

func UnixMillis(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

var ISOWeekCodec = NewTimeCodec(func(t time.Time) string {
	y, w := t.ISOWeek()
	return fmt.Sprintf("%d-%02d", y, w)

}, func(value string) (time.Time, error) {
	match := isoweekRx.FindStringSubmatch(value)
	if match == nil {
		return time.Time{}, ErrInvalidTimeString
	}
	year, _ := strconv.Atoi(string(match[1]))
	week, _ := strconv.Atoi(string(match[2]))
	if !(0 < week && week <= 53) {
		return time.Time{}, ErrInvalidTimeString
	}
	t := time.Date(year, 1, 0, 0, 0, 0, 0, time.UTC)
	for t.Weekday() > time.Sunday {
		t = t.Add(-24 * time.Hour)
	}
	t = t.Add(time.Duration(week+1) * 7 * 24 * time.Hour)
	return t, nil
})

var MillisTimeCodec = UnixMillisTimeCodec(0)

func UnixMillisTimeCodec(step time.Duration) TimeCodec {
	if step < time.Millisecond {
		step = time.Millisecond
	}
	unit := int64(step / time.Millisecond)

	return NewTimeCodec(func(t time.Time) string {
		ms := UnixMillis(t)
		return strconv.FormatInt(ms-(ms%unit), 10)
	}, func(s string) (t time.Time, err error) {
		var n int64
		if n, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		}
		return time.Unix(n/1000, (n%1000)*int64(time.Millisecond)), nil
	})

}

func UnixTimeCodec(step time.Duration) TimeCodec {
	if step < time.Second {
		step = time.Second
	}
	unit := int64(step / time.Second)

	return NewTimeCodec(func(t time.Time) string {
		s := t.Unix()
		return strconv.FormatInt(s-(s%unit), 10)
	}, func(s string) (t time.Time, err error) {
		var n int64
		if n, err = strconv.ParseInt(s, 10, 64); err != nil {
			return
		}
		return time.Unix(n, 0), nil
	})

}

type TimeDecoders []TimeDecoder

func (tds TimeDecoders) UnmarshalTime(s string) (t time.Time, err error) {
	for i := 0; i < len(tds); i++ {
		if t, err = tds[i].UnmarshalTime(s); err == nil {
			return
		}
	}
	err = errors.New("Invalid time format")
	return
}

const (
	ANSIC         LayoutCodec = time.ANSIC
	UnixDate      LayoutCodec = time.UnixDate
	RubyDate      LayoutCodec = time.RubyDate
	RFC822        LayoutCodec = time.RFC822
	RFC822Z       LayoutCodec = time.RFC822Z
	RFC850        LayoutCodec = time.RFC850
	RFC1123       LayoutCodec = time.RFC1123
	RFC1123Z      LayoutCodec = time.RFC1123Z
	RFC3339       LayoutCodec = time.RFC3339
	RFC3339Date   LayoutCodec = "2006-01-02"
	RFC3339Millis LayoutCodec = "2006-01-02T15:04:05.999Z07:00"
	RFC3339Nano   LayoutCodec = time.RFC3339Nano
)
