package tcodec_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/alxarch/evdb/tcodec"
)

func Test_LayoutCodec(t *testing.T) {
	d := time.Now()
	layout := "2006-01-02"
	expect := d.Format(layout)
	c := tcodec.LayoutCodec(layout)
	if actual := c.MarshalTime(d); actual != expect {
		t.Errorf("Invalid date string %s", expect)
	}
	tm, err := c.UnmarshalTime(expect)
	if err != nil {
		t.Error(err)
	}
	sexpect, _ := time.Parse(layout, expect)
	if tm != sexpect {
		t.Error("Invalid time ", tm)
	}

}
func Test_Millis(t *testing.T) {
	now := time.Now()
	ms := tcodec.UnixMillis(now)
	dt := now.UnixNano() - ms*1000000
	if dt < 0 {
		dt = -dt
	}
	if dt > int64(time.Millisecond) {
		t.Error("Wrong millis", dt)
	}
}
func Test_MillisCodec(t *testing.T) {
	tnow := time.Now()
	now := tcodec.UnixMillis(tnow)
	snow := fmt.Sprintf("%d", now)
	tm, err := tcodec.MillisTimeCodec.UnmarshalTime(snow)
	if err != nil {
		t.Error(err)
	}

	if tcodec.UnixMillis(tm) != now {
		t.Error("Invalid time ", now-tcodec.UnixMillis(tm))
	}
	s := tcodec.MillisTimeCodec.MarshalTime(tnow)
	if s != snow {
		t.Error("Invalid decoder output ", s, snow)
	}
	_, err = tcodec.MillisTimeCodec.UnmarshalTime("foo")
	if err == nil {
		t.Error("Invalid error")
	}
}
func Test_NewTimeCodecNoDec(t *testing.T) {
	defer func() {
		if msg := recover(); msg == nil {
			t.Error("Didn't panic without encoder")
		}
	}()
	lc := tcodec.LayoutCodec("")
	tcodec.NewTimeCodec(nil, lc.UnmarshalTime)
}

func Test_NewTimeCodecNoEnc(t *testing.T) {
	defer func() {
		if msg := recover(); msg == nil {
			t.Error("Didn't panic without encoder")
		}
	}()
	lc := tcodec.LayoutCodec("")
	tcodec.NewTimeCodec(lc.MarshalTime, nil)
}

func Test_ISOWeekCodec(t *testing.T) {
	tm, err := tcodec.ISOWeekCodec.UnmarshalTime("2017-09")
	if err != nil {
		t.Error(err.Error())
	}
	if _, w := tm.ISOWeek(); w != 9 {
		t.Error(fmt.Sprintf("ISOWeek invalid week %d", w))
	}
	if y, _ := tm.ISOWeek(); y != 2017 {
		t.Error(fmt.Sprintf("ISOWeek invalid year %d", y))
	}
	s := tcodec.ISOWeekCodec.MarshalTime(tm)
	if s != "2017-09" {
		t.Error(fmt.Sprintf("ISOWeek invalid string %s", s))

	}

	if _, err := tcodec.ISOWeekCodec.UnmarshalTime("2017-09a"); err != tcodec.InvalidISOWeekString {
		t.Error("Invalid error")
	}
	if _, err := tcodec.ISOWeekCodec.UnmarshalTime("2017-99"); err != tcodec.InvalidWeekNumberError {
		t.Error("Invalid error")
	}
}

// func Test_Round(t *testing.T) {
// 	now := time.Now()
// 	var actual, expect time.Time
// 	actual = tcodec.Round(now, time.Minute)
// 	expect = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
// 	if actual != expect {
// 		t.Errorf("Invalid round %s\n%s\n%s", time.Minute, actual, expect)
// 	}
// 	actual = tcodec.Round(now, time.Nanosecond)
// 	expect = now
// 	if actual != expect {
// 		t.Errorf("Invalid round %s\n%s\n%s", time.Nanosecond, actual, expect)
// 	}
//
// }
func Test_UnixTimeCodec(t *testing.T) {
	for _, unit := range []time.Duration{time.Second, time.Minute, time.Hour} {
		c := tcodec.UnixTimeCodec(unit)
		now := time.Now()

		seconds := now.Unix()

		expect := strconv.FormatInt(seconds-(seconds%int64(unit/time.Second)), 10)
		actual := c.MarshalTime(now)
		if actual != expect {
			t.Errorf("Invalid marshal %s\n%s\n%s", unit, actual, expect)
		}
		tm, err := c.UnmarshalTime(actual)
		if err != nil {
			t.Error(err)
		}
		var etm time.Time
		switch unit {
		case time.Hour:
			etm = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())
		case time.Minute:
			etm = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
		default:
			etm = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Truncate(time.Second).Second(), 0, now.Location())
		}
		if tm != etm {
			t.Errorf("Invalid unmarshal %s\n%s\n%s", unit, tm, etm)

		}

	}

}
func Test_UnixMillisTimeCodec(t *testing.T) {
	for _, unit := range []time.Duration{time.Second, time.Minute, time.Hour} {
		c := tcodec.UnixMillisTimeCodec(unit)
		now := time.Now()

		ms := tcodec.UnixMillis(now)

		expect := strconv.FormatInt(ms-(ms%int64(unit/time.Millisecond)), 10)
		actual := c.MarshalTime(now)
		if actual != expect {
			t.Errorf("Invalid marshal %s\n%s\n%s", unit, actual, expect)
		}
		tm, err := c.UnmarshalTime(actual)
		if err != nil {
			t.Error(err)
		}
		var etm time.Time
		switch unit {
		case time.Hour:
			etm = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())
		case time.Minute:
			etm = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
		default:
			etm = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Truncate(time.Second).Second(), 0, now.Location())
		}
		if tm != etm {
			t.Errorf("Invalid unmarshal %s\n%s\n%s", unit, tm, etm)

		}

	}

}
