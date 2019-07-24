package meter

import "time"

// TimeRange is a range of time with a specific step
type TimeRange struct {
	Start time.Time     `json:"start"`
	End   time.Time     `json:"end"`
	Step  time.Duration `json:"step"`
}

type TimeRel int

const (
	TimeRelNone TimeRel = iota
	TimeRelAround
	TimeRelOverlapsBefore
	TimeRelBefore
	TimeRelEqual
	TimeRelAfter
	TimeRelOverlapsAfter
	TimeRelBetween
)

func (tr *TimeRange) Truncate(tm time.Time) time.Time {
	if tr.Step > 0 {
		return tm.Truncate(tr.Step).In(tm.Location())
	}
	if tr.Step == 0 {
		return time.Time{}
	}
	return tm
}

func (tr *TimeRange) Each(fn func(time.Time, int)) {
	start := tr.Start.Truncate(tr.Step)
	end := tr.End.Truncate(tr.Step)
	for i := 0; !end.After(start); start, i = start.Add(tr.Step), i+1 {
		fn(start, i)
	}
}

func (tr *TimeRange) NumSteps() int {
	start := tr.Start.Truncate(tr.Step)
	end := tr.End.Truncate(tr.Step)
	return int(end.Sub(start) / tr.Step)
}

func (tr *TimeRange) SameShape(other *TimeRange) bool {
	return tr.Step == other.Step && tr.NumSteps() == other.NumSteps()
}

func (tr *TimeRange) Rel(other *TimeRange) TimeRel {
	if tr.Step != other.Step {
		return TimeRelNone
	}
	tminA, tmaxA, tminB, tmaxB := tr.Start, tr.End, other.Start, other.End

	if tminB.Equal(tminA) {
		if tmaxB.After(tmaxA) {
			return TimeRelAround
		}
		if tmaxB.Equal(tmaxA) {
			return TimeRelEqual
		}
		return TimeRelBetween
	}
	// tminB != tminA
	if tminB.After(tmaxA) {
		return TimeRelAfter
	}
	// tminB <= tmaxA
	if tmaxB.Before(tminA) {
		return TimeRelBefore
	}
	// tmaxB >= tminA

	if tminB.Before(tminA) {
		if tmaxB.After(tmaxA) {
			return TimeRelAround
		}
		return TimeRelOverlapsBefore
	}
	// tminB >= tminA
	if tmaxB.After(tmaxA) {
		if tminB.Before(tminA) {
			return TimeRelAround
		}
		return TimeRelOverlapsAfter
	}
	// tmaxB <= tmaxA
	return TimeRelBetween
}

func (tr TimeRange) Offset(d time.Duration) TimeRange {
	tr.Start, tr.End = tr.Start.Add(d), tr.End.Add(d)
	return tr
}
