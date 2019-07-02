package meter

import (
	"context"
	"time"
)

// Query is a query for event results
type Query struct {
	TimeRange
	Match      Fields   `json:"match,omitempty"`
	Group      []string `json:"group,omitempty"`
	EmptyValue string   `json:"empty,omitempty"`
}

// TruncateTimestamp truncates a timestamp to Query.Step
func (q *Query) TruncateTimestamp(ts int64) int64 {
	step := int64(q.Step / time.Second)
	return stepTS(ts, step)
}

// TimeRange is a range of time with a specific step
type TimeRange struct {
	Start time.Time     `json:"start"`
	End   time.Time     `json:"end"`
	Step  time.Duration `json:"step"`
}

func (tr *TimeRange) Truncate(tm time.Time) time.Time {
	if tr.Step > 0 {
		return tm.Truncate(tr.Step).In(tm.Location())
	}
	if tr.Step == 0 {
		return time.Time{}
	}
	return tm
}

func (tr *TimeRange) Sequence() []time.Time {
	if tr.Step <= 0 {
		return nil
	}

	start := tr.Truncate(tr.Start)
	end := tr.Truncate(tr.End)
	n := end.Sub(start) / tr.Step
	seq := make([]time.Time, 0, n)
	for s := start; end.Sub(s) >= 0; s = s.Add(tr.Step) {
		seq = append(seq, s)
	}
	return seq
}

// Querier runs queries
type Querier interface {
	Query(ctx context.Context, q Query, events ...string) (Results, error)
}
