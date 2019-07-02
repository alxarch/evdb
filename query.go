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

// Querier runs queries
type Querier interface {
	Query(ctx context.Context, q Query, events ...string) (Results, error)
}

func (q Query) Between(start, end time.Time) Query {
	q.Start = start
	q.End = end
	return q
}
func (q Query) GroupBy(group ...string) Query {
	q.Group = group
	return q
}
func (q Query) Where(label string, values ...string) Query {
	for _, v := range values {
		q.Match = append(q.Match, Field{label, v})
	}
	return q
}
func (q Query) At(step time.Duration) Query {
	q.Step = step
	return q
}
