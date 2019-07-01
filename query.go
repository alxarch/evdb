package meter

import (
	"context"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Query is a query for event results
type Query struct {
	TimeRange
	Match      Fields   `json:"match,omitempty"`
	Group      []string `json:"group,omitempty"`
	EmptyValue string   `json:"empty,omitempty"`
}

// URL adds the query to a URL
func (q *Query) URL(baseURL string, events ...string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	values := url.Values(make(map[string][]string))
	values.Set("start", strconv.FormatInt(q.Start.Unix(), 10))
	values.Set("end", strconv.FormatInt(q.End.Unix(), 10))
	for _, label := range q.Group {
		values.Add("group", label)
	}
	if q.Step != 0 {
		values.Set("step", q.Step.String())
	}
	match := q.Match.Sorted()
	for _, field := range match {
		values.Add(`match.`+field.Label, field.Value)
	}
	if q.EmptyValue != "" {
		values.Set("empty", q.EmptyValue)
	}
	for _, event := range events {
		values.Add("event", event)
	}
	u.RawQuery = values.Encode()
	return u.String(), nil

}

// TruncateTimestamp truncates a timestamp to Query.Step
func (q *Query) TruncateTimestamp(ts int64) int64 {
	step := int64(q.Step / time.Second)
	return stepTS(ts, step)
}

// SetValues sets query values from a URL query
func (q *Query) SetValues(values url.Values) {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			q.Step, _ = time.ParseDuration(step[0])
		} else {
			q.Step = 0
		}
	} else {
		q.Step = -1
	}
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	if start > 0 {
		q.Start = time.Unix(start, 0).In(time.UTC)
	}
	if end, _ := strconv.ParseInt(values.Get("end"), 10, 64); end > 0 {
		q.End = time.Unix(end, 0).In(time.UTC)
	}

	match := q.Match[:0]
	for key, values := range values {
		if strings.HasPrefix(key, "match.") {
			label := strings.TrimPrefix(key, "match.")
			for _, value := range values {
				match = append(match, Field{
					Label: label,
					Value: value,
				})
			}
		}
	}
	group, ok := values["group"]
	if ok && len(group) == 0 {
		group = make([]string, 0, len(match))
		for i := range match {
			m := &match[i]
			group = appendDistinct(group, m.Label)
		}
	}
	sort.Stable(match)
	q.Match, q.Group = match, group
	q.EmptyValue = values.Get("empty")
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

// QueryRunner runs queries
type Querier interface {
	Query(ctx context.Context, q Query, events ...string) (Results, error)
}
