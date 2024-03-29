package evdb

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// Result is a query result
type Result struct {
	TimeRange
	Event  string     `json:"event,omitempty"`
	Fields Fields     `json:"fields,omitempty"`
	Data   DataPoints `json:"data,omitempty"`
}

func (r *Result) MarshalJSON() ([]byte, error) {
	var start, end int64
	if !r.Start.IsZero() {
		start = r.Start.Unix()
	}
	if !r.End.IsZero() {
		end = r.End.Unix()
	}
	tmp := jsonResult{
		TimeRange: [3]int64{
			start, end, int64(r.Step.Seconds()),
		},
		Event:  r.Event,
		Fields: r.Fields,
		Data:   r.Data,
	}
	return json.Marshal(&tmp)
}

type jsonResult struct {
	TimeRange [3]int64   `json:"time"`
	Event     string     `json:"event,omitempty"`
	Fields    Fields     `json:"fields,omitempty"`
	Data      DataPoints `json:"data,omitempty"`
}

func (r *Result) UnmarshalJSON(data []byte) error {
	tmp := jsonResult{}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*r = Result{
		Event: tmp.Event,
		TimeRange: TimeRange{
			Start: time.Unix(tmp.TimeRange[0], 0),
			End:   time.Unix(tmp.TimeRange[1], 0),
			Step:  time.Duration(tmp.TimeRange[2]) * time.Second,
		},
		Fields: tmp.Fields,
		Data:   tmp.Data,
	}
	return nil
}

// Results is a slice of results
type Results []Result

// Add adds a result
func (results Results) Add(event string, fields Fields, t int64, v float64) Results {
	for i := range results {
		r := &results[i]
		if r.Event != event {
			continue
		}
		if !r.Fields.Equal(fields) {
			continue
		}
		r.Data = r.Data.Add(t, v)
		return results
	}
	return append(results, Result{
		Event:  event,
		Fields: fields.Copy(),
		Data:   []DataPoint{{t, v}},
	})

}

var _ Querier = (Results)(nil)

// Query implements Querier interface
func (results Results) Query(_ context.Context, q *Query) (Results, error) {
	var scan Results
	t := &q.TimeRange
	start, end := t.Start.Unix(), t.End.Unix()
	for i := range results {
		r := &results[i]
		if r.Event != q.Event {
			continue
		}
		if !q.Fields.Match(r.Fields) {
			continue
		}
		switch rel := r.TimeRange.Rel(t); rel {
		case TimeRelEqual, TimeRelBetween:
			scan = append(scan, *r)
		case TimeRelAfter, TimeRelBefore, TimeRelNone:
			continue
		case TimeRelOverlapsAfter, TimeRelOverlapsBefore, TimeRelAround:
			scan = append(scan, Result{
				TimeRange: q.TimeRange,
				Event:     q.Event,
				Fields:    r.Fields,
				Data:      r.Data.Slice(start, end),
			})
		default:
			continue
		}
	}
	return scan, nil

}

// Sort sorts fields and datapoints
func (r *Result) Sort() {
	sort.Stable(r.Data)
	sort.Stable(r.Fields)
}

// Sort sorts fields and datapoints in all results
func (results Results) Sort() {
	for i := range results {
		r := &results[i]
		r.Sort()
	}
}

type ResultGroup struct {
	Fields  Fields
	Results Results
}
type GroupedResults []ResultGroup

func (results Results) Group(empty string, by ...string) (groups GroupedResults) {
	scratch := Fields(make([]Field, 0, len(by)))
	for i := range results {
		r := &results[i]
		scratch = r.Fields.AppendGrouped(scratch[:0], empty, by)
		groups = groups.add(scratch, r)
	}
	return groups

}
func (g GroupedResults) add(fields Fields, r *Result) GroupedResults {
	for i := range g {
		group := &g[i]
		if group.Fields.Equal(fields) {
			group.Results = append(group.Results, *r)
			return g
		}
	}
	return append(g, ResultGroup{
		Fields:  fields.Copy(),
		Results: Results{*r},
	})
}
