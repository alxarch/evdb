package evdb

import (
	"context"
	"encoding/json"
)

// Result is a query result
type Result struct {
	TimeRange
	Event  string     `json:"event,omitempty"`
	Fields Fields     `json:"fields,omitempty"`
	Data   DataPoints `json:"data,omitempty"`
}

func (r *Result) MarshalJSON() ([]byte, error) {
	type jsonResult struct {
		TimeRange [3]int64   `json:"time"`
		Event     string     `json:"event,omitempty"`
		Fields    Fields     `json:"fields,omitempty"`
		Data      DataPoints `json:"data,omitempty"`
	}
	var start, end int64
	if !r.Start.IsZero() {
		start = r.Start.Unix()
	}
	if !r.End.IsZero() {
		start = r.End.Unix()
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

var _ ScanQuerier = (Results)(nil)

// ScanQuery implements ScanQuerier interface
func (results Results) ScanQuery(_ context.Context, q *ScanQuery) (Results, error) {
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
