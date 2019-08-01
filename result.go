package evdb

import (
	"context"
	"encoding/json"
	"strings"
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

func FlattenResults(multi ...Results) (flat Results) {
	for _, m := range multi {
		for _, r := range m {
			flat = append(flat, r)
		}
	}
	return
}

// FormatResults convers results to requested format
func FormatResults(format string, rows ...Results) (interface{}, bool) {
	switch strings.ToLower(format) {
	case "table":
		rr := FlattenResults(rows...)
		tbl := rr.EventSummaries("").Table()
		return tbl, true
	case "totals":
		var totals []Totals
		for _, row := range rows {
			totals = append(totals, row.Totals())
		}
		return totals, true
	case "events":
		var sums []FieldSummaries
		for _, row := range rows {
			sums = append(sums, row.FieldSummaries())
		}
		return sums, true
	case "", "results":
		return rows, true
	default:
		return nil, false
	}

}
