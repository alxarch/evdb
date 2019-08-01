package evutil

import (
	"sort"
	"strings"

	db "github.com/alxarch/evdb"
)

func FlattenResults(multi ...db.Results) (flat db.Results) {
	for _, m := range multi {
		for _, r := range m {
			flat = append(flat, r)
		}
	}
	return
}

// FormatResults convers results to requested format
func FormatResults(format string, rows ...db.Results) (interface{}, bool) {
	switch strings.ToLower(format) {
	case "table":
		rr := FlattenResults(rows...)
		tbl := NewEventSummaries("", rr).Table()
		return tbl, true
	case "totals":
		var totals []Totals
		for _, row := range rows {
			totals = append(totals, NewTotals(row))
		}
		return totals, true
	case "events":
		var sums []FieldSummaries
		for _, row := range rows {
			sums = append(sums, NewFieldSummaries(row))
		}
		return sums, true
	case "", "results":
		return rows, true
	default:
		return nil, false
	}

}

type Total struct {
	Event  string    `json:"event"`
	Fields db.Fields `json:"fields,omitempty"`
	Total  float64   `json:"total"`
}

type Totals []Total

// Totals returns a totals-only Results slice
func NewTotals(results db.Results) (totals Totals) {
	totals = make([]Total, len(results))
	for i := range results {
		r := &results[i]
		totals[i] = Total{
			Event:  r.Event,
			Fields: r.Fields,
			Total:  r.Data.Sum(),
		}
	}
	return
}

// FieldSummary is a query result presented as a summary of field values
type FieldSummary struct {
	Event  string             `json:"event"`
	Label  string             `json:"label"`
	Values map[string]float64 `json:"values"`
}

// Add ads a value to a field summary
func (s *FieldSummary) Add(value string, n float64) {
	if s.Values == nil {
		s.Values = make(map[string]float64)
	}
	s.Values[value] += n
}

// FieldSummaries is a slice of FieldSummary results
type FieldSummaries []FieldSummary

// FieldSummaries converts a series of results to a slice of FieldSummary results
func NewFieldSummaries(results db.Results) (s FieldSummaries) {
	for i := range results {
		r := &results[i]
		for j := range r.Fields {
			f := &r.Fields[j]
			s = s.append(r.Event, f.Label, f.Value, r.Data.Sum())
		}
	}
	return s
}

func (sums FieldSummaries) append(event, label, value string, n float64) FieldSummaries {
	for i := range sums {
		sum := &sums[i]
		if sum.Event == event && sum.Label == label {
			sum.Add(value, n)
			return sums
		}
	}
	return append(sums, FieldSummary{
		Event:  event,
		Label:  label,
		Values: map[string]float64{value: n},
	})
}

// EventSummaries is a result summary format
type EventSummaries struct {
	Labels []string
	Events []string
	Data   []EventSummary
}

// EventSummary groups values with totals
type EventSummary struct {
	Values []string
	Totals map[string]float64
}

// NewEventSummaries groups results as EventSummaries
func NewEventSummaries(empty string, results db.Results) *EventSummaries {
	s := new(EventSummaries)
	for i := range results {
		r := &results[i]
		s.Events = appendDistinct(s.Events, r.Event)
		for i := range r.Fields {
			f := &r.Fields[i]
			s.Labels = appendDistinct(s.Labels, f.Label)
		}
	}
	sort.Strings(s.Labels)
	values := make([]string, len(s.Labels))
	sort.Strings(s.Events)
	for i := range results {
		r := &results[i]
		n := r.Data.Sum()
		if n == 0 {
			continue
		}
		values = r.Fields.AppendValues(values[:0], empty, s.Labels...)
		s.add(r.Event, values, n)
	}

	return s
}

// TableRow is a helper to convert event summary to table
func (r *EventSummary) TableRow(events []string) []interface{} {
	row := make([]interface{}, 0, len(r.Values)+len(events))
	for _, v := range r.Values {
		row = append(row, v)
	}
	for _, event := range events {
		row = append(row, r.Totals[event])
	}
	return row
}

func (s *EventSummaries) add(event string, values []string, n float64) {
	for i := range s.Data {
		sum := &s.Data[i]
		if stringsEqual(sum.Values, values) {
			sum.Totals[event] = n
			return
		}
	}
	s.Data = append(s.Data, EventSummary{
		Values: values,
		Totals: map[string]float64{event: n},
	})

}

// Table returns event summaries as a table
func (s *EventSummaries) Table() Table {
	tbl := Table{}
	for _, label := range s.Labels {
		tbl.Columns = append(tbl.Columns, "label:"+label)
	}
	for _, event := range s.Events {
		tbl.Columns = append(tbl.Columns, "event:"+event)
	}
	for i := range s.Data {
		r := &s.Data[i]
		tbl.Data = append(tbl.Data, r.TableRow(s.Events))
	}
	return tbl
}

// Table holds generic tabular data
type Table struct {
	Columns []interface{}   `json:"cols"`
	Data    [][]interface{} `json:"data"`
}

func appendDistinct(dst []string, src ...string) []string {
	for i, s := range src {
		if indexOf(dst, s[:i]) == -1 {
			dst = append(dst, s)
		}
	}
	return dst
}

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

func stringsEqual(a, b []string) bool {
	if len(a) == len(b) {
		b = b[:len(a)]
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	return false
}
