package meter

import (
	"sort"
)

// Result is a query result
type Result struct {
	ScanResult
	Event string `json:"event,omitempty"`
}

// Results is a slice of results
type Results []Result

// ResultType is a type of result
type ResultType int

func (results Results) ByEvent() map[string]ScanResult {
	byEvent := make(map[string]ScanResult)
	for i := range results {
		r := &results[i]
		s := byEvent[r.Event]
		data := s.Data.Merge(mergeSum, r.Data...)
		fields := s.Fields.Merge(r.Fields...)
		byEvent[r.Event] = ScanResult{
			Fields: fields,
			Data:   data,
		}
	}
	for event, r := range byEvent {
		sort.Sort(r.Data)
		sort.Sort(r.Fields)
		byEvent[event] = r
	}
	return byEvent
}

// Result types
const (
	ArrayResult ResultType = iota
	TotalsResult
	EventSummaryResult
	FieldSummaryResult
)

// ResultTypeFromString converts a string to ResultType
func ResultTypeFromString(s string) ResultType {
	switch s {
	case "totals":
		return TotalsResult
	case "events":
		return EventSummaryResult
	case "fields":
		return FieldSummaryResult
	default:
		return ArrayResult
	}
}

// Add adds a result
func (results Results) Add(event string, fields Fields, ts int64, n float64) Results {

	for i := range results {
		r := &results[i]
		if r.Event == event && r.Fields.Equal(fields) {
			r.Data = r.Data.MergePoint(mergeSum, ts, n)
			return results
		}
	}
	return append(results, Result{
		Event: event,
		ScanResult: ScanResult{
			Fields: fields.Copy(), // Do not keep a reference to Fields
			Data:   []DataPoint{{ts, n}},
		},
	})
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

type Total struct {
	Event  string  `json:"event"`
	Fields Fields  `json:"fields,omitempty"`
	Total  float64 `json:"total"`
}

type Totals []Total

// Totals returns a totals-only Results slice
func (results Results) Totals() (totals Totals) {
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

// FieldSummaries is a slice of FieldSummary results
type FieldSummaries []FieldSummary

// FieldSummaries converts a series of results to a slice of FieldSummary results
func (results Results) FieldSummaries() (s FieldSummaries) {
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

// EventSummaries groups results as EventSummaries
func (results Results) EventSummaries(empty string) *EventSummaries {
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
