package meter

import (
	"fmt"
	"reflect"
	"sort"
)

// Result is a query result
type Result struct {
	TimeRange
	Event  string     `json:"event,omitempty"`
	Fields Fields     `json:"fields,omitempty"`
	Data   DataPoints `json:"data,omitempty"`
}

// Results is a slice of results
type Results []Result

// Merge implements Value interface
func (r *Result) Merge(m Merger, x interface{}, rev bool) (interface{}, error) {
	if r == nil {
		return nil, nil
	}
	switch x := x.(type) {
	case []interface{}:
		var v []interface{}
		for _, x := range x {
			y, err := r.Merge(m, x, rev)
			if err != nil {
				return nil, err
			}
			v = append(v, y)
		}
		return v, nil
	case *Result:
		if x == nil {
			return nil, nil
		}
		var v Result
		var err error
		if rev {
			v.Event = x.Event
			v.Fields = x.Fields
			v.Data = x.Data.Copy()
			err = v.Data.MergeVector(m, r.Data)
		} else {
			v.Event = r.Event
			v.Fields = r.Fields
			v.Data = r.Data.Copy()
			err = v.Data.MergeVector(m, x.Data)
		}
		if err != nil {
			return nil, err
		}
		return &v, nil
	case float64:
		var v Result
		v.Event = r.Event
		v.Fields = r.Fields
		if rev {
			v.Data = r.Data.Copy().MergeValue(m, x)
		} else {
			v.Data = r.Data.Copy().MergeValueR(m, x)
		}
		return &m, nil
	default:
		return nil, fmt.Errorf("Unsupported operand %q", reflect.TypeOf(x))
	}
}

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

// func (r Results) Merge(fields Fields, data ...DataPoint) Results {
// 	for i := range r {
// 		rr := &results[i]
// 		if rr.Fields.Equal(fields) {
// 			r.Data = r.Data.Aggregate(mergeSum, data...)
// 			return results
// 		}
// 	}
// 	return append(results, Result{
// 		Fields: fields.Copy(),
// 		Data:   data,
// 	})
// }

// // ResultType is a type of result
// type ResultType int
// // Result types
// const (
// 	ArrayResult ResultType = iota
// 	TotalsResult
// 	EventSummaryResult
// 	FieldSummaryResult
// )

// // ResultTypeFromString converts a string to ResultType
// func ResultTypeFromString(s string) ResultType {
// 	switch s {
// 	case "totals":
// 		return TotalsResult
// 	case "events":
// 		return EventSummaryResult
// 	case "fields":
// 		return FieldSummaryResult
// 	default:
// 		return ArrayResult
// 	}
// }
