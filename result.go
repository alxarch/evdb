package meter

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
)

// Result is a query result
type Result struct {
	ScanResult
	Event string `json:"event,omitempty"`
}

// Results is a slice of results
type Results []Result

// DataPoint is a time/count pair
type DataPoint struct {
	Timestamp int64
	Value     float64
}

// DataPoints is a collection of DataPoints
type DataPoints []DataPoint

// ResultType is a type of result
type ResultType int

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
			r.Add(ts, n)
			return results
		}
	}
	return append(results, Result{
		Event: event,
		ScanResult: ScanResult{
			Fields: fields,
			Data:   []DataPoint{{ts, n}},
		},
	})
}

// Add adds a datapoint
func (s DataPoints) Add(t int64, v float64) DataPoints {
	for i := len(s) - 1; 0 <= i && i < len(s); i-- {
		d := &s[i]
		if d.Timestamp == t {
			d.Value += v
			return s
		}
	}
	return append(s, DataPoint{Timestamp: t, Value: v})
}

// ValueAt searches for the value at a specific time
func (s DataPoints) ValueAt(ts int64) float64 {
	for i := range s {
		if d := &s[i]; d.Timestamp == ts {
			return d.Value
		}
	}
	return math.NaN()
}

// IndexOf returns the index of tm in the collection of data points
func (s DataPoints) IndexOf(ts int64) int {
	for i := range s {
		if d := &s[i]; d.Timestamp == ts {
			return i
		}
	}
	return -1
}

func (s DataPoints) Avg() float64 {
	return s.Sum() / float64(len(s))
}

func (s DataPoints) Sum() (sum float64) {
	for i := range s {
		d := &s[i]
		sum += d.Value
	}
	return
}

// Len implements sort.Interface
func (s DataPoints) Len() int {
	return len(s)
}

// Swap implements sort.Interface
func (s DataPoints) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less implements sort.Interface
func (s DataPoints) Less(i, j int) bool {
	return s[i].Timestamp < s[j].Timestamp
}

func (p DataPoint) appendJSON(data []byte) ([]byte, error) {
	data = append(data, '[')
	data = strconv.AppendInt(data, p.Timestamp, 10)
	data = append(data, ',')
	data = strconv.AppendFloat(data, p.Value, 'f', -1, 64)
	data = append(data, ']')
	return data, nil
}

// MarshalJSON implements json.Marshaler interface
func (p DataPoint) MarshalJSON() ([]byte, error) {
	return p.appendJSON(make([]byte, 0, 64))
}

// UnmarshalJSON implements json.Unmarshaler interface
func (p *DataPoint) UnmarshalJSON(data []byte) error {
	value := [2]json.Number{}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	p.Timestamp, err = value[0].Int64()
	if err != nil {
		return err
	}
	p.Value, err = value[1].Float64()
	if err != nil {
		return err
	}
	return nil
}

// MarshalJSON implements json.Marshal interface
func (s DataPoints) MarshalJSON() (data []byte, err error) {
	if s == nil {
		return nil, nil
	}
	data = make([]byte, 1, len(s)*64+2)
	data[0] = '['
	for i := range s {
		p := &s[i]
		if i != 0 {
			data = append(data, ',')

		}
		data, _ = p.appendJSON(data)
	}
	data = append(data, ']')
	return data, nil
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
