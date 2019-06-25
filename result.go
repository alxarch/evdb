package meter

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

// Result is a query result
type Result struct {
	Event  string      `json:"event"`
	Fields Fields      `json:"fields,omitempty"`
	Total  float64     `json:"total"`
	Data   []DataPoint `json:"data,omitempty"`
}

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

// Reset resets a result
func (r *Result) Reset() {
	*r = Result{
		Data: r.Data[:0],
	}
}

// Add adds n times at ts time to a result
func (r *Result) Add(ts int64, n float64) {
	r.Total += n
	for i := len(r.Data) - 1; 0 <= i && i < len(r.Data); i-- {
		d := &r.Data[i]
		if d.Timestamp == ts {
			d.Value += n
			return
		}
	}
	r.Data = append(r.Data, DataPoint{Timestamp: ts, Value: n})
	return
}

// Results is a slice of results
type Results []Result

// Add adds a result
func (results Results) Add(event string, fields Fields, n float64, ts int64) Results {
	for i := range results {
		r := &results[i]
		if r.Event == event && r.Fields.Equal(fields) {
			r.Add(ts, n)
			return results
		}
	}
	return append(results, Result{
		Event:  event,
		Fields: fields,
		Total:  n,
		Data:   []DataPoint{{ts, n}},
	})
}

// DataPoint is a time/count pair
type DataPoint struct {
	Timestamp int64
	Value     float64
}

// DataPoints is a collection of DataPoints
type DataPoints []DataPoint

// Find searches for the count at a specific time
func (s DataPoints) Find(tm time.Time) (float64, bool) {
	if i := s.IndexOf(tm); 0 <= i && i < len(s) {
		return s[i].Value, true
	}
	return 0, false
}

// IndexOf returns the index of tm in the collection of data points
func (s DataPoints) IndexOf(tm time.Time) int {
	ts := tm.Unix()
	for i := range s {
		if d := &s[i]; d.Timestamp == ts {
			return i
		}
	}
	return -1
}

// Sort sorts a slice of data points in place
func (s DataPoints) Sort() {
	sort.Sort(s)
}

func (s DataPoints) Len() int {
	return len(s)
}

func (s DataPoints) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s DataPoints) Less(i, j int) bool {
	return s[i].Timestamp < s[j].Timestamp
}

// MarshalJSON implements json.Marshaler interface
func (p DataPoint) MarshalJSON() (data []byte, err error) {
	data = make([]byte, 0, 64)
	data = append(data, '[')
	data = strconv.AppendInt(data, p.Timestamp, 10)
	data = append(data, ',')
	data = strconv.AppendFloat(data, p.Value, 'f', -1, 64)
	data = append(data, ']')
	return data, nil
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
		data = append(data, '[')
		data = strconv.AppendInt(data, p.Timestamp, 10)
		data = append(data, ',')
		data = strconv.AppendFloat(data, p.Value, 'f', -1, 64)
		data = append(data, ']')
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

// FieldSummaries is a slice of FieldSummary results
type FieldSummaries []FieldSummary

// Totals returns a totals-only Results slice
func (results Results) Totals() Results {
	for i := range results {
		r := &results[i]
		r.Data = r.Data[:0]
	}
	return results
}

// FieldSummaries converts a series of results to a slice of FieldSummary results
func (results Results) FieldSummaries() (s FieldSummaries) {
	for i := range results {
		r := &results[i]
		for j := range r.Fields {
			f := &r.Fields[j]
			s = s.append(r.Event, f.Label, f.Value, r.Total)
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
		if r.Total == 0 {
			continue
		}
		values = r.Fields.AppendValues(values[:0], empty, s.Labels...)
		s.add(r.Event, values, r.Total)
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
