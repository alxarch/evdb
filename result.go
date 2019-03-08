package meter

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

type Result struct {
	Event  string      `json:"event"`
	Fields Fields      `json:"fields,omitempty"`
	Total  int64       `json:"total"`
	Data   []DataPoint `json:"data,omitempty"`
}

type ResultType int

const (
	ArrayResult ResultType = iota
	TotalsResult
	EventSummaryResult
	FieldSummaryResult
)

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

func (r *Result) Reset() {
	*r = Result{
		Data: r.Data[:0],
	}
}

func (r *Result) Add(ts, n int64) {
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

type Results []Result

func (results Results) Add(event string, fields Fields, n, ts int64) Results {
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

type DataPoint struct {
	Timestamp, Value int64
}

type DataPoints []DataPoint

func (data DataPoints) Find(tm time.Time) (int64, bool) {
	if i := data.IndexOf(tm); 0 <= i && i < len(data) {
		return data[i].Value, true
	}
	return 0, false
}

func (data DataPoints) IndexOf(tm time.Time) int {
	ts := tm.Unix()
	for i := range data {
		if d := &data[i]; d.Timestamp == ts {
			return i
		}
	}
	return -1
}

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

func (p DataPoint) MarshalJSON() (data []byte, err error) {
	data = make([]byte, 0, 64)
	data = append(data, '[')
	data = strconv.AppendInt(data, p.Timestamp, 10)
	data = append(data, ',')
	data = strconv.AppendInt(data, p.Value, 10)
	data = append(data, ']')
	return data, nil
}

func (p *DataPoint) UnmarshalJSON(data []byte) (err error) {
	value := [2]int64{}
	if err = json.Unmarshal(data, &value); err == nil {
		p.Timestamp, p.Value = value[0], value[1]
	}
	return
}

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
		data = strconv.AppendInt(data, p.Value, 10)
		data = append(data, ']')
	}
	data = append(data, ']')
	return data, nil
}

type FieldSummary struct {
	Event  string           `json:"event"`
	Label  string           `json:"label"`
	Values map[string]int64 `json:"values"`
}

func (s *FieldSummary) Add(value string, n int64) {
	if s.Values == nil {
		s.Values = make(map[string]int64)
	}
	s.Values[value] += n
}

type FieldSummaries []FieldSummary

func (results Results) Totals() Results {
	for i := range results {
		r := &results[i]
		r.Data = r.Data[:0]
	}
	return results
}
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

func (sums FieldSummaries) append(event, label, value string, n int64) FieldSummaries {
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
		Values: map[string]int64{value: n},
	})
}

type EventSummaries struct {
	Labels []string
	Events []string
	Data   []EventSummary
}
type EventSummary struct {
	Values []string
	Totals map[string]int64
}

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

func (s *EventSummaries) add(event string, values []string, n int64) {
	for i := range s.Data {
		sum := &s.Data[i]
		if stringsEqual(sum.Values, values) {
			sum.Totals[event] = n
			return
		}
	}
	s.Data = append(s.Data, EventSummary{
		Values: values,
		Totals: map[string]int64{event: n},
	})

}

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

type Table struct {
	Columns []interface{}   `json:"cols"`
	Data    [][]interface{} `json:"data"`
}
