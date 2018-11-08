package meter

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"
)

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

type ScanResult struct {
	Event  string      `json:"event"`
	Fields Fields      `json:"fields,omitempty"`
	Total  int64       `json:"total"`
	Data   []DataPoint `json:"data,omitempty"`
}

var resultsPool sync.Pool

func blankResult() *ScanResult {
	if x := resultsPool.Get(); x != nil {
		return x.(*ScanResult)
	}
	return new(ScanResult)
}

func closeResults(results ...*ScanResult) {
	for _, r := range results {
		r.Close()
	}
}

func (r *ScanResult) Close() {
	if r != nil {
		r.Reset()
		resultsPool.Put(r)
	}
}

func (r *ScanResult) Reset() {
	*r = ScanResult{
		Fields: r.Fields[:0],
		Data:   r.Data[:0],
	}
}

func (r *ScanResult) Add(ts, n int64) error {
	r.Total += n
	if len(r.Data) > 0 {
		if d := r.Data[len(r.Data)-1]; d.Timestamp == ts {
			d.Value += n
			return nil
		}
	}
	r.Data = append(r.Data, DataPoint{Timestamp: ts, Value: n})
	return nil
}

type MultiScanResults map[string][]*ScanResult

func (m MultiScanResults) Close() {
	if m == nil {
		return
	}
	for event, results := range m {
		delete(m, event)
		closeResults(results...)
	}
}
func (m MultiScanResults) Results() (results []*ScanResult) {
	for event := range m {
		results = append(results, m[event]...)
	}
	return results
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

func (sums FieldSummaries) Append(results ...*ScanResult) FieldSummaries {
	for _, r := range results {
		for j := range r.Fields {
			f := &r.Fields[j]
			sums = sums.append(r.Event, f.Label, f.Value, r.Total)
		}
	}
	return sums
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

func (fields Fields) AppendValues(dst []string, empty string, labels ...string) []string {
	for _, label := range labels {
		v := fields.Get(label)
		if v == "" {
			v = empty
		}
		dst = append(dst, v)
	}
	return dst
}

func NewEventSummaries(empty string, results ...*ScanResult) *EventSummaries {
	s := new(EventSummaries)
	for _, r := range results {
		s.Events = appendDistinct(s.Events, r.Event)
		for i := range r.Fields {
			f := &r.Fields[i]
			s.Labels = appendDistinct(s.Labels, f.Label)
		}
	}
	sort.Strings(s.Labels)
	values := make([]string, len(s.Labels))
	sort.Strings(s.Events)
	for _, r := range results {
		if r.Total == 0 {
			continue
		}
		values = r.Fields.AppendValues(values[:0], empty, s.Labels...)
		s.add(r.Event, values, r.Total)
	}

	return s
}

type EventSummary struct {
	Values []string
	Totals map[string]int64
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
