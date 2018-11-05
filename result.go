package meter

import (
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
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

var _ sort.Interface = DataPoints{}

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

type TimeSeries struct {
	Event  string     `json:"event"`
	Fields Fields     `json:"fields"`
	Data   DataPoints `json:"data"`
}

type EventScan struct {
	*Query
	Event   string
	Results []TimeSeries
}
type SummaryScan struct {
	*Query
	Event   string
	Results []Summary
}
type EventSummary struct {
	*Query
	Event   string    `json:"event"`
	Results []Summary `json:"results"`
}

type Summary struct {
	Label  string           `json:"label"`
	Values map[string]int64 `json:"values"`
}

func NewSummaryScan(event string, q *Query) *SummaryScan {
	scan := SummaryScan{
		Query:   q,
		Event:   event,
		Results: make([]Summary, len(q.Group)),
	}
	for i, label := range q.Group {
		scan.Results[i] = Summary{
			Label:  label,
			Values: make(map[string]int64),
		}
	}
	return &scan
}

func (scan *SummaryScan) ScanEvent(_ string, id uint64, fields Fields, _, n int64) {
	for i := range scan.Results {
		d := &scan.Results[i]
		if i := fields.IndexOf(d.Label); 0 <= i && i < len(fields) {
			f := &fields[i]
			d.Values[f.Value] += n
		} else if scan.EmptyValue != "" {
			d.Values[scan.EmptyValue] += n
		}
	}
}

type Query struct {
	Match      Fields        `json:"match,omitempty"`
	Group      []string      `json:"group,omitempty"`
	Start      time.Time     `json:"start"`
	End        time.Time     `json:"end"`
	Step       time.Duration `json:"step"`
	EmptyValue string        `json:"empty,omitempty"`
}

type TimeSeriesScan struct {
	*Query
	Event   string       `json:"event"`
	Results []TimeSeries `json:"results"`
	index   map[uint64]int
	labels  []string
	step    int64
	err     error
}

func (scan TimeSeriesScan) Err() error {
	return scan.err
}

func NewTimeSeriesScan(event string, q *Query) *TimeSeriesScan {
	scan := TimeSeriesScan{
		Query: q,
		Event: event,
		index: make(map[uint64]int),
	}
	if len(q.Group) > 0 {
		scan.labels = append(make([]string, 0, len(q.Group)), q.Group...)
	} else if len(q.Match) > 0 {
		scan.labels = q.Match.AppendLabelsDistinct(make([]string, 0, len(q.Match)))
	}
	if scan.Step < time.Second {
		scan.step = 1
	} else {
		scan.step = int64(scan.Step / time.Second)
	}

	return &scan
}

func (scan *TimeSeriesScan) ScanEvent(_ string, id uint64, fields Fields, ts, n int64) {
	i := scan.indexOf(id, fields)
	if 0 <= i && i < len(scan.Results) {
		r := &scan.Results[i]
		ts -= ts % scan.step
		if last := len(r.Data) - 1; 0 <= last && last < len(r.Data) {
			if d := &r.Data[last]; d.Timestamp == ts {
				d.Value += n
				return
			}
		}
		r.Data = append(r.Data, DataPoint{Timestamp: ts, Value: n})
	}
}

func (scan *TimeSeriesScan) Labels() []string {
	if scan.labels == nil {
	}
	return scan.labels
}

func (scan *TimeSeriesScan) set(id uint64, i int) {
	if scan.index == nil {
		scan.index = make(map[uint64]int)
	}
	scan.index[id] = i
}

func (scan *TimeSeriesScan) normalizeFields(fields Fields) Fields {
	// Get a filtered copy of fields
	result := Fields(make([]Field, len(scan.labels)))
	for i, label := range scan.labels {
		if j := fields.IndexOf(label); 0 <= j && j < len(fields) {
			result[i] = fields[j]
		} else {
			result[i] = Field{Label: label, Value: scan.EmptyValue}
		}
	}
	sort.Sort(result)
	return result
}
func (scan *TimeSeriesScan) indexOf(id uint64, fields Fields) int {
	if i, ok := scan.index[id]; ok {
		return i
	}

	fields = scan.normalizeFields(fields)

	for i := range scan.Results {
		r := &scan.Results[i]
		if r.Fields.Equal(fields) {
			scan.index[id] = i
			return i
		}
	}
	scan.index[id] = len(scan.Results)
	scan.Results = append(scan.Results, TimeSeries{
		Event:  scan.Event,
		Fields: fields,
	})
	return scan.index[id]
}

func (q *Query) SetValues(values url.Values) {
	q.Step, _ = time.ParseDuration(values.Get("step"))
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	q.Start = time.Unix(start, 0)
	end, _ := strconv.ParseInt(values.Get("end"), 10, 64)
	q.End = time.Unix(end, 0)
	q.Group = append(q.Group[:0], values["group"]...)
	q.Match = SplitAppendFields(q.Match[:0], ':', values["match"]...)
	sort.Stable(q.Match)
	q.EmptyValue = values.Get("empty")
}
