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
	tmp := s[i]
	s[i] = s[j]
	s[j] = tmp
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

func (s DataPoints) MarshalJSON() (data []byte, err error) {
	if s == nil {
		return nil, nil
	}
	data = make([]byte, 1, len(s)*64+2)
	data[0] = '['
	for i, p := range s {
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

func (p *DataPoint) UnmarshalJSON(data []byte) (err error) {
	value := [2]int64{}
	if err = json.Unmarshal(data, &value); err == nil {
		p.Timestamp, p.Value = value[0], value[1]
	}
	return
}

type TimeSeries struct {
	Event  string      `json:"event"`
	Fields Fields      `json:"fields"`
	Data   []DataPoint `json:"data"`
}

type SummaryScan struct {
	Event      string    `json:"event"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Match      []Field   `json:"match,omitmepty"`
	Group      []string  `json:"group"`
	Results    []Summary `json:"results"`
	EmptyValue string    `json:"empty"`
}

type Summary struct {
	Label  string           `json:"label"`
	Values map[string]int64 `json:"values"`
}

func (s *SummaryScan) ScanEvent(_ string, id uint64, fields Fields, _, n int64) {
	if len(s.Results) != len(s.Group) {
		s.Results = make([]Summary, len(s.Group))
		for i, label := range s.Group {
			s.Results[i] = Summary{
				Label:  label,
				Values: make(map[string]int64),
			}
		}
	}
	for i := range s.Results {
		d := &s.Results[i]
		if i := fields.IndexOf(d.Label); 0 <= i && i < len(fields) {
			f := &fields[i]
			d.Values[f.Value] += n
		} else if s.EmptyValue != "" {
			d.Values[s.EmptyValue] += n
		}
	}
}

func (s *SummaryScan) Matcher() RawFieldMatcher {
	m := getRawMatcher()
	m.Reset(s.Match)
	return m
}

type TimeSeriesScan struct {
	Event      string        `json:"event"`
	Match      Fields        `json:"match,omitempty"`
	Group      []string      `json:"group,omitempty"`
	Start      time.Time     `json:"start"`
	End        time.Time     `json:"end"`
	Step       time.Duration `json:"step"`
	EmptyValue string        `json:"empty,omitempty"`
	Results    []TimeSeries  `json:"results"`
	index      map[uint64]*TimeSeries
	labels     []string
}

func (q *TimeSeriesScan) ScanEvent(_ string, id uint64, fields Fields, ts, n int64) {
	s := q.findOrCreateTimeSeries(id, fields)
	ts /= int64(time.Second)
	if last := len(s.Data) - 1; 0 <= last && last < len(s.Data) {
		if d := &s.Data[last]; d.Timestamp == ts {
			d.Value += n
			return
		}
	}
	s.Data = append(s.Data, DataPoint{Timestamp: ts, Value: n})
}

func (q *TimeSeriesScan) Labels() []string {
	if q.labels == nil {
		if len(q.Group) > 0 {
			q.labels = make([]string, len(q.Group))
			copy(q.labels, q.Group)
		} else if len(q.Match) > 0 {
			q.labels = make([]string, 0, len(q.Match))
			q.labels = q.Match.AppendLabels(q.labels)
		} else {
			q.labels = []string{}
		}
	}
	return q.labels
}

func (q *TimeSeriesScan) Matcher() RawFieldMatcher {
	if len(q.Match) == 0 {
		return identityFieldMatcher{}
	}
	m := new(rawFieldMatcher)
	m.Reset(q.Match)
	return m
}

func (q *TimeSeriesScan) findOrCreateTimeSeries(id uint64, fields Fields) (ts *TimeSeries) {
	if ts = q.index[id]; ts != nil {
		return ts
	}
	if q.index == nil {
		q.index = make(map[uint64]*TimeSeries)
	}
	fields = fields.Filter(q.Labels()...)
	for _, label := range q.Labels() {
		if fields.IndexOf(label) == -1 {
			fields = append(fields, Field{Label: label, Value: q.EmptyValue})
		}
	}
	sort.Sort(fields)
	for i := range q.Results {
		ts = &q.Results[i]
		if ts.Fields.Equal(fields) {
			q.index[id] = ts
			return ts
		}
	}
	q.Results = append(q.Results, TimeSeries{
		Event:  q.Event,
		Fields: fields,
	})
	ts = &q.Results[len(q.Results)-1]
	q.index[id] = ts
	return
}

func (q *TimeSeriesScan) SetValues(values url.Values) {
	q.Event = values.Get("event")
	q.Step, _ = time.ParseDuration(values.Get("step"))
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	q.Start = time.Unix(start, 0)
	end, _ := strconv.ParseInt(values.Get("end"), 10, 64)
	q.End = time.Unix(end, 0)
	q.Group = append(q.Group[:0], values["group"]...)
	q.Match = SplitAppendFields(q.Match[:0], ':', values["q"]...)
	q.EmptyValue = values.Get("empty")
}

func (q *SummaryScan) SetValues(values url.Values) {
	q.Event = values.Get("event")
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	q.Start = time.Unix(start, 0)
	end, _ := strconv.ParseInt(values.Get("end"), 10, 64)
	q.End = time.Unix(end, 0)
	q.Group = append(q.Group[:0], values["group"]...)
	q.Match = SplitAppendFields(q.Match[:0], ':', values["q"]...)
	q.EmptyValue = values.Get("empty")
}
