package meter

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

type Result struct {
	Event  string
	Labels map[string]string
	Data   DataPoints
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

type Results []Result

func (r *Result) Match(values map[string]string) bool {

	if len(r.Labels) != len(values) {
		return false
	}
	if r.Labels == nil && values == nil {
		return true
	}
	for key, value := range values {
		if r.Labels[key] != value {
			return false
		}
	}
	return true
}
func (rs Results) IndexOf(event string, values map[string]string) int {
	if rs == nil {
		return -1
	}
	for i, r := range rs {
		if r.Event == event && r.Match(values) {
			return i
		}
	}
	return -1
}

func (rs Results) Find(event string, values map[string]string) *Result {
	if i := rs.IndexOf(event, values); i < 0 {
		return nil
	} else {
		return &rs[i]
	}
}

type FrequencyMap map[string]map[string]int64

func (rs Results) FrequencyMap() FrequencyMap {
	m := make(map[string]map[string]int64, len(rs))
	for i := 0; i < len(rs); i++ {
		r := rs[i]
		for j := 0; j < len(r.Data); j++ {
			p := r.Data[j]
			for label, value := range r.Labels {
				if m[label] == nil {
					m[label] = make(map[string]int64)
				}
				m[label][value] += p.Value
			}
		}
	}
	return m
}
