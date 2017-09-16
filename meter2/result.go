package meter2

import (
	"sort"
	"strconv"
	"time"
)

type Result struct {
	Event  string
	Group  string
	Labels LabelValues
	Data   DataPoints
}

type DataPoint struct {
	Timestamp, Value int64
}

type DataPoints []DataPoint

func (data DataPoints) Find(tm time.Time) (int64, bool) {
	for i := 0; i < len(data); i++ {
		if data[i].Timestamp == tm.Unix() {
			return data[i].Value, true
		}
	}
	return 0, false
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

func (s DataPoints) MarshalJSON() ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	data := make([]byte, 0, len(s)*32)
	data = append(data, '[')
	for i, p := range s {
		if i > 0 {
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

type Results []Result

func (s Results) Find(event, group string, values LabelValues) int {
iloop:
	for i, r := range s {
		if r.Event == event && r.Group == group && len(values) == len(r.Labels) {
			for key, value := range values {
				if r.Labels[key] != value {
					continue iloop
				}
			}
			return i
		}
	}
	return -1
}
