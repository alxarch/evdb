package meter2

import (
	"sort"
	"strconv"
)

type Result struct {
	Event  string
	Labels LabelValues
	Data   DataPointSequence

	key, field string
}

type DataPoint struct {
	Timestamp, Value int64
}

type DataPointSequence []DataPoint

var _ sort.Interface = DataPointSequence{}

func (s DataPointSequence) Sort() {
	sort.Sort(s)
}

func (s DataPointSequence) Len() int {
	return len(s)
}

func (s DataPointSequence) Swap(i, j int) {
	tmp := s[i]
	s[i] = s[j]
	s[j] = tmp
}

func (s DataPointSequence) Less(i, j int) bool {
	return s[i].Timestamp < s[j].Timestamp
}

func (s DataPointSequence) MarshalJSON() ([]byte, error) {
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

type ResultSequence []Result

func (s ResultSequence) Find(key, field string) int {
	for i, r := range s {
		if r.key == key && r.field == field {
			return i
		}
	}
	return -1
}
