package meter

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

type Result struct {
	Event string
	// Group  LabelValues
	Labels LabelValues
	Data   DataPoints
}

type DataPoint struct {
	Timestamp, Value int64
}

type DataPoints []DataPoint

func (data DataPoints) Find(tm time.Time) (int64, bool) {
	if i := data.IndexOf(tm); i < 0 {
		return 0, false
	} else {
		return data[i].Value, true
	}
}

func (data DataPoints) IndexOf(tm time.Time) int {
	if data == nil {
		return -1
	}
	ts := tm.Unix()
	for i := 0; i < len(data); i++ {
		if data[i].Timestamp == ts {
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

func (s Results) IndexOf(event string, values LabelValues) int {
	if s == nil {
		return -1
	}
	for i, r := range s {
		if r.Event == event && r.Labels.Equal(values) {
			return i
		}
	}
	return -1
}
func (s Results) Find(event string, values LabelValues) *Result {
	if i := s.IndexOf(event, values); i < 0 {
		return nil
	} else {
		return &s[i]
	}
}

func CollectResults(scan <-chan ScanResult) <-chan Results {
	out := make(chan Results)
	go func() {
		var results Results
		for r := range scan {
			if len(r.Group) != 0 && len(r.Values) != 0 {
				for key := range r.Values {
					if indexOf(r.Group, key) < 0 {
						delete(r.Values, key)
					}
				}
			}
			results = results.Append(r)
		}
		out <- results
	}()
	return out

}

func (results Results) Append(r ScanResult) Results {
	values := r.Values
	if r.Group != nil {
		values = LabelValues{}
		for _, g := range r.Group {
			values[g] = r.Values[g]
		}
	}
	p := DataPoint{r.Time.Unix(), r.count}
	if i := results.IndexOf(r.Name, values); i < 0 {
		return append(results, Result{
			Event:  r.Name,
			Labels: values,
			// Group:  r.Group,
			Data: DataPoints{p},
		})
	} else if j := results[i].Data.IndexOf(r.Time); j < 0 {
		results[i].Data = append(results[i].Data, p)
	} else {
		results[i].Data[j].Value += r.count
	}
	return results
}
