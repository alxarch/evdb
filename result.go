package evdb

import "encoding/json"

// Result is a query result
type Result struct {
	TimeRange
	Event  string     `json:"event,omitempty"`
	Fields Fields     `json:"fields,omitempty"`
	Data   DataPoints `json:"data,omitempty"`
}

func (r *Result) MarshalJSON() ([]byte, error) {
	type jsonResult struct {
		TimeRange [3]int64   `json:"time"`
		Event     string     `json:"event,omitempty"`
		Fields    Fields     `json:"fields,omitempty"`
		Data      DataPoints `json:"data,omitempty"`
	}
	tmp := jsonResult{
		TimeRange: [3]int64{
			r.Start.Unix(), r.End.Unix(), int64(r.Step.Seconds()),
		},
		Event:  r.Event,
		Fields: r.Fields,
		Data:   r.Data,
	}
	return json.Marshal(&tmp)
}

// Results is a slice of results
type Results []Result

// Add adds a result
func (results Results) Add(event string, fields Fields, t int64, v float64) Results {
	for i := range results {
		r := &results[i]
		if r.Event != event {
			continue
		}
		if !r.Fields.Equal(fields) {
			continue
		}
		r.Data = r.Data.Add(t, v)
		return results
	}
	return append(results, Result{
		Event:  event,
		Fields: fields.Copy(),
		Data:   []DataPoint{{t, v}},
	})

}
