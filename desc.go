package meter

import (
	"net/url"
)

type Desc struct {
	name        string
	err         error
	resolutions []Resolution
	labels      []string
	t           MetricType
}

type MetricType uint8

const (
	_                    MetricType = iota
	MetricTypeIncrement             // Increment value on store
	MetricTypeUpdateOnce            // Update value once on store
	MetricTypeUpdate                // Update value on store
)

func (t MetricType) RedisCmd() string {
	switch t {
	case MetricTypeIncrement:
		return "HINCRBY"
	case MetricTypeUpdateOnce:
		return "HSETNX"
	case MetricTypeUpdate:
		return "HSET"
	default:
		return ""
	}
}

func (d *Desc) Describe() *Desc {
	return d
}

type Descriptor interface {
	Describe() *Desc
}

func NewCounterDesc(name string, labels []string, res ...Resolution) *Desc {
	return NewDesc(MetricTypeIncrement, name, labels, res...)
}
func NewValueDesc(name string, labels []string, res ...Resolution) *Desc {
	return NewDesc(MetricTypeUpdate, name, labels, res...)
}
func NewDesc(t MetricType, name string, labels []string, res ...Resolution) *Desc {
	d := new(Desc)
	if labels != nil {
		labels = distinct(labels...)
	} else {
		labels = []string{}
	}
	d.t, d.name, d.labels = t, name, labels
	d.resolutions = distinctNonZeroResolutions(res...)
	return d
}

func (d *Desc) Error() error {
	return d.err
}
func (d *Desc) Name() string {
	return d.name
}
func (d *Desc) Type() MetricType {
	return d.t
}
func (d *Desc) Labels() []string {
	return d.labels
}
func (d *Desc) Resolutions() []Resolution {
	return d.resolutions
}
func (d *Desc) Resolution(name string) (r Resolution, ok bool) {
	for _, res := range d.resolutions {
		if res.Name() == name {
			return res, true
		}
	}
	return
}

// func (d *Desc) LabelIndex(label string) int {
// 	return indexOf(d.labels, label)
// }
func (d *Desc) HasLabel(label string) bool {
	return indexOf(d.labels, label) != -1
}

// func (d *Desc) LabelValues(values []string) LabelValues {
// 	lvs := LabelValues{}
// 	for i := 0; i < len(d.labels) && i < len(values); i++ {
// 		if v := values[i]; v != "" {
// 			lvs[d.labels[i]] = values[i]
// 		}
// 	}
// 	return lvs
// }

func distinctNonZeroResolutions(res ...Resolution) []Resolution {
	if res == nil {
		return []Resolution{}
	}
	n := 0
iloop:
	for i := 0; i < len(res); i++ {
		if res[i].IsZero() {
			continue
		}
		for j := 0; j < n; j++ {
			if res[i].Name() == res[j].Name() {
				continue iloop
			}
		}
		res[n] = res[i]
		n++
	}
	return res[:n]
}

func (d *Desc) MatchingQueries(q url.Values) url.Values {
	if d == nil || q == nil {
		return nil
	}
	m := make(map[string][]string, len(q))
	for key, values := range q {
		if d.HasLabel(key) {
			m[key] = values
		}
	}
	return m
}

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

func distinct(values ...string) []string {
	if values == nil {
		return []string{}
	}
	j := 0
	for _, value := range values {
		if 0 <= j && j < len(values) && indexOf(values[:j], value) == -1 {
			values[j] = value
			j++
		}
	}
	return values[:j]
}
