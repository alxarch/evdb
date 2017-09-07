package meter2

import (
	"bytes"
	"fmt"
)

type Desc struct {
	name    string
	err     error
	layouts []Layout
	labels  []string
	index   map[string]int
}

func (d *Desc) Describe() *Desc {
	return d
}

type Descriptor interface {
	Describe() *Desc
}
type Collector interface {
	Collect(chan<- Metric)
}

func NewDesc(name string, labels []string, res ...Resolution) *Desc {
	labels = distinct(labels...)
	dims := LabelDimensions(labels...)
	m := make([]Layout, len(res))
	for i, r := range res {
		m[i] = Layout{r, dims}
	}
	return NewDescLayouts(name, labels, m...)
}
func NewDescLayouts(name string, labels []string, layouts ...Layout) *Desc {
	d := new(Desc)
	d.name, d.labels = name, labels
	d.index = PositionIndex(d.labels)
	if i := checkDistinct(labels); i != -1 {
		d.err = fmt.Errorf("Duplicate label %s", labels[i])
		return d
	}
	for i, layout := range layouts {
		if !layout.SubsetOf(d.labels) {
			d.err = fmt.Errorf("Layout %s not a subset of labels", layout)
			return d
		}
		layout = layout.Copy()
		layout.Dimensions.SortBy(d.index)
		layouts[i] = layout
	}
	d.layouts = layouts
	return d
}

func (d *Desc) Layouts() []Layout {
	return d.layouts
}
func (d *Desc) Error() error {
	return d.err
}
func (d *Desc) Name() string {
	return d.name
}
func (d *Desc) AppendName(b *bytes.Buffer) {
	b.WriteString(d.name)
}
func (d *Desc) Labels() []string {
	return d.labels
}

func (d *Desc) HasLabel(s string) bool {
	if d == nil || d.labels == nil {
		return false
	}
	for _, label := range d.labels {
		if label == s {
			return true
		}
	}
	return false
}

func (d *Desc) Dimensions(res Resolution) Dimensions {
	if d.layouts == nil {
		return Dimensions{}
	}
	name := res.Name()
	var dims Dimensions
	for _, layout := range d.layouts {
		if layout.Dimensions == nil || layout.name != name {
			continue
		}
		dims = append(dims, layout.Dimensions...)
	}
	return dims
}

type DescriptorFunc func() *Desc

func (f DescriptorFunc) Describe() *Desc {
	return f()
}

//
// type anonymousEvent struct {
// 	desc Desc
// 	// counters *Counters
// }
//
// var _ Event = anonymousEvent{}
//
// func newAnonymousEvent(labels []string, resolutions Granularity) (a anonymousEvent, err error) {
// 	resolutions, err = CheckEventParams(resolutions, labels)
// 	d := *Desc{}
// 	d.labels, d.resolutions, d.err = labels, resolutions, err
// 	a.desc = d
// 	if err == nil {
// 		a.counters = NewCounters()
// 	}
// 	return
// }
//
// func (a anonymousEvent) Describe() Desc {
// 	return a.desc
// }
// func (a anonymousEvent) Collect(ch chan<- Metric) {
// 	labels := a.desc.Labels()
// 	a.counters.Each(func(field string, c Counter) {
// 		values := SplitValueFieldN(field, len(labels))
// 		ch <- anonymousMetric{values, c, a.desc}
// 	})
// }

// func (e anonymousDesc) valueField(values []string, b *bytes.Buffer) {
// 	n := len(values)
// 	if n > len(e.labels) {
// 		n = len(e.labels)
// 	}
// 	for i := 0; i < n; i++ {
// 		if i > 0 {
// 			b.WriteByte(LabelSeparator)
// 		}
// 		b.WriteString(values[i])
// 	}
// }
//
// // Helper to avoid allocation when logging an event via Labels
// func (e anonymousDesc) valueFieldLabels(labels Labels, b *bytes.Buffer) {
// 	if labels == nil {
// 		return
// 	}
// 	for i := 0; i < len(e.labels); i++ {
// 		if i > 0 {
// 			b.WriteByte(LabelSeparator)
// 		}
// 		b.WriteString(labels[e.labels[i]])
// 	}
// 	return
// }

// func (a anonymousEvent) WithLabels(values Labels) Metric {
// 	b := bget()
// 	labels := a.desc.Labels()
// 	AppendValueFieldLabels(values, labels, b)
// 	c := a.counters.GetB(b.Bytes())
// 	bput(b)
// 	return anonymousMetric{LabelValues(values, labels...), c, a.desc}
// }

// func (a anonymousEvent) WithLabelValues(values []string) Metric {
// 	b := bget()
// 	AppendValueField(values, len(a.desc.Labels()), b)
// 	c := a.counters.GetB(b.Bytes())
// 	bput(b)
// 	return anonymousMetric{values, c, a.desc}
// }
//
// func NewNamedEvent(name string, labels []string, resolutions Granularity) (Event, error) {
// 	a, err := newAnonymousEvent(labels, resolutions)
// 	d := a.desc.(anonymousDesc)
// 	d.name = name
// 	a.desc = d
// 	return a, err
// }
