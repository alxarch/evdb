package meter2

import (
	"bytes"
)

type Desc struct {
	name        string
	err         error
	resolutions []Resolution
	labels      []string
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
	d := new(Desc)
	labels = distinct(labels...)
	d.name, d.labels = name, labels
	d.resolutions = res
	return d
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
func (d *Desc) Resolutions() []Resolution {
	return d.resolutions
}

func (d *Desc) LabelIndex(label string) int {
	if d != nil {
		for i := 0; i < len(d.labels); i++ {
			if label == d.labels[i] {
				return i
			}

		}
	}
	return -1
}
func (d *Desc) HasLabel(label string) bool {
	return d.LabelIndex(label) != -1
}

type DescriptorFunc func() *Desc

func (f DescriptorFunc) Describe() *Desc {
	return f()
}
