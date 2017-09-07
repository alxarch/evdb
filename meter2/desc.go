package meter2

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
	if labels != nil {
		labels = distinct(labels...)
	} else {
		labels = []string{}
	}
	d.name, d.labels = name, labels
	d.resolutions = distinctNonZeroResolutions(res...)
	return d
}

func (d *Desc) Error() error {
	return d.err
}
func (d *Desc) Name() string {
	return d.name
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

func (d *Desc) LabelIndex(label string) int {
	return indexOf(d.labels, label)
}
func (d *Desc) HasLabel(label string) bool {
	return indexOf(d.labels, label) != -1
}

func distinctNonZeroResolutions(res ...Resolution) []Resolution {
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
