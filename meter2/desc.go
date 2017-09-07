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
func (d *Desc) Labels() []string {
	return d.labels
}
func (d *Desc) Resolutions() []Resolution {
	return d.resolutions
}

func (d *Desc) LabelIndex(label string) int {
	return indexOf(d.labels, label)
}
func (d *Desc) HasLabel(label string) bool {
	return indexOf(d.labels, label) != -1
}
