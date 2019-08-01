package events

// Event stores counters for an event
type Event struct {
	Name   string   `json:"name"`
	Labels []string `json:"labels"`
	*Counters
}

// NewEvent creates a new Event using the specified labels
func NewEvent(name string, labels ...string) *Event {
	const defaultEventSize = 64
	e := Event{
		Name:     name,
		Labels:   labels,
		Counters: NewCounters(defaultEventSize),
	}

	return &e
}

// MergeTask creates a task that merges src to dst filling missing labels with values from static
func MergeTask(dst, src *Event, static map[string]string) func() {
	type labelIndex struct {
		Index int
		Value string
	}
	index := make([]labelIndex, len(dst.Labels))
	values := make([]string, len(dst.Labels))
	for i, label := range dst.Labels {
		index[i].Index = -1
		index[i].Value = static[label]
		for j := range src.Labels {
			if src.Labels[j] == label {
				index[i].Index = j
				break
			}
		}
	}
	var scratch CounterSlice
	return func() {
		scratch = src.Counters.Flush(scratch.Reset())
		for i := range scratch {
			c := &scratch[i]
			for j := range values {
				i := &index[j]
				if 0 <= i.Index && i.Index < len(c.Values) {
					values[j] = c.Values[i.Index]
				} else {
					values[j] = i.Value
				}
			}
			dst.Add(c.Count, values...)
		}

	}
}
