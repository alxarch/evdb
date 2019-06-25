package meter

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
