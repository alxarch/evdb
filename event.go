package meter

type Event struct {
	Type   *EventType
	Value  float64
	Labels Labels
}
