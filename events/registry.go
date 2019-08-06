package events

import (
	"sync"
)

// Registry maps event names to events
type Registry struct {
	mu     sync.RWMutex
	events map[string]*Event
}

// Len returns the number of events in the registry
func (r *Registry) Len() (n int) {
	r.mu.RLock()
	n = len(r.events)
	r.mu.RUnlock()
	return
}

// AppendEvents appends registered events
func (r *Registry) AppendEvents(events []*Event) []*Event {
	r.mu.RLock()
	for _, event := range r.events {
		events = append(events, event)
	}
	r.mu.RUnlock()
	return events
}

// Get finds a registered event by name
func (r *Registry) Get(name string) (e *Event) {
	r.mu.RLock()
	e = r.events[name]
	r.mu.RUnlock()
	return
}

// Register registers an event if not already registered
func (r *Registry) Register(event *Event) bool {
	if event == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, duplicate := r.events[event.Name]; duplicate {
		return false
	}
	if r.events == nil {
		r.events = make(map[string]*Event)
	}
	r.events[event.Name] = event
	return true
}
