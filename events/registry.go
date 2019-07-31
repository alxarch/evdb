package events

import (
	"sync"
)

type Registry struct {
	mu     sync.RWMutex
	events map[string]*Event
}

func (r *Registry) Events() map[string]*Event {
	events := r.appendEvents(make([]*Event, 0, 8))
	if len(events) == 0 {
		return nil
	}
	m := make(map[string]*Event, len(events))
	for _, e := range events {
		m[e.Name] = e
	}
	return m
}

func (r *Registry) appendEvents(events []*Event) []*Event {
	r.mu.RLock()
	for _, event := range r.events {
		events = append(events, event)
	}
	r.mu.RUnlock()
	return events
}

func (r *Registry) Get(name string) (e *Event) {
	r.mu.RLock()
	e = r.events[name]
	r.mu.RUnlock()
	return
}

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
