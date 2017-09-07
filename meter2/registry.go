package meter2

import (
	"errors"
	"sync"
)

var (
	ErrDuplicateEvent    = errors.New("Duplicate event registration.")
	ErrNilRegistry       = errors.New("Registry is nil")
	ErrNilEvent          = errors.New("Event is nil")
	ErrNilDesc           = errors.New("Desc is nil")
	ErrUnregisteredEvent = errors.New("Unregistered event")
)

type Registry struct {
	events map[string]Event
	mu     sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{
		events: make(map[string]Event),
	}
}

var defaultRegistry = NewRegistry()

func (c *Registry) Collect(ch chan<- Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, event := range c.events {
		event.Collect(ch)
	}
}

func (c *Registry) Get(name string) (e Event) {
	c.mu.RLock()
	e = c.events[name]
	c.mu.RUnlock()
	return
}

func (c *Registry) Register(event Event) error {
	if event == nil {
		return ErrNilEvent
	}
	desc := event.Describe()
	if desc == nil {
		return ErrNilDesc
	}
	if err := desc.Error(); err != nil {
		return err
	}
	name := desc.Name()
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, duplicate := c.events[name]; duplicate && d != nil {
		return ErrDuplicateEvent
	}
	c.events[name] = event
	return nil
}