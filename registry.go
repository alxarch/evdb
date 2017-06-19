package meter

import (
	"errors"
	"sync"
)

type Registry struct {
	events map[string]*Event
	mu     sync.RWMutex
}

var (
	DuplicateTypeError = errors.New("Duplicate type registration.")
)

func (r *Registry) Register(name string, t *Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events == nil {
		r.events = make(map[string]*Event)
	}
	if _, ok := r.events[name]; ok {
		return DuplicateTypeError
	}
	r.events[name] = t
	return nil
}
func (r *Registry) MustRegister(name string, t *Event) {
	if err := r.Register(name, t); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(name string) *Event {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.events == nil {
		return nil
	}
	return r.events[name]
}
func (r *Registry) Each(fn func(name string, t *Event)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for n, t := range r.events {
		fn(n, t)
	}
}

var defaultRegistry = NewRegistry()

func Register(name string, t *Event) error {
	return defaultRegistry.Register(name, t)
}

func MustRegister(name string, t *Event) {
	defaultRegistry.MustRegister(name, t)
}

func GetEvent(name string) *Event {
	return defaultRegistry.Get(name)
}

func NewRegistry() *Registry {
	return &Registry{
		events: make(map[string]*Event),
	}
}

func (r *Registry) Logger() *Logger {
	lo := NewLogger()
	lo.Registry = r
	return lo
}
