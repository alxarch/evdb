package meter

import (
	"errors"
	"sync"
)

type Registry struct {
	types map[string]*EventType
	mu    sync.RWMutex
}

var (
	DuplicateTypeError = errors.New("Duplicate type registration.")
)

func (r *Registry) Register(name string, t *EventType) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if nil == r.types {
		r.types = make(map[string]*EventType)
	}
	if _, ok := r.types[name]; ok {
		return DuplicateTypeError
	}
	r.types[name] = t
	return nil
}

func (r *Registry) Get(name string) *EventType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.types == nil {
		return nil
	}
	return r.types[name]
}
func (r *Registry) Each(fn func(name string, t *EventType)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for n, t := range r.types {
		fn(n, t)
	}
}

var defaultRegistry = NewRegistry()

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func RegisterEventType(name string, t *EventType) error {
	return defaultRegistry.Register(name, t)
}

func GetEventType(name string) *EventType {
	return defaultRegistry.Get(name)
}

func NewRegistry() *Registry {
	return &Registry{
		types: make(map[string]*EventType),
	}
}
