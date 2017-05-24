package meter

import (
	"sync"
	"time"
)

type Registry struct {
	Pool    *AttributesPool
	Aliases Aliases
	types   map[string]*EventType
	mu      sync.RWMutex
}

func (r *Registry) NewEvent(name string, attr []string) *Event {
	t := r.Get(name)
	if r == nil {
		return nil
	}
	return &Event{
		Type:       t,
		Time:       time.Now(),
		Count:      1,
		Attributes: r.Attributes(attr...),
	}
}

func (r *Registry) Register(name string, t *EventType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if nil == r.types {
		r.types = make(map[string]*EventType)
	}
	if nil == t {
		delete(r.types, name)
	} else {
		r.types[name] = t
	}
}

func (r *Registry) GetOrDefault(name string, def *EventType) *EventType {
	if t := r.Get(name); t != nil {
		return t
	}
	r.Register(name, def)
	return def
}

func (r *Registry) Get(name string) *EventType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.types == nil {
		return nil
	}
	return r.types[name]
}

var DefaultRegistry = NewRegistry()

func RegisterEventType(name string, t *EventType) {
	DefaultRegistry.Register(name, t)
}

func GetEventType(name string) *EventType {
	return DefaultRegistry.Get(name)
}

// Attributes creates an Attributes array replacing any aliases
func (r *Registry) Attributes(attr ...string) Attributes {
	n := len(attr)
	n = n - n%2
	cp := r.Pool.Get(n)
	for i := 0; i < n; i += 2 {
		cp[i] = r.Aliases.Alias(attr[i])
		cp[i+1] = attr[i+1]
	}
	return cp
}

func NewRegistry() *Registry {
	return &Registry{
		types: make(map[string]*EventType),
		Pool:  defaultPool,
	}
}

func (r *Registry) SetAlias(aliases ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Aliases == nil {
		r.Aliases = Aliases(make(map[string]string))
	}
	r.Aliases.Set(aliases...)
}

// var registryContextKey = struct{ string }{"meter-registry"}
//
// func RegistryFromContext(ctx context.Context) *Registry {
// 	if r, ok := ctx.Value(registryContextKey).(*Registry); ok {
// 		return r
// 	}
// 	return nil
// }
//
// func MustRegistryFromContext(ctx context.Context) *Registry {
// 	if r := RegistryFromContext(ctx); r != nil {
// 		return r
// 	}
// 	panic(errors.New("No meter registry in context"))
// }
