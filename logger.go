package meter

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

var (
	UnregisteredEventError = errors.New("Unregistered event type")
)

func (lo *Logger) Log(name string, n int64, attr ...string) error {
	if e := lo.Registry.Get(name); e != nil {
		e.Log(n, e.Labels(attr, lo.Aliases)...)
		return nil
	}
	return UnregisteredEventError
}

func (lo *Logger) Persist(tm time.Time) error {
	err := make(chan error)
	lo.wg.Add(1)
	go func() {
		defer lo.wg.Done()
		err <- lo.persist(tm)
	}()
	return <-err

}
func (lo *Logger) persist(tm time.Time) error {
	errs := make(map[string]error)
	lo.Registry.Each(func(name string, t *Event) {
		if err := t.Persist(tm, lo.Redis); err != nil {
			errs[name] = err
		}
	})
	if len(errs) > 0 {
		return FlushError(errs)
	}
	return nil
}

type FlushError map[string]error

func (e FlushError) Error() string {
	if len(e) > 0 {
		names := make([]string, len(e))
		i := 0
		for name, _ := range e {
			names[i] = name
			i++
		}
		return fmt.Sprintf("Failed to persist events %s", names)
	}
	return "No error"
}

type Logger struct {
	*Registry
	Redis   *redis.Client
	Aliases Aliases
	wg      sync.WaitGroup
}

func NewLogger(r *redis.Client) *Logger {
	lo := &Logger{
		Registry: defaultRegistry,
		Aliases:  NewAliases(),
		Redis:    r,
	}
	return lo
}

func (lo *Logger) Close() {
	lo.wg.Wait()
}
