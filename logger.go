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

func (lo *Logger) MustLog(name string, n int64, labels ...string) {
	if err := lo.Log(name, n, labels...); err != nil {
		panic(err)
	}
}
func (lo *Logger) Log(name string, n int64, labels ...string) error {
	if e := lo.Registry.Get(name); e != nil {
		e.Log(n, e.Labels(labels, lo.Aliases)...)
		return nil
	}
	return UnregisteredEventError
}

func (lo *Logger) MustPersist(tm time.Time, r *redis.Client) {
	if err := lo.Persist(tm, r); err != nil {
		panic(err)
	}
}

func (lo *Logger) Persist(tm time.Time, r *redis.Client) error {
	err := make(chan error)
	lo.wg.Add(1)
	go func() {
		defer lo.wg.Done()
		err <- lo.persist(tm, r)
	}()
	return <-err

}
func (lo *Logger) persist(tm time.Time, r *redis.Client) error {
	errs := make(map[string]error)
	lo.Registry.Each(func(name string, t *Event) {
		if err := t.Persist(tm, r); err != nil {
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
	Aliases Aliases
	wg      sync.WaitGroup
}

func NewLogger() *Logger {
	return &Logger{
		Registry: NewRegistry(),
		Aliases:  NewAliases(),
	}
}

func (lo *Logger) Close() {
	lo.wg.Wait()
}

var defaultLogger = &Logger{
	Aliases:  defaultAliases,
	Registry: defaultRegistry,
}

func Log(name string, n int64, labels ...string) error {
	return defaultLogger.Log(name, n, labels...)
}

func LogEvent(e *Event, n int64, labels ...string) {
	if e != nil {
		e.Log(n, e.Labels(labels, defaultAliases)...)
	}
}

func MustLog(name string, n int64, labels ...string) {
	defaultLogger.MustLog(name, n, labels...)
}
func Persist(tm time.Time, r *redis.Client) error {
	return defaultLogger.Persist(tm, r)
}
func MustPersist(tm time.Time, r *redis.Client) {
	defaultLogger.MustPersist(tm, r)
}
