package meter

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
		e.Log(n, e.AliasedLabels(labels, lo.Aliases)...)
		return nil
	}
	return UnregisteredEventError
}

func (lo *Logger) MustPersist(tm time.Time, r *redis.Client) {
	if err := lo.Persist(tm, r); err != nil {
		panic(err)
	}
}

func (lo *Logger) FlushErrors() int64 {
	return atomic.LoadInt64(&lo.errors)
}

func (lo *Logger) Persist(tm time.Time, r *redis.Client) error {
	err := make(chan error)
	lo.wg.Add(1)
	go func() {
		defer lo.wg.Done()
		err <- lo.persist(tm, r, lo.Resolutions...)
	}()
	return <-err

}
func (lo *Logger) PersistAt(tm time.Time, r *redis.Client, res ...*Resolution) error {
	err := make(chan error)
	lo.wg.Add(1)
	go func() {
		defer lo.wg.Done()
		err <- lo.persist(tm, r, res...)
	}()
	return <-err

}
func (lo *Logger) persist(tm time.Time, r *redis.Client, res ...*Resolution) error {
	errs := make(map[string]error)
	lo.Registry.Each(func(name string, t *Event) {
		if err := t.Persist(tm, r, res...); err != nil {
			atomic.AddInt64(&lo.errors, 1)
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
	Aliases     Aliases
	Resolutions []*Resolution
	errors      int64
	wg          sync.WaitGroup
}

func NewLogger(resolutions ...*Resolution) *Logger {
	if len(resolutions) == 0 {
		resolutions = append(resolutions, NoResolution)
	}
	return &Logger{
		Registry:    NewRegistry(),
		Aliases:     NewAliases(),
		Resolutions: resolutions,
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
		e.Log(n, e.AliasedLabels(labels, defaultAliases)...)
	}
}

func MustLog(name string, n int64, labels ...string) {
	defaultLogger.MustLog(name, n, labels...)
}
func Persist(tm time.Time, r *redis.Client, res ...*Resolution) error {
	return defaultLogger.PersistAt(tm, r, res...)
}
func MustPersist(tm time.Time, r *redis.Client, res ...*Resolution) {
	if err := Persist(tm, r, res...); err != nil {
		panic(err)
	}
}
