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

func (lo *Logger) MustLog(name string, n int64, labels Labels) {
	if err := lo.Log(name, n, labels); err != nil {
		panic(err)
	}
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
	errors  int64
	wg      sync.WaitGroup
}

func (lo *Logger) Log(name string, n int64, labels Labels) error {
	if e := lo.Registry.Get(name); e != nil {
		e.LogWith(n, e.AliasedLabels(labels, lo.Aliases))
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

func (lo *Logger) Persist(tm time.Time, r redis.UniversalClient) error {
	err := make(chan error)
	lo.wg.Add(1)
	go func() {
		defer lo.wg.Done()
		err <- lo.persist(tm, r)
	}()
	return <-err

}
func (lo *Logger) persist(tm time.Time, r redis.UniversalClient) error {
	errs := make(map[string]error)
	lo.Registry.Each(func(name string, e *Event) {
		if err := e.Persist(tm, r); err != nil {
			atomic.AddInt64(&lo.errors, 1)
			errs[name] = err
		}
	})
	if len(errs) > 0 {
		return FlushError(errs)
	}
	return nil
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

func Log(name string, n int64, labels Labels) error {
	return defaultLogger.Log(name, n, labels)
}

func LogEvent(e *Event, n int64, labels Labels) {
	if e != nil {
		e.LogWith(n, e.AliasedLabels(labels, defaultAliases))
	}
}
func LogEventWithLabelValues(e *Event, n int64, values ...string) {
	if e != nil {
		e.LogWithLabelValues(n, values...)
	}
}

func MustLog(name string, n int64, labels Labels) {
	defaultLogger.MustLog(name, n, labels)
}

func Persist(tm time.Time, r redis.UniversalClient) error {
	return defaultLogger.Persist(tm, r)
}

func MustPersist(tm time.Time, r redis.UniversalClient) {
	if err := Persist(tm, r); err != nil {
		panic(err)
	}
}
