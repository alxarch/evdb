package meter

import (
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

var (
	UnregisteredEventError = errors.New("Unregistered event type")
)

func (lo *Logger) Log(name string, n float64, attr ...string) error {
	t := lo.Registry.Get(name)
	if t == nil {
		return UnregisteredEventError
	}
	labels := t.Labels(attr, lo.aliases)
	t.increment(labels, n)
	// log.Println(t.counters.Batch())
	// lo.queue <- Event{t, n, labels}
	return nil
}

func (lo *Logger) Persist(tm time.Time) (err error) {
	// Use a transaction to ensure each event type is persisted entirely
	lo.Registry.Each(func(name string, t *EventType) {
		if err == nil {
			err = t.Persist(tm, lo.Redis)
		}
	})
	return
}

type Logger struct {
	*Registry
	Redis   *redis.Client
	aliases Aliases
	// queue   chan Event
	// tick *time.Ticker
	wg   sync.WaitGroup
	done chan struct{}
}

type Options struct {
	Redis redis.Options
	// QueueSize     int           // Size of event queue buffer
	// NumWorkers    int           // Number of event queue workers
	FlushInterval time.Duration // Interval to flush counters
}

func NewLogger(r *Registry, aliases Aliases, options Options) *Logger {
	if r == nil {
		r = defaultRegistry
	}
	lo := &Logger{
		Registry: r,
		aliases:  aliases,
		Redis:    redis.NewClient(&options.Redis),
		// queue:    make(chan Event, options.QueueSize),
		done: make(chan struct{}),
	}
	// if options.NumWorkers < 1 {
	// 	options.NumWorkers = 1
	// }
	// lo.wg.Add(options.NumWorkers)
	// for i := 0; i < options.NumWorkers; i++ {
	// 	go lo.worker()
	// }
	if options.FlushInterval > 0 {
		go func() {
			lo.wg.Add(1)
			defer lo.wg.Done()
			tick := time.NewTicker(options.FlushInterval)
			defer tick.Stop()
			for {
				select {
				case <-lo.done:
					return
				case tm := <-tick.C:
					lo.Persist(tm)
				}
			}
		}()
	}
	return lo
}

// func (lo *Logger) worker() {
// 	lo.wg.Add(1)
// 	defer lo.wg.Done()
// 	defer func() {
// 		lo.Persist(time.Now())
// 	}()
// 	for {
// 		select {
// 		case <-lo.done:
// 			return
// 		case e := <-lo.queue:
// 			e.Type.Increment(e.Labels.Map(), e.Value)
// 		}
// 	}
//
// }
func (lo *Logger) Close() {
	close(lo.done)
	lo.wg.Wait()
	lo.Persist(time.Now())
	// for {
	// 	select {
	// 	default:
	// 		return
	// 	case e := <-lo.queue:
	// 		e.Type.Increment(e.Labels.Map(), e.Value)
	// 	}
	// }

}
