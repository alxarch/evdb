package meter

import (
	"context"
	"time"

	batch "github.com/alxarch/go-batch"
	"github.com/go-redis/redis"
)

const (
	// DefaultBatchSize for log events
	DefaultBatchSize = 1000
	// DefaultFlushInterval for log events
	DefaultFlushInterval = 5 * time.Second
)

type Sink struct {
	*batch.Queue
	Registry *Registry
	Redis    redis.UniversalClient
}

func NewSink(r redis.UniversalClient, size int, interval time.Duration) *Sink {
	s := &Sink{
		Registry: DefaultRegistry,
		Redis:    r,
	}
	s.Queue = &batch.Queue{
		Size:     size,
		Interval: interval,
		Drainer:  s,
	}
	return s
}

func (s *Sink) batchCounters(batch []interface{}) (int64, TTLCounters) {
	counters := TTLCounters{}
	n := int64(0)
	pool := defaultPool
	if s.Registry != nil {
		pool = s.Registry.Pool
	}
	for _, x := range batch {
		if e, ok := x.(*Event); ok && e != nil {
			n += e.Increment(counters)
			// Attributes are not longer needed
			pool.Put(e.Attributes)
		}
	}

	return n, counters
}

// Log queues an event object to the log queue
func (s *Sink) MustLog(e *Event) {
	if err := s.Log(e); err != nil {
		panic(err)
	}
}

func (s *Sink) Log(e *Event) error {
	return s.Queue.Add(e)
}

func (s *Sink) Drain(ctx context.Context, batch []interface{}) error {
	_, counters := s.batchCounters(batch)
	if _, err := counters.Persist(s.Redis); err != nil {
		// log.Printf("Failed to drain stats: %s", err)
		return err
	}
	return nil
}
