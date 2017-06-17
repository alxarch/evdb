package meter_test

import (
	"log"
	"testing"
	"time"

	m "github.com/alxarch/go-meter"
	"github.com/go-redis/redis"
)

func Test_Registry(t *testing.T) {
	r := m.NewRegistry()
	f := m.NewFilter(m.ResolutionDaily, m.Daily, []string{"foo"})
	g := m.NewEventType("goo", nil, nil, f)
	r.Register("goo", g)
	// now := time.Now()
	a := m.NewAliases()
	a.Set("FOO", "foo")
	lo := m.NewLogger(r, a, m.Options{
		Redis: redis.Options{Addr: ":6379"},
		// QueueSize:  8,
		// NumWorkers: 4,
	})
	lo.Log("goo", 1.0, "foo", "bar")
	lo.Log("goo", 6.0, "FOO", "baz")
	lo.Log("goo", 1.0)
	lo.Close()
	// b := r.Get("goo").Flush(time.Now())
	// bb := r.Get("goo").Batch(now)
	// log.Println(b)
	log.Println(g.Persist(time.Now(), lo.Redis))

}
