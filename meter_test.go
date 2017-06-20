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
	labels := m.NormalizeLabels("foo")
	// f := m.NewFilter(m.ResolutionDaily, m.Daily, []string{"foo"})
	g := m.NewEvent("goo", labels...)
	r.Register("goo", g)
	// now := time.Now()
	a := m.NewAliases()
	a.Set("FOO", "foo")
	rc := redis.NewClient(&redis.Options{Addr: ":6379"})
	lo := r.Logger()
	lo.Log("goo", 1.0, "foo", "bar")
	lo.Log("goo", 6.0, "FOO", "baz")
	lo.Log("goo", 1.0)
	lo.Close()
	// b := r.Get("goo").Flush(time.Now())
	// bb := r.Get("goo").Batch(now)
	// log.Println(b)
	log.Println(g.Persist(time.Now(), rc))

}

func Test_DefaultRegistry(t *testing.T) {
	e := m.NewEvent("foo", "bar")
	m.Register("foo", e)
}
