package mdbredis_test

import (
	"context"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/alxarch/go-meter/v2/mdbredis"
	"github.com/go-redis/redis"
)

func TestDB(t *testing.T) {
	opts, _ := redis.ParseURL("redis://127.0.0.1:6379/10")
	rc := redis.NewClient(opts)
	db, err := mdbredis.Open(rc, 1000, "meter", mdbredis.ResolutionHourly)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.FlushDB()
	defer db.Close()
	db.AddEvent("cost")
	now := time.Now().In(time.UTC)
	db.Storer("cost").Store(&meter.Snapshot{
		Time:   now,
		Labels: []string{"foo", "bar"},
		Counters: []meter.Counter{
			{Count: 3, Values: []string{"bax", "baz"}},
			{Count: 2, Values: []string{"fax", "faz"}},
		},
	})
	ctx := context.Background()
	q := meter.Query{
		TimeRange: meter.TimeRange{
			Start: now,
			End:   now,
			Step:  time.Hour,
		},
		Group: []string{"foo"},
	}
	results, err := db.Query(ctx, q, "cost")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Error("Invalid results")
	}
}
