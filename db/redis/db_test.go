package redisdb_test

import (
	"context"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/alxarch/go-meter/v2/db/redis"
)

func TestDB(t *testing.T) {
	db, err := redisdb.Open("redis://127.0.0.1:6379/10", redisdb.ResolutionHourly)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Redis.FlushDB()
	defer db.Close()
	now := time.Now().In(time.UTC)
	db.Store(meter.Snapshot{
		Event:  "cost",
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
		},
		Group: []string{"foo"},
	}
	results, err := db.Query(ctx, &q, "cost")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Error("Invalid results")
	}
}
