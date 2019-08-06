package evredis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/evredis"
)

func TestDB(t *testing.T) {
	now := time.Now().In(time.UTC)
	options := evredis.Config{
		Redis:       "",
		KeyPrefix:   fmt.Sprintf("evredis:test:%d", now.UnixNano()),
		ScanSize:    1000,
		Resolutions: []evredis.Resolution{evredis.ResolutionHourly},
	}
	db, err := evredis.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, _ := db.Storer("cost")
	s.Store(&evdb.Snapshot{
		Time:   now,
		Labels: []string{"foo", "bar"},
		Counters: []events.Counter{
			{Count: 3, Values: []string{"bax", "baz"}},
			{Count: 2, Values: []string{"fax", "faz"}},
		},
	})
	ctx := context.Background()
	q := evdb.ScanQuery{
		Event: "cost",
		TimeRange: evdb.TimeRange{
			Start: now,
			End:   now,
			Step:  time.Hour,
		},
	}
	results, err := db.ScanQuery(ctx, &q)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Error("Invalid results")
	}
}
