package mdbredis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/alxarch/go-meter/v2/mdbredis"
)

func TestDB(t *testing.T) {
	now := time.Now().In(time.UTC)
	options := mdbredis.Options{
		Redis:       "",
		KeyPrefix:   fmt.Sprintf("mdbredis:test:%d", now.UnixNano()),
		ScanSize:    1000,
		Resolutions: []mdbredis.Resolution{mdbredis.ResolutionHourly},
	}
	db, err := mdbredis.Open(options, "cost")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
