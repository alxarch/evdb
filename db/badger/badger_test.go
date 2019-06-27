package badgerdb_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	bdb "github.com/alxarch/go-meter/v2/db/badger"
	"github.com/dgraph-io/badger/v2"
)

func TestBadgerEvents(t *testing.T) {

	d := path.Join(os.TempDir(), fmt.Sprintf("meter-test-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(d, os.ModeDir|os.ModePerm); err != nil {
		t.Fatal(err)
	}
	opts := badger.DefaultOptions
	opts.Dir = d
	opts.ValueDir = d

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal("Failed to open badger", err)
	}
	events, err := bdb.Open(db, "test")
	if err != nil {
		t.Fatal("Failed to open badger store", err)
	}
	tm := time.Date(2019, time.May, 15, 13, 14, 0, 0, time.UTC)
	req := meter.StoreRequest{
		Event: "test",
		Time:  tm,
		Labels: []string{
			"country",
			"host",
			"method",
		},
		Counters: meter.Snapshot{
			{
				Values: []string{"USA", "www.example.org", "GET"},
				Count:  12,
			},
			{
				Values: []string{"USA", "www.example.org", "POST"},
				Count:  10,
			},
			{
				Values: []string{"GRC", "www.example.org", "GET"},
				Count:  4,
			},
			{
				Values: []string{"USA", "example.org", "GET"},
				Count:  8,
			},
		},
	}
	if err := events.Store(&req); err != nil {
		t.Fatal("Failed to store counters", err)
	}
	qr := meter.ScanQueryRunner(events)
	ctx := context.Background()
	q := meter.Query{
		TimeRange: meter.TimeRange{
			Step:  time.Second,
			Start: tm.Add(-1 * time.Hour),
			End:   tm.Add(24 * time.Hour),
		},
		Match: meter.Fields{
			{Label: "country", Value: "USA"},
		},
	}
	results, err := qr.RunQuery(ctx, &q, "test")
	if err != nil {
		t.Fatal("Query failed", err)
	}
	if len(results) != 3 {
		t.Fatal("numResults", len(results))
	}

}
