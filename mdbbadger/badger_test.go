package mdbbadger_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	"github.com/alxarch/go-meter/v2/mdbbadger"
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
	events, err := mdbbadger.Open(db, "test")
	if err != nil {
		t.Fatal("Failed to open badger store", err)
	}
	tm := time.Date(2019, time.May, 15, 13, 14, 0, 0, time.UTC)
	req := meter.Snapshot{
		Time: tm,
		Labels: []string{
			"country",
			"host",
			"method",
		},
		Counters: []meter.Counter{
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
	if err := events.Storer("test").Store(&req); err != nil {
		t.Fatal("Failed to store counters", err)
	}
	ctx := context.Background()
	q := meter.ScanQuery{
		Event: "test",
		TimeRange: meter.TimeRange{
			Step:  time.Second,
			Start: tm.Add(-1 * time.Hour),
			End:   tm.Add(24 * time.Hour),
		},
		Match: meter.Fields{
			{Label: "country", Value: "USA"},
		},
	}
	results, err := events.ScanQuery(ctx, &q)
	if err != nil {
		t.Fatal("Query failed", err)
	}
	if len(results) != 3 {
		t.Fatal("numResults", len(results))
	}

}
