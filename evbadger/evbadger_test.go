package evbadger_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/evbadger"
	"github.com/alxarch/evdb/events"
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
	edb, err := evbadger.Open(db)
	if err != nil {
		t.Fatal("Failed to open badger store", err)
	}
	tm := time.Date(2019, time.May, 15, 13, 14, 0, 0, time.UTC)
	req := evdb.Snapshot{
		Time: tm,
		Labels: []string{
			"country",
			"host",
			"method",
		},
		Counters: []events.Counter{
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
	st, err := edb.Storer("test")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Store(&req); err != nil {
		t.Fatal("Failed to store counters", err)
	}
	ctx := context.Background()
	q := evdb.ScanQuery{
		Event: "test",
		TimeRange: evdb.TimeRange{
			Step:  time.Second,
			Start: tm.Add(-1 * time.Hour),
			End:   tm.Add(24 * time.Hour),
		},
		Fields: evdb.MatchFields{
			"country": evdb.MatchString("USA"),
		},
	}
	results, err := edb.ScanQuery(ctx, &q)
	if err != nil {
		t.Fatal("Query failed", err)
	}
	if len(results) != 3 {
		t.Fatal("numResults", len(results))
	}

}
