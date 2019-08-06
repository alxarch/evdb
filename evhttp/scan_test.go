package evhttp_test

import (
	"context"
	"testing"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/evhttp"
	"github.com/alxarch/evdb/evutil"
	"github.com/alxarch/evdb/internal/assert"
)

func TestScan(t *testing.T) {
	s := evutil.NewMemoryStore("foo", "bar")
	snap := &evdb.Snapshot{
		Labels: []string{"color", "taste"},
		Counters: []events.Counter{
			{Count: 112, Values: []string{"blue", "bitter"}},
			{Count: 34, Values: []string{"red", "sweet"}},
		},
	}
	fooStore, _ := s.Storer("foo")
	if err := fooStore.Store(snap); err != nil {
		t.Fatal(err)
	}
	h := evhttp.ScanQueryHandler(s)
	scan := evhttp.ScanQuerier{
		HTTPClient: &mockHTTPClient{h},
		URL:        "http://example.com/scan",
	}
	ctx := context.Background()
	match := evdb.MatchFields{
		"color": evdb.MatchString("blue"),
	}
	now := time.Now().Truncate(time.Second)
	tr := evdb.TimeRange{
		Start: now.Add(-time.Hour),
		End:   now.Add(time.Hour),
		Step:  time.Hour,
	}
	q := evdb.ScanQuery{
		TimeRange: tr,
		Event:     "foo",
		Fields:    match,
	}
	{
		results, err := s.Scan(ctx, q)
		assert.NoError(t, err)
		assert.Equal(t, results, evdb.Results{
			{
				Event:     "foo",
				TimeRange: tr,
				Fields: evdb.Fields{
					{Label: "color", Value: "blue"},
					{Label: "taste", Value: "bitter"},
				},
				Data: evdb.DataPoints{
					{Timestamp: now.Truncate(time.Hour).Unix(), Value: 112},
				},
			},
		})

	}
	{

		results, err := scan.ScanQuery(ctx, &q)
		assert.NoError(t, err)
		assert.Equal(t, results, evdb.Results{
			{
				Event:     "foo",
				TimeRange: tr,
				Fields: evdb.Fields{
					{Label: "color", Value: "blue"},
					{Label: "taste", Value: "bitter"},
				},
				Data: evdb.DataPoints{
					{Timestamp: now.Truncate(time.Hour).Unix(), Value: 112},
				},
			},
		})
	}
}
