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

func TestQuery(t *testing.T) {
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
	h := evhttp.QueryHandler(s)
	exec := evhttp.Execer{
		HTTPClient: &mockHTTPClient{h},
		URL:        "http://example.com/scan",
	}
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	tr := evdb.TimeRange{
		Start: now.Add(-time.Hour),
		End:   now,
		Step:  time.Hour,
	}
	{

		results, err := exec.Exec(ctx, tr, `foo{color:blue}`)
		assert.NoError(t, err)
		assert.Equal(t, results, []evdb.Results{{
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
		}})
	}
}
