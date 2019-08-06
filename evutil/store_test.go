package evutil_test

import (
	"context"
	"testing"
	"time"

	meter "github.com/alxarch/evdb"
	"github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/evutil"
	"github.com/alxarch/evdb/internal/assert"
)

func Test_MemoryStore(t *testing.T) {
	m := new(evutil.MemoryStorer)
	r1 := meter.Snapshot{
		Time:   time.Now(),
		Labels: []string{"color", "taste"},
		Counters: []events.Counter{
			{
				Values: []string{"blue", "sour"},
				Count:  3,
			},
			{
				Values: []string{"blue", "bitter"},
				Count:  10,
			},
			{
				Values: []string{"green", "bitter"},
				Count:  12,
			},
		},
	}
	if err := m.Store(&r1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	q1 := meter.Query{
		Event: "foo",
		TimeRange: meter.TimeRange{
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now().Add(time.Hour),
		},
		Fields: meter.MatchFields{
			"color": meter.MatchString("blue"),
		},
	}

	s := evutil.MemoryStore{"foo": m}
	results, err := s.Scan(ctx, q1)
	if err != nil {
		t.Fatalf(`Unexpected error %s`, err)
	}
	if len(results) == 0 {
		t.Errorf("Invalid number of items %d", len(results))
	}

	e := events.New("foo", "bar", "baz")
	now := time.Now()
	assert.NoError(t, meter.FlushAt(e, m, now))
	n := e.Add(42, "baz", "goo")
	assert.Equal(t, n, int64(42))
	assert.NoError(t, meter.FlushAt(e, m, time.Now()))
	snap := m.Last()
	assert.OK(t, snap != nil, "Non nil snap")
	assert.Equal(t, len(snap.Counters), 1)
	assert.Equal(t, snap.Counters[0], events.Counter{Count: 42, Values: []string{"baz", "goo"}})
	// assert.Equal(t, snap.Counters, events.CounterSlice{
	// 	{Count: 42, Values: []string{"baz", "goo"}},
	// })
	sto, _ := evutil.NewMemoryStore("foo").Storer("foo")
	assert.OK(t, m != nil, "Non nil memstore")
	assert.NoError(t, sto.Store(&r1))
	s1 := new(evutil.MemoryStorer)
	s2 := new(evutil.MemoryStorer)
	tee := evutil.TeeStore(s1, s2)
	assert.NoError(t, tee.Store(&r1))
	assert.Equal(t, s1.Len(), 1)
	assert.Equal(t, s2.Len(), 1)

}
