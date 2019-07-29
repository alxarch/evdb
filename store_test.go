package evdb_test

import (
	"context"
	"testing"
	"time"

	meter "github.com/alxarch/evdb"
	"github.com/alxarch/evdb/events"
)

func Test_MemoryStore(t *testing.T) {
	m := new(meter.MemoryStorer)
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
	q1 := meter.ScanQuery{
		Event: "foo",
		TimeRange: meter.TimeRange{
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now().Add(time.Hour),
		},
		Fields: meter.MatchFields{
			"color": meter.MatchString("blue"),
		},
	}

	s := meter.MemoryStore{"foo": m}
	results, err := s.Scan(ctx, q1)
	if err != nil {
		t.Fatalf(`Unexpected error %s`, err)
	}
	if len(results) == 0 {
		t.Errorf("Invalid number of items %d", len(results))
	}

}
