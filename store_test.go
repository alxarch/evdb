package meter_test

import (
	"context"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter/v2"
)

func Test_MemoryStore(t *testing.T) {
	m := new(meter.MemoryStore)
	m.Event = "test"
	r1 := meter.Snapshot{
		Time:   time.Now(),
		Labels: []string{"color", "taste"},
		Counters: []meter.Counter{
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
		TimeRange: meter.TimeRange{
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now().Add(time.Hour),
		},
		Match: meter.Fields{
			{Label: "color", Value: "blue"},
		},
	}
	results, err := m.Scan(ctx, q1.TimeRange, q1.Match)
	if err != nil {
		t.Fatalf(`Unexpected error %s`, err)
	}
	if len(results) == 0 {
		t.Errorf("Invalid number of items %d", len(results))
	}

}
