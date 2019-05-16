package meter_test

import (
	"context"
	"testing"
	"time"

	"github.com/alxarch/go-meter"
)

func Test_MemoryStore(t *testing.T) {
	m := new(meter.MemoryStore)
	m.Event = "test"
	r1 := meter.StoreRequest{
		Time:   time.Now(),
		Event:  "test",
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
	it := m.Scan(ctx, &q1)
	var n int64
	for it.Next() {
		item := it.Item()
		n += item.Count
	}
	if n != 13 {
		t.Errorf("Invalid number of items %d", n)
	}

}
