package meter

import (
	"context"
	"testing"
	"time"
)

func Test_Eval(t *testing.T) {
	e, err := ParseEval("a + (b + c)")
	if err != nil {
		t.Fatal(err)
	}
	events := e.Events()
	if len(events) != 3 {
		t.Errorf("Invalid events %v", events)
		return

	}
	if events[0] != "a" {
		t.Errorf("Invalid events %v", events)
	}
	if events[1] != "b" {
		t.Errorf("Invalid events %v", events)
	}
	if events[2] != "c" {
		t.Errorf("Invalid events %v", events)
	}

}
func Test_EvalQuery(t *testing.T) {
	var (
		now       = time.Now()
		labels    = []string{"color", "taste"}
		snapshots = []Snapshot{
			{
				Time:   now.Add(-1 * time.Minute),
				Labels: labels,
				Counters: CounterSlice{
					{
						Values: []string{"red", "bitter"},
						Count:  42,
					},
					{
						Values: []string{"yellow", "bitter"},
						Count:  8,
					},
					{
						Values: []string{"red", "sweet"},
						Count:  64,
					},
				},
			},
			{
				Time:   now.Add(-1 * time.Second),
				Labels: labels,
				Counters: CounterSlice{
					{
						Values: []string{"red", "bitter"},
						Count:  42,
					},
					{
						Values: []string{"yellow", "bitter"},
						Count:  8,
					},
					{
						Values: []string{"yellow", "sour"},
						Count:  9,
					},
				},
			},
			{
				Time:   now,
				Labels: labels,
				Counters: CounterSlice{
					{
						Values: []string{"red", "bitter"},
						Count:  24,
					},
					{
						Values: []string{"yellow", "bitter"},
						Count:  11,
					},
					{
						Values: []string{"yellow", "sour"},
						Count:  100,
					},
					{
						Values: []string{"green", "sweet"},
						Count:  2,
					},
				},
			},
		}
		fooStore = new(MemoryStore)
		barStore = new(MemoryStore)
		store    = TeeStore(fooStore, barStore)
		scanners = ScannerIndex{
			"foo": fooStore,
			"bar": barStore,
		}
		querier = ScanQuerier(scanners)
		evaler  = QueryEvaler(querier)
		ctx     = context.Background()
		q       = Query{
			TimeRange: TimeRange{
				Start: now.Add(-1 * time.Hour),
				End:   now,
				Step:  time.Minute,
			},
			Match: Fields{
				{
					Label: "color",
					Value: "red",
				},
			},
		}
	)
	for i := range snapshots {
		s := &snapshots[i]
		if err := store.Store(s); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	}
	{
		results, err := querier.Query(ctx, q, "foo", "bar")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(results) != 4 {
			t.Errorf("Invalid results size %d != 4", len(results))
		}
		if len(results[0].Data) != 2 {
			t.Error(results[0].Data)
		}

	}
	{
		results, err := evaler.Eval(ctx, q, "foo / bar")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(results) != 1 {
			t.Error(results)
		}
		if len(results[0].Data) != 2 {
			t.Errorf("Invalid results size %d != 2", len(results))
		}

	}

}
