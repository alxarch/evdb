package meter

import (
	"context"
	"testing"
	"time"
)

func Test_Parser_Eval(t *testing.T) {
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
		scanner  = MemoryScanner{
			"foo": fooStore,
			"bar": barStore,
		}
		evaler = NewEvaler(&scanner)
		ctx    = context.Background()
		q      = EvalQuery{
			TimeRange: TimeRange{
				Start: now.Add(-1 * time.Hour),
				End:   now,
				Step:  time.Minute,
			},
			Query: `foo{color: red}`,
		}
	)
	for i := range snapshots {
		s := &snapshots[i]
		if err := store.Store(s); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	}
	{
		q := q
		results, err := evaler.Eval(ctx, q)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(results) != 4 {
			t.Errorf("Invalid results size %d != 4", len(results))
		}
		for _, r := range results {
			t.Error(r)
		}

	}
	// {
	// 	results, err := evaler.Eval(ctx, q, "foo / bar")
	// 	if err != nil {
	// 		t.Fatalf("Unexpected error: %s", err)
	// 	}
	// 	if len(results) != 1 {
	// 		t.Error(results)
	// 	}
	// 	if len(results[0].Data) != 2 {
	// 		t.Errorf("Invalid results size %d != 2", len(results))
	// 	}

	// }

}

func TestParser_Reset(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{`foo{}`, false},
		{`foo{bar: baz}`, false},
		{`foo{bar: baz|42}`, false},
		{`foo{bar: baz|42}[-10:h]`, false},
		{`{
			*group{foo, bar, agg: avg}
			*match{foo: bar}
			!avg{
				foo{bar: baz|42}[1:h],
				foo{bar: baz|42},
			} + 2

		}

			
		`, false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := new(Parser)
			if err := p.Reset(tt.name, TimeRange{}); (err != nil) != tt.wantErr {
				t.Errorf("Parser.Reset() error = %#v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
