package events_test

import (
	"reflect"
	"strconv"
	"testing"

	meter "github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/internal/assert"
)

func Test_UnsafeCounters(t *testing.T) {
	var cc meter.UnsafeCounters
	assert.Equal(t, cc.Add(1, "foo", "bar"), int64(1))
	assert.Equal(t, cc.Add(41, "foo", "bar"), int64(42))
	assert.Equal(t, cc.Len(), 1)
	snapshot := cc.Flush(nil)
	assert.Equal(t, len(snapshot), 1)
	assert.Equal(t, snapshot[0].Count, int64(42))
	assert.Equal(t, cc.Get(0).Count, int64(0))
	assert.Equal(t, cc.Add(1, "foo", "bar"), int64(1))
	assert.Equal(t, cc.Add(41, "foo", "bar"), int64(42))
	assert.Equal(t, cc.Add(0, "bar", "baz"), int64(0))
	cc.Pack()
	packed := cc.Flush(nil)
	assert.Equal(t, len(packed), 1)
	assert.Equal(t, packed[0].Count, int64(42))
}

func TestCounterSlice_FilterZero(t *testing.T) {
	tests := []struct {
		counters meter.CounterSlice
		want     meter.CounterSlice
	}{
		{meter.CounterSlice{{Count: 0}}, meter.CounterSlice{}},
		{meter.CounterSlice{{Count: 8}}, meter.CounterSlice{{Count: 8}}},
		{meter.CounterSlice{{Count: 8}, {Count: 0}}, meter.CounterSlice{{Count: 8}}},
		{meter.CounterSlice{{Count: 0}, {Count: 8}}, meter.CounterSlice{{Count: 8}}},
		{meter.CounterSlice{{Count: 0}, {Count: 8}, {Count: 0}}, meter.CounterSlice{{Count: 8}}},
	}
	for i, tt := range tests {
		name := strconv.Itoa(i)
		t.Run(name, func(t *testing.T) {
			got := tt.counters.FilterZero()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CounterSlice.FilterZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
