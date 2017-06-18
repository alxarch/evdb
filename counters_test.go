package meter_test

import (
	"sync"
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_CounterParallel(t *testing.T) {
	var wg sync.WaitGroup
	c := meter.Counter{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(d int64) {
			defer wg.Done()
			c.Inc(d)
		}(int64(i))

	}
	wg.Wait()
	if n := c.Get(); n != 45 {
		t.Errorf("Invalid state %d", n)
	}
}
func Test_Counter(t *testing.T) {
	c := meter.Counter{}
	if n := c.Get(); n != 0 {
		t.Errorf("Invalid initial value %d", n)
	}
	if n := c.Inc(2); n != 2 {
		t.Errorf("Invalid inc value %d", n)
	}
	if n := c.Inc(-3); n != -1 {
		t.Errorf("Invalid inc value %d", n)
	}
	if n := c.Set(4); n != -1 {
		t.Errorf("Invalid set value %d", n)
	}
}

func Test_Counters(t *testing.T) {
	cc := meter.NewCounters()
	if n := cc.Increment("foo", 1); n != 1 {
		t.Errorf("Invalid counters increment %d", n)
	}

	cc.BatchIncrement(map[string]int64{
		"foo": 2,
		"bar": 3,
	})
	b := cc.Batch()
	if foo := b["foo"]; foo != 3 {
		t.Errorf("Invalid counters increment %d", foo)

	}
	if bar := b["bar"]; bar != 3 {
		t.Errorf("Invalid counters increment %d", bar)

	}

}
