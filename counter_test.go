package meter_test

import (
	"testing"

	"github.com/alxarch/go-meter/v2"
)

func Test_UnsafeCounters(t *testing.T) {
	var cc meter.UnsafeCounters
	AssertEqual(t, cc.Add(1, "foo", "bar"), int64(1))
	AssertEqual(t, cc.Add(41, "foo", "bar"), int64(42))
	AssertEqual(t, cc.Len(), 1)
	snapshot := cc.Flush(nil)
	AssertEqual(t, len(snapshot), 1)
	AssertEqual(t, snapshot[0].Count, int64(42))
	AssertEqual(t, cc.Get(0).Count, int64(0))

}
