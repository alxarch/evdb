package meter_test

import (
	"net/url"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

func Test_Query(t *testing.T) {

	b := meter.QueryBuilder{}
	b = b.From("test")
	assert.Equal(t, b.Events, []string{"test"})
	b = b.At(meter.ResolutionDaily)
	assert.Equal(t, "daily", b.Resolution)
	now := time.Now()
	b = b.Between(now, now)
	assert.Equal(t, now, b.Start)
	assert.Equal(t, now, b.End)
	b = b.GroupBy("bar")
	assert.Equal(t, []string{"bar"}, b.Group)
	b = b.Where("foo", "bar", "baz")
	assert.Equal(t, url.Values{"foo": []string{"bar", "baz"}},
		b.Query)

	r := meter.NewRegistry()
	desc := meter.NewCounterDesc("test", []string{"foo", "bar"}, meter.ResolutionDaily)
	e := meter.NewEvent(desc)
	r.Register(e)
	qs := b.Queries(r)
	assert.Equal(t, []meter.Query{
		meter.Query{
			Event:      e,
			Start:      now,
			End:        now,
			Group:      []string{"bar"},
			Resolution: meter.ResolutionDaily,
			Values: []map[string]string{
				map[string]string{"foo": "bar"},
				map[string]string{"foo": "baz"},
			},
		},
	}, qs)
	perm := meter.QueryPermutations(url.Values{"foo": []string{"bar", "baz"}, "answer": []string{"42"}})
	assert.Equal(t, []map[string]string{
		map[string]string{"answer": "42", "foo": "bar"},
		map[string]string{"answer": "42", "foo": "baz"},
	}, perm)
	perm = b.QueryValues(desc)
	assert.Equal(t, []map[string]string{
		map[string]string{"foo": "bar"},
		map[string]string{"foo": "baz"},
	}, perm)

}
