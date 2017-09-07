package meter_test

import (
	"sort"
	"testing"
	"time"

	redis "github.com/go-redis/redis"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

func Test_Results(t *testing.T) {
	now := time.Now()
	labels := meter.Labels{"bar": "baz"}
	labels2 := meter.Labels{"bar": "bax"}
	records := meter.RecordSequence([]*meter.Record{
		{
			Name:   "foo",
			Field:  "bar:baz",
			Time:   now,
			Labels: labels,
			Result: redis.NewStringResult("10", nil),
		},
		{
			Name:   "foo",
			Field:  "bar:baz",
			Labels: labels,
			Time:   now.Add(time.Hour),
			Result: redis.NewStringResult("42", nil),
		},
		{
			Name:   "foo",
			Field:  "bar:bax",
			Time:   now,
			Labels: labels2,
			Result: redis.NewStringResult("10", nil),
		},
		{
			Name:   "foo",
			Field:  "bar:bax",
			Labels: labels2,
			Time:   now.Add(time.Hour),
			Result: redis.NewStringResult("42", nil),
		},
	})
	expect := []*meter.Result{
		{
			Event:  "foo",
			Labels: map[string]string{"bar": "baz"},
			Data: []meter.DataPoint{
				{
					Timestamp: now.Unix(),
					Value:     10,
				},
				{
					Timestamp: now.Add(time.Hour).Unix(),
					Value:     42,
				},
			},
		},
		{
			Event:  "foo",
			Labels: map[string]string{"bar": "bax"},
			Data: []meter.DataPoint{
				{
					Timestamp: now.Unix(),
					Value:     10,
				},
				{
					Timestamp: now.Add(time.Hour).Unix(),
					Value:     42,
				},
			},
		},
	}
	actual := records.Results()

	sort.Slice(actual, func(i int, j int) bool {
		return actual[i].Labels["bar"] > actual[j].Labels["bar"]

	})
	assert.Equal(t, expect, actual)
}
