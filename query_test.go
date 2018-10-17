package meter

import (
	"net/url"
	"reflect"
	"testing"
	"time"
)

func Test_Query(t *testing.T) {

	b := QueryBuilder{}
	b = b.From("test")
	if len(b.Events) != 1 || b.Events[0] != "test" {
		t.Errorf("Invalid b.Events: %v", b.Events)

	}
	b = b.At(ResolutionDaily)
	if b.Resolution != "daily" {
		t.Errorf("Invalid resolution: %s", b.Resolution)
	}
	now := time.Now()
	b = b.Between(now, now)
	if !now.Equal(b.End) {
		t.Errorf("Invalid b.End: %s", b.End)
	}
	b = b.GroupBy("bar")
	if !reflect.DeepEqual([]string{"bar"}, b.Group) {
		t.Errorf("Invalid group: %v", b.Group)
	}
	b = b.Where("foo", "bar", "baz")
	if !reflect.DeepEqual(b.Query, url.Values{"foo": []string{"bar", "baz"}}) {
		t.Errorf("Invalid query after where: %v", b.Query)
	}

	r := NewRegistry()
	desc := NewCounterDesc("test", []string{"foo", "bar"}, ResolutionDaily)
	e := NewEvent(desc)
	r.Register(e)
	qs := b.Queries(r)
	if !reflect.DeepEqual([]Query{
		Query{
			Event:      e,
			Start:      now,
			End:        now,
			Group:      []string{"bar"},
			Resolution: ResolutionDaily,
			Values: []map[string]string{
				map[string]string{"foo": "bar"},
				map[string]string{"foo": "baz"},
			},
		},
	}, qs) {
		t.Errorf("Invalid queries: %v", qs)
	}
	perm := QueryPermutations(url.Values{"foo": []string{"bar", "baz"}, "answer": []string{"42"}})
	if !reflect.DeepEqual(perm, []map[string]string{
		map[string]string{"answer": "42", "foo": "bar"},
		map[string]string{"answer": "42", "foo": "baz"},
	}) {
		t.Errorf("Invalid perm: %v", perm)
	}
	perm = b.QueryValues(desc)
	if !reflect.DeepEqual(perm, []map[string]string{
		map[string]string{"foo": "bar"},
		map[string]string{"foo": "baz"},
	}) {
		t.Errorf("Invalid perm: %v", perm)
	}

}
