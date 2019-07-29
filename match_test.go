package evdb_test

import (
	"testing"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/internal/assert"
)

func Test_MatchString(t *testing.T) {
	m := evdb.MatchString("foo")
	assert.OK(t, m.MatchString("foo"), "MatchString matches")
	assert.Equal(t, m.String(), "foo")
	assert.OK(t, m.Match([]byte("foo")), "MatchString matches bytes")
	assert.OK(t, !m.MatchString("bar"), "MatchString doesn't match")
	assert.OK(t, !m.Match([]byte("bar")), "MatchString doesn't match bytes")

}

func Test_MatchPrefix(t *testing.T) {
	m := evdb.MatchPrefix("foo")
	assert.OK(t, m.MatchString("foo"), "MatchPrefix matches")
	assert.OK(t, m.MatchString("foobar"), "MatchPrefix matches")
	assert.OK(t, !m.MatchString(""), "MatchPrefix matches")
	assert.OK(t, !m.MatchString("bar"), "MatchPrefix matches")
	assert.OK(t, !m.MatchString("barfoo"), "MatchPrefix matches")
	assert.OK(t, !m.MatchString("fo"), "MatchPrefix matches")
	assert.OK(t, m.Match([]byte("foo")), "MatchPrefix matches")
	assert.OK(t, m.Match([]byte("foobar")), "MatchPrefix matches")
	assert.OK(t, !m.Match([]byte("")), "MatchPrefix matches")
	assert.OK(t, !m.Match([]byte("bar")), "MatchPrefix matches")
	assert.OK(t, !m.Match([]byte("barfoo")), "MatchPrefix matches")
	assert.OK(t, !m.Match([]byte("fo")), "MatchPrefix matches")

}

func Test_MatchSuffix(t *testing.T) {
	m := evdb.MatchSuffix("foo")
	assert.OK(t, m.MatchString("foo"), "MatchSuffix matches")
	assert.OK(t, !m.MatchString("foobar"), "MatchSuffix matches")
	assert.OK(t, !m.MatchString(""), "MatchSuffix matches")
	assert.OK(t, !m.MatchString("bar"), "MatchSuffix matches")
	assert.OK(t, m.MatchString("barfoo"), "MatchSuffix matches")
	assert.OK(t, !m.MatchString("fo"), "MatchSuffix matches")
	assert.OK(t, m.Match([]byte("foo")), "MatchSuffix matches")
	assert.OK(t, !m.Match([]byte("foobar")), "MatchSuffix matches")
	assert.OK(t, !m.Match([]byte("")), "MatchSuffix matches")
	assert.OK(t, !m.Match([]byte("bar")), "MatchSuffix matches")
	assert.OK(t, m.Match([]byte("barfoo")), "MatchSuffix matches")
	assert.OK(t, !m.Match([]byte("fo")), "MatchSuffix matches")

}

func Test_MatchAny(t *testing.T) {
	m := evdb.MatchAny("foo", "bar")
	assert.OK(t, m.MatchString("foo"), "MatchAny matches")
	assert.OK(t, !m.MatchString("foobar"), "MatchAny matches")
	assert.OK(t, !m.MatchString(""), "MatchAny matches")
	assert.OK(t, m.MatchString("bar"), "MatchAny matches")
	assert.OK(t, !m.MatchString("barfoo"), "MatchAny matches")
	assert.OK(t, !m.MatchString("fo"), "MatchAny matches")
	assert.OK(t, m.Match([]byte("foo")), "MatchAny matches")
	assert.OK(t, !m.Match([]byte("foobar")), "MatchAny matches")
	assert.OK(t, !m.Match([]byte("")), "MatchAny matches")
	assert.OK(t, m.Match([]byte("bar")), "MatchAny matches")
	assert.OK(t, !m.Match([]byte("barfoo")), "MatchAny matches")
	assert.OK(t, !m.Match([]byte("fo")), "MatchAny matches")
}

func Test_Matchers(t *testing.T) {
	m := evdb.Matchers{
		evdb.MatchAny("foo", "bar"),
		evdb.MatchPrefix("baz"),
	}
	assert.OK(t, m.MatchString("foo"), "Matchers matches")
	assert.OK(t, !m.MatchString("foobar"), "Matchers matches")
	assert.OK(t, !m.MatchString(""), "Matchers matches")
	assert.OK(t, m.MatchString("bar"), "Matchers matches")
	assert.OK(t, m.MatchString("bazfoo"), "Matchers matches")
	assert.OK(t, !m.MatchString("fo"), "Matchers matches")
	assert.OK(t, m.Match([]byte("foo")), "Matchers matches")
	assert.OK(t, !m.Match([]byte("foobar")), "Matchers matches")
	assert.OK(t, !m.Match([]byte("")), "Matchers matches")
	assert.OK(t, m.Match([]byte("bar")), "Matchers matches")
	assert.OK(t, m.Match([]byte("bazfoo")), "Matchers matches")
	assert.OK(t, !m.Match([]byte("fo")), "Matchers matches")
}

func Test_MatchFields(t *testing.T) {
	var m evdb.MatchFields

	assert.OK(t, m.MatchString("foo", "bar"), "Empty MatchFields matches")
	assert.OK(t, m.MatchBytes("foo", []byte("bar")), "Empty MatchFields matches")
	m = m.Add("foo", evdb.MatchString("bar"))
	assert.OK(t, m.MatchString("foo", "bar"), "MatchFields matches")
	assert.OK(t, m.MatchBytes("foo", []byte("bar")), "MatchFields matches")
	assert.OK(t, !m.MatchString("foo", "baz"), "MatchFields matches")
	assert.OK(t, !m.MatchBytes("foo", []byte("baz")), "MatchFields matches")
	m = m.Add("foo", evdb.MatchString("baz"))
	assert.OK(t, m.MatchString("foo", "baz"), "MatchFields matches")
	m = m.Set("foo", evdb.MatchString("baz"))
	assert.OK(t, m.MatchString("foo", "baz"), "MatchFields matches")
	assert.OK(t, !m.MatchString("foo", "bar"), "MatchFields matches")
	m = nil
	m = m.Set("foo", evdb.MatchString("baz"))
	assert.OK(t, m.MatchString("foo", "baz"), "MatchFields matches")
	assert.OK(t, !m.MatchString("foo", "bar"), "MatchFields matches")

	var m0, m1 evdb.MatchFields
	m = m0.Merge(m1)
	assert.OK(t, m == nil, "MatchFields nil")
	m0 = m0.Set("foo", evdb.MatchString("bar"))
	m1 = m1.Set("foo", evdb.MatchString("baz"))
	m1 = m1.Set("bar", evdb.MatchString("baz"))
	m = m0.Merge(m1)
	assert.Equal(t, len(m), 2)
	mm, ok := m["foo"].(evdb.Matchers)
	assert.OK(t, ok, "foo Matchers")
	assert.Equal(t, len(mm), 2)
	assert.Equal(t, mm[0], evdb.MatchString("bar"))
	assert.Equal(t, mm[1], evdb.MatchString("baz"))
	m = m.Add("foo", evdb.MatchString("woo"))
	mmm, ok := m["foo"].(evdb.Matchers)
	assert.Equal(t, len(mmm), 3)
	assert.Equal(t, mmm[0], evdb.MatchString("bar"))
	assert.Equal(t, mmm[1], evdb.MatchString("baz"))
	assert.Equal(t, mmm[2], evdb.MatchString("woo"))
}
