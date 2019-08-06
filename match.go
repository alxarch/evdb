package evdb

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/alxarch/evdb/internal/misc"
	errors "golang.org/x/xerrors"
)

type Matcher interface {
	Match([]byte) bool
	MatchString(string) bool
}

var _ Matcher = (*regexp.Regexp)(nil)

type MatchString string

var _ Matcher = MatchString("")

func (m MatchString) Match(b []byte) bool {
	return string(m) == string(b)
}
func (m MatchString) String() string {
	return string(m)
}
func (m MatchString) MatchString(s string) bool {
	return string(m) == s
}

type Matchers []Matcher

var _ Matcher = Matchers(nil)

func (mm Matchers) Match(b []byte) bool {
	for _, m := range mm {
		if m.Match(b) {
			return true
		}
	}
	return false
}

// func (mm Matchers) String() string {
// 	return fmt.Sprintf("%s", ([]Matcher)(mm))
// }
func (mm Matchers) MatchString(s string) bool {
	for _, m := range mm {
		if m.MatchString(s) {
			return true
		}
	}
	return false
}

type MatchSuffix string

func (suffix MatchSuffix) Match(b []byte) bool {
	if n := len(b) - len(suffix); 0 <= n && n <= len(b) {
		return string(b[n:]) == string(suffix)
	}
	return false
}

func (suffix MatchSuffix) MatchString(s string) bool {
	return strings.HasSuffix(s, string(suffix))
}

type MatchPrefix string

func (prefix MatchPrefix) Match(b []byte) bool {
	return len(b) >= len(prefix) && string(b[:len(prefix)]) == string(prefix)
}

func (prefix MatchPrefix) MatchString(s string) bool {
	return strings.HasPrefix(s, string(prefix))
}

// MatchAny creates a Matcher matching any value
func MatchAny(values ...string) Matcher {
	distinct := make([]string, 0, len(values))
	distinct = misc.AppendDistinct(distinct, values...)
	for i, s := range distinct {
		distinct[i] = regexp.QuoteMeta(s)
	}
	pattern := fmt.Sprintf(`^(%s)$`, strings.Join(distinct, "|"))
	rx, _ := regexp.Compile(pattern)
	return rx
}

type MatchFields map[string]Matcher

func (mf MatchFields) MatchString(label string, s string) bool {
	if m := mf[label]; m != nil {
		return m.MatchString(s)
	}
	return true
}

func (mf MatchFields) MatchBytes(label string, b []byte) bool {
	if m := mf[label]; m != nil {
		return m.Match(b)
	}
	return true
}

func (mf MatchFields) Match(fields Fields) bool {
	if mf == nil {
		return true
	}
	for i := range fields {
		f := &fields[i]
		if !mf.MatchString(f.Label, f.Value) {
			return false
		}
	}
	return true
}

func (mf MatchFields) Copy() MatchFields {
	if mf == nil {
		return nil
	}
	cp := make(MatchFields, len(mf))
	for label, m := range mf {
		cp[label] = m
	}
	return cp
}

func (mf MatchFields) Set(label string, m Matcher) MatchFields {
	if mf == nil {
		return MatchFields{label: m}
	}
	mf[label] = m
	return mf
}
func mergeMatchers(a, b Matcher) Matcher {
	switch a := a.(type) {
	case nil:
		return b
	case Matchers:
		if b, ok := b.(Matchers); ok {
			return append(a, b...)
		}
		return append(a, b)
	default:
		m := Matchers{a}
		if b, ok := b.(Matchers); ok {
			return append(m, b...)
		}
		return append(m, b)
	}

}

func (mf MatchFields) Add(label string, m Matcher) MatchFields {
	if m == nil {
		return mf
	}
	if mf == nil {
		return MatchFields{label: m}
	}
	mf[label] = mergeMatchers(mf[label], m)
	return mf
}

func (mf MatchFields) Merge(other MatchFields) MatchFields {
	if mf == nil {
		return other.Copy()
	}
	for label, m := range other {
		mf = mf.Add(label, m)
	}
	return mf
}

type matchDB struct {
	match Matcher
	DB
}

func newMatchDB(db DB, m Matcher) (DB, error) {
	if db, ok := db.(*matchDB); ok {
		db.match = mergeMatchers(db.match, m)
		return db, nil
	}
	return &matchDB{
		match: m,
		DB:    db,
	}, nil
}

func (m *matchDB) Scan(ctx context.Context, queries ...Query) (Results, error) {
	cp := make([]Query, 0, len(queries))
	for _, q := range queries {
		if m.match.MatchString(q.Event) {
			cp = append(cp, q)
		}
	}
	return m.DB.Scan(ctx, cp...)
}

func (m *matchDB) Storer(event string) (Storer, error) {
	if !m.match.MatchString(event) {
		return nil, errors.Errorf("Event %q does not match %s", event, m.match)
	}
	return m.DB.Storer(event)
}

func (m *matchDB) apply(db DB) (DB, error) {
	return newMatchDB(db, m.match)
}

// MatchEvents filters queries and stores for a db
func MatchEvents(m Matcher) Option {
	return &matchDB{match: m}
}
