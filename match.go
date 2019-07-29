package evdb

import (
	"fmt"
	"regexp"
	"strings"
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
	distinct = appendDistinct(distinct, values...)
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

func (mf MatchFields) Add(label string, m Matcher) MatchFields {
	if mf == nil {
		return MatchFields{label: m}
	}
	switch mm := mf[label].(type) {
	case nil:
		mf[label] = m
	case Matchers:
		mf[label] = append(mm, m)
	default:
		mf[label] = Matchers{mm, m}
	}
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

// type MatchValues map[string][]string

// func (m MatchValues) Add(label string, values ...string) MatchValues {
// 	if m == nil {
// 		m = make(map[string][]string)
// 	}
// 	m[label] = appendDistinct(m[label], values...)
// 	return m
// }

// func (m MatchValues) Del(label string, values ...string) {
// 	all := m[label]
// 	if len(all) == 0 {
// 		return
// 	}
// 	keep := make([]string, 0, len(all))
// vloop:
// 	for _, v := range all {
// 		for _, d := range values {
// 			if v == d {
// 				continue vloop
// 			}
// 		}
// 		keep = append(keep, v)
// 	}
// 	m[label] = keep
// }

// func (m MatchValues) Merge(values MatchValues) MatchValues {
// 	if m == nil {
// 		return values.Copy()
// 	}
// 	for label, values := range values {
// 		v := m[label]
// 		m[label] = appendDistinct(v, values...)
// 	}
// 	return m
// }

// func (m MatchValues) Copy() MatchValues {
// 	if m == nil {
// 		return nil
// 	}
// 	cp := make(map[string][]string, len(m))
// 	for label, values := range m {
// 		cp[label] = append(([]string)(nil), values...)
// 	}
// 	return cp
// }

// func (m MatchValues) Includes(other MatchValues) bool {
// 	for label, values := range other {
// 		vv := m[label]
// 		if len(vv) == 0 {
// 			return false
// 		}
// 		for _, v := range values {
// 			if indexOf(vv, v) == -1 {
// 				return false
// 			}
// 		}
// 	}
// 	return true
// }

// type stringMatcher interface {
// 	MatchString(v string) bool
// }

// type match struct {
// 	isRx  bool
// 	match string
// }
// type matchString string

// func (m matchString) MatchString(s string) bool {
// 	return string(m) == s
// }
// func (m *match) matcher() (stringMatcher, error) {
// 	if m.isRx {
// 		return regexp.Compile(m.match)
// 	}
// 	return matchString(m.match), nil
// }

// type stringMatchers []stringMatcher

// func (s stringMatchers) addMatch(a matchString) stringMatchers {
// 	for _, m := range s {
// 		if m, ok := m.(matchString); ok && m == a {
// 			return s
// 		}
// 	}
// 	return append(s, a)
// }
// func (s stringMatchers) addRx(rx *regexp.Regexp) stringMatchers {
// 	rxs := rx.String()
// 	for _, m := range s {
// 		if m, ok := m.(*regexp.Regexp); ok && m.String() == rxs {
// 			return s
// 		}
// 	}
// 	return append(s, rx)
// }

// func (s stringMatchers) Add(m stringMatcher) stringMatchers {
// 	if m == nil {
// 		return s
// 	}
// 	switch m := m.(type) {
// 	case matchString:
// 		return s.addMatch(m)
// 	case *regexp.Regexp:
// 		return s.addRx(m)
// 	default:
// 		return append(s, m)
// 	}
// }

// func (s stringMatchers) MatchString(v string) bool {
// 	for _, m := range s {
// 		if m.MatchString(v) {
// 			return true
// 		}
// 	}
// 	return false
// }

// type MatchEq string
// type matchEq string
// func (m MatchEq) valueMatcher() valueMatcher {
// 	return
// 	v string) bool {
// 	return string(m) == v
// }
// type MatchRx string

// func (m matchEq) MatchValue(v string) bool {
// 	return string(m) == v
// }
// type FieldMatcher interface {
// 	MatchFields(f Fields) bool
// }

// type MatchMode byte

// const (
// 	MatchEqual MatchMode = '='
// 	MatchRx    MatchMode = '~'
// 	// MatchGlob  MatchOp = '*'
// 	MatchNot MatchMode = '!'
// )

// type Match struct {
// 	Mode  MatchMode `json:"m"`
// 	Value string    `json:"v"`
// }

// func (m *Match) valueMatcher() (valueMatcher, error) {
// 	switch m.Mode {
// 	case MatchRx:
// 		var rxs rxMatchers
// 		for _, v := range m.Values {
// 			rx, err := regexp.Compile(v)
// 			if err != nil {
// 				return nil, err
// 			}
// 			rxs = append(rxs, rx)
// 		}
// 		return rxs, nil
// 	case MatchNot:
// 		return newNotMathcer(m.Values...), nil
// 	case MatchEqual:
// 		return newAnyMatcher(m.Values...), nil
// 	default:
// 		return nil, errors.Errorf("Invalid match op %q", m.Op)
// 	}
// }

// // func newNotMathcer(values ...string) valueMatcher {
// // 	return notMatcher(appendDistinct(make([]string, 0, len(values)), values...))
// // }
// // func newAnyMatcher(values ...string) valueMatcher {
// // 	return anyMatcher(appendDistinct(make([]string, 0, len(values)), values...))
// // }

// type Matches []Match

// type Matchets map[string]Match

// func (fms Matchers) Merge(other Matchers) Matchers {
// 	if fms == nil {
// 		fms = make(map[string]OpMatchers)
// 	}
// 	for label, others := range other {
// 		fms[label] = fms[label].Merge(others...)
// 	}
// 	return fms
// }

// func (fms Matchers) FieldMatcher() (FieldMatcher, error) {
// 	fields := make(map[string]valueMatcher)
// 	for label, matchers := range fms {
// 		switch len(matchers) {
// 		case 0:
// 			continue
// 		case 1:
// 			m, err := matchers[0].Matcher()
// 			if err != nil {
// 				return nil, err
// 			}
// 			fields[label] = m
// 		default:
// 			var vms valueMatchers
// 			for _, matcher := range matchers {
// 				m, err := matcher.Matcher()
// 				if err != nil {
// 					return nil, err
// 				}
// 				vms = append(vms, m)
// 			}
// 			fields[label] = vms
// 		}
// 	}
// 	return fieldMatcher(fields), nil

// }

// type valueMatcher interface {
// 	MatchValue(v string) bool
// }

// type rxMatchers []*regexp.Regexp

// func (rxs rxMatchers) MatchValue(v string) bool {
// 	for _, rx := range rxs {
// 		if rx.MatchString(v) {
// 			return true
// 		}
// 	}
// 	return false
// }

// type notMatcher []string

// func (not notMatcher) MatchValue(v string) bool {
// 	for _, m := range not {
// 		if m == v {
// 			return false
// 		}
// 	}
// 	return true
// }

// type anyMatcher []string

// func (any anyMatcher) MatchValue(v string) bool {
// 	for _, m := range any {
// 		if m == v {
// 			return true
// 		}
// 	}
// 	return false
// }

// type valueMatchers []valueMatcher

// func (vms valueMatchers) MatchValue(v string) bool {
// 	for _, m := range vms {
// 		if m.MatchValue(v) {
// 			return true
// 		}
// 	}
// 	return false
// }

// type fieldMatcher map[string]valueMatcher

// func (m fieldMatcher) MatchFields(fields Fields) bool {
// 	for i := range fields {
// 		f := &fields[i]
// 		mm := m[f.Label]
// 		if mm == nil {
// 			continue
// 		}
// 		if !mm.MatchValue(f.Value) {
// 			return false
// 		}
// 	}
// 	return true
// }

// // type matcher interface {
// // 	Match(value string) bool
// // 	String() string
// // 	reset(s string) (matcher, error)
// // }

// // type Matchers []Matcher

// // func (any Matchers) merge(other Matcher) (Matcher, bool) {
// // 	for i, m := range any {
// // 		mm, ok := m.merge(other)
// // 		if ok {
// // 			any[i] = mm
// // 			return any, true
// // 		}
// // 	}
// // 	return append(any, other), true
// // }

// // func (any Matchers) Match(fields Fields) bool {
// // 	for _, m := range any {
// // 		if m.Match(fields) {
// // 			return true
// // 		}
// // 	}
// // 	return false
// // }

// // 	var any []string
// // 	if err := json.Unmarshal([]byte(s), &any); err != nil {
// // 		return nil, err
// // 	}
// // 	return matchAny(any), nil
// // }

// // func (any matchAny) String() string {
// // 	if any == nil {
// // 		return "null"
// // 	}
// // 	size := len(any) + 2
// // 	for _, v := range any {
// // 		size += len(v) + 2
// // 	}
// // 	b := make([]byte, size)
// // 	b = any.AppendJSON(b)
// // 	return string(b)
// // }

// // func (any matchAny) AppendJSON(b []byte) []byte {
// // 	if any == nil {
// // 		return append(b, "null"...)
// // 	}
// // 	b = append(b, '[')
// // 	for i, v := range any {
// // 		if i > 0 {
// // 			b = append(b, ',')
// // 		}
// // 		b = strconv.AppendQuote(b, v)
// // 	}
// // 	return append(b, ']')
// // }
// // func (any matchAny) Match(v string) bool {
// // 	for _, m := range any {
// // 		if m == v {
// // 			return true
// // 		}
// // 	}
// // 	return false
// // }

// // type MatchAny []Matcher

// // func (any MatchAny) Match(fields Fields) bool {
// // 	for _, m := range any {
// // 		if m.Match(fields) {
// // 			return true
// // 		}
// // 	}
// // 	return false
// // }

// func (m Matchers) Add(op MatchOp, label string, values ...string) Matchers {
// 	if m == nil {
// 		m = make(map[string]OpMatchers)
// 	}
// 	opm := m.find(op, label)
// 	opm.Values = append(opm.Values, values...)
// 	return m
// }

// func (m Matchers) find(op MatchOp, label string) *OpMatcher {
// 	ms := m[label]
// 	for i := range ms {
// 		opm := &ms[i]
// 		if opm.Op == op {
// 			return opm
// 		}
// 	}
// 	m[label] = append(ms, OpMatcher{
// 		Op: op,
// 	})
// 	return &ms[len(ms)-1]
// }

// func (m MatchValues) merge(other Matcher) (Matcher, bool) {
// 	switch other := other.(type) {
// 	case MatchValues:
// 		return m.Merge(other), true
// 	case Matchers:
// 		return other.merge(m)
// 	default:
// 		return nil, false
// 	}
// }

// type matcherType int

// const (
// 	_ matcherType = iota
// 	mString
// 	mRegexp
// 	mPrefix
// 	mSuffix
// 	mMulti
// )

// type jsonMatcher struct {
// 	Type matcherType `json:"type"`
// 	Value  interface{} `json:"value"`
// }

// func MatcherToInterface(m Matcher) (interface{}, error) {
// 	switch m := m.(type) {
// 	case *regexp.Regexp:
// 		return map[string]string{"regexp": m.String()}
// 	case MatchPrefix:
// 		return map[string]string{"prefix": string(m)}
// 	case MatchSuffix:
// 		return map[string]string{"suffix": string(m)}
// 	case MatchString:
// 		return string(m)
// 	case Matchers:
// 		xx := make([]interface{}, len(m))
// 		for i, mm := range m {
// 			x, err := MatcherToInterface(mm)
// 			if err != nil {
// 				return nil, err
// 			}
// 			xx[i] =x
// 		}
// 		return xx
// 	default:
// 		return nil, errors.New("Invalid matcher")
// 	}
// }
// func MatcherFromInterface(x interface{}) (Matcher, error) {
// 	switch x := x.(type) {
// 	case Matcher:
// 		return x, nil
// 	case []interface{}:
// 		matchers := make(Matchers, len(x))
// 		for i := range x {
// 			m, err := MatcherFromInterface(x[i])
// 			if err != nil {
// 				return nil, err
// 			}
// 			matchers[i] = m
// 		}
// 		return matchers, nil
// 	case map[string]interface{}:
// 		if px, isPrefix := x["prefix"]; isPrefix {
// 			if prefix, ok := px.(string); ok {
// 				return MatchPrefix(prefix)
// 			}
// 			return nil, errors.New("Invalid prefix matcher")

// 		}
// 		if sx, isSuffix := x["suffix"]; isSuffix {
// 			if suffix, ok := sx.(string); ok {
// 				return MatchSuffix(suffix)
// 			}
// 			return nil, errors.New("Invalid suffix matcher")

// 		}
// 		if rx, isRegexp := x["regexp"]; isRegexp {
// 			if pattern, ok := rx.(string); ok {
// 				return regexp.Compile(pattern)
// 			}
// 			return nil, errors.New("Invalid regexp matcher")

// 		}
// 	case string:
// 		return MatchString(x), nil
// 	}
// }

// func AppendMatcherJSON(m Matcher, dst []byte) []byte {
// 	if m == nil {
// 		return append(dst, "null"...)
// 	}
// 	switch m := m.(type) {
// 	case Matchers:
// 		if len(m) == 0 {
// 			return append(dst, "[]"...)
// 		}
// 		dst = append(dst, '[')
// 		for i, mm := range m {
// 			if i > 0 {
// 				dst = append(dst, ',')
// 			}
// 			dst = AppendMatcherJSON(dst, mm)
// 		}
// 		return append(dst, ']')
// 	case *regexp.Regexp:
// 		dst = append(dst, `{"rx":`...)
// 		dst = strconv.AppendQuote(dst, m.String())
// 		return append(dst, '}')
// 	case MatchString:
// 		dst = strconv.AppendQuote(dst, string(m))
// 	case MatchSuffix:
// 		dst = append(dst, `{"suffix":`...)
// 		dst = strconv.AppendQuote(dst, string(m))
// 		return append(dst, '}')
// 	case MatchPrefix:
// 		dst = append(dst, `{"prefix":`...)
// 		dst = strconv.AppendQuote(dst, string(m))
// 		return append(dst, '}')
// 	default:
// 		return nil

// }
// func (mf MatchFields) MarshalJSON() ([]byte, error) {
// 	if mf == nil {
// 		return []byte("null"), nil
// 	}

// 	jmf := make(map[string], len(mf))
// 	for label, m := range mf {
// 		case MatchPrefix:
// 			jmf[label] = jsonMatcher{mPrefix, string(m)}
// 		case MatchSuffix:
// 			jmf[label] = jsonMatcher{mSuffix, string(m)}
// 		case MatchString:
// 			jmf[label] = jsonMatcher{mString, string(m)}
// 		case MatchRegexp:
// 			jmf[label] = jsonMatcher{mRegexp, m.String()}
// 		case Matchers:
// 			data, err := m.
// 			jmf[label] = jsonMatcher{mMulti, m.String()}
// 		}
// 	}

// }
