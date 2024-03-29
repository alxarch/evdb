package evhttp

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/alxarch/evdb"
	errors "golang.org/x/xerrors"
)

// TimeRangeURL sets URL query values for a TimeRange
func TimeRangeURL(rawURL string, t *evdb.TimeRange) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	values := u.Query()
	EncodeTimeRange(values, *t)
	u.RawQuery = values.Encode()
	return u.String(), nil
}

// TimeRangeFromURL parses a TimeRange from URL query
func TimeRangeFromURL(values url.Values) (t evdb.TimeRange, err error) {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			if t.Step, err = time.ParseDuration(step[0]); err != nil {
				return
			}
		} else {
			t.Step = 0
		}
	} else {
		t.Step = -1
	}
	start, err := ParseTime(values.Get("start"))
	if err != nil {
		return
	}
	if !start.IsZero() {
		t.Start = start
	}
	end, err := ParseTime(values.Get("end"))
	if err != nil {
		return
	}
	if !end.IsZero() {
		t.End = end
	}
	return

}

// ScanURL sets URL query from a Query
func ScanURL(baseURL string, q *evdb.Query) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	values := u.Query()
	if err := EncodeQuery(values, q); err != nil {
		return "", err
	}
	u.RawQuery = values.Encode()
	return u.String(), nil
}

// QueryFromURL parses a Query from a URL query
func QueryFromURL(values url.Values) (q evdb.Query, err error) {
	q.TimeRange, err = TimeRangeFromURL(values)
	if err != nil {
		return
	}
	q.Fields, err = MatchFieldsFromURL(values)
	if err != nil {
		return
	}
	q.Event = values.Get("event")
	if q.Event == "" {
		err = errors.Errorf("Missing query.event")
		return
	}
	return
}

// MatchFieldsFromURL parses MatchFields from URL query
func MatchFieldsFromURL(values url.Values) (m evdb.MatchFields, err error) {
	m = make(map[string]evdb.Matcher)
	for key := range values {
		if !strings.HasPrefix(key, "match.") {
			continue
		}
		label := strings.TrimPrefix(key, "match.")
		var typ string
		if parts := strings.SplitN(label, ".", 2); len(parts) == 2 {
			label, typ = parts[1], parts[0]
		}
		switch strings.ToLower(typ) {
		case "regexp":
			rx, err := regexp.Compile(values.Get(key))
			if err != nil {
				return nil, errors.Errorf("Invalid query.%s: %s", key, err)
			}
			m[label] = rx
		case "suffix":
			m[label] = evdb.MatchSuffix(values.Get(key))
		case "prefix":
			m[label] = evdb.MatchPrefix(values.Get(key))
		case "equals":
			m[label] = evdb.MatchString(values.Get(key))
		case "":
			m[label] = evdb.MatchAny(values[key]...)
		default:
			return nil, errors.Errorf("Invalid match type %q", typ)
		}
	}
	return

}

// EncodeTimeRange sets URL query values for a TimeRange
func EncodeTimeRange(values url.Values, q evdb.TimeRange) {
	if !q.Start.IsZero() {
		values.Set("start", strconv.FormatInt(q.Start.Unix(), 10))
	}
	if !q.End.IsZero() {
		values.Set("end", strconv.FormatInt(q.End.Unix(), 10))
	}
	if q.Step != 0 {
		values.Set("step", q.Step.String())
	}
}

// EncodeQuery sets URL query values for a Query
func EncodeQuery(values url.Values, q *evdb.Query) error {
	if q == nil {
		return errors.Errorf("Nil query")
	}
	for label, m := range q.Fields {
		switch m := m.(type) {
		case *regexp.Regexp:
			values.Set("match.regexp."+label, m.String())
		case evdb.MatchSuffix:
			values.Set("match.suffix."+label, string(m))
		case evdb.MatchPrefix:
			values.Set("match.prefix."+label, string(m))
		case evdb.MatchString:
			values.Set("match."+label, string(m))
		default:
			return errors.Errorf("Cannot convert %q matcher to query", label)
		}
	}
	EncodeTimeRange(values, q.TimeRange)
	if q.Event != "" {
		values.Set("event", q.Event)
	}
	return nil
}

// ParseTime parses time in various formats
func ParseTime(v string) (time.Time, error) {
	if strings.Contains(v, ":") {
		if strings.Contains(v, ".") {
			return time.ParseInLocation(time.RFC3339Nano, v, time.UTC)
		}
		return time.ParseInLocation(time.RFC3339, v, time.UTC)
	}
	if strings.Contains(v, "-") {
		return time.ParseInLocation("2006-01-02", v, time.UTC)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(n, 0), nil
}
