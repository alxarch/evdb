package evhttp

import (
	"net/url"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/alxarch/evdb"
)

func tm(s string) time.Time {
	tm, err := ParseTime(s)
	if err != nil {
		panic(err)
	}
	return tm
}

func TestQueryFromURL(t *testing.T) {
	tr := evdb.TimeRange{
		Start: tm("2019-08-01"),
		End:   tm("2019-08-02"),
		Step:  time.Hour,
	}
	tests := []struct {
		values  string
		wantQ   evdb.Query
		wantErr bool
	}{
		{"", evdb.Query{TimeRange: evdb.TimeRange{Step: -1}}, true},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.foo=bar&match.foo=baz", evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchAny("bar", "baz"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.suffix.foo=bar", evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchSuffix("bar"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.prefix.foo=bar", evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchPrefix("bar"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.equals.foo=bar", evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchString("bar"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.regexp.foo=bar.*", evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": regexp.MustCompile("bar.*"),
			},
		}, false},
		{`start=2019-08-01&end=2019-08-02&step=1h&event=win&match.regexp.foo=bar%28foo`, evdb.Query{
			TimeRange: tr,
		}, true},
		{`start=2019-08-01&end=2019-08-02&step=1h&event=win&match.invalid.foo=bar%28foo`, evdb.Query{
			TimeRange: tr,
		}, true},
	}
	for _, tt := range tests {
		name := tt.values
		values, err := url.ParseQuery(tt.values)
		if err != nil {
			t.Fatal("failed to parse query", err)
		}
		t.Run(name, func(t *testing.T) {
			gotQ, err := QueryFromURL(values)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryFromURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotQ, tt.wantQ) {
				t.Errorf("QueryFromURL() = %v, want %v", gotQ, tt.wantQ)
			}
		})
	}
}

func TestEncodeQuery(t *testing.T) {
	tr := evdb.TimeRange{
		Start: tm("2019-08-01"),
		End:   tm("2019-08-02"),
		Step:  time.Hour,
	}
	tests := []struct {
		wantStr string
		q       *evdb.Query
		wantErr bool
	}{
		{"", nil, true},
		{"end=1564704000&event=win&match.regexp.foo=%5E%28bar%7Cbaz%29%24&start=1564617600&step=1h0m0s", &evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchAny("bar", "baz"),
			},
		}, false},
		{"end=1564704000&event=win&match.suffix.foo=bar&start=1564617600&step=1h0m0s", &evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchSuffix("bar"),
			},
		}, false},
		{"end=1564704000&event=win&match.prefix.foo=bar&start=1564617600&step=1h0m0s", &evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchPrefix("bar"),
			},
		}, false},
		{"end=1564704000&event=win&match.foo=bar&start=1564617600&step=1h0m0s", &evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchString("bar"),
			},
		}, false},
		{"end=1564704000&event=win&match.regexp.foo=bar.%2A&start=1564617600&step=1h0m0s", &evdb.Query{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": regexp.MustCompile("bar.*"),
			},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.wantStr, func(t *testing.T) {
			values := url.Values{}
			if err := EncodeQuery(values, tt.q); (err != nil) != tt.wantErr {
				t.Errorf("EncodeQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if s := values.Encode(); s != tt.wantStr {
				t.Errorf("EncodeQuery() = %s, want %s", s, tt.wantStr)

			}
		})
	}
}
