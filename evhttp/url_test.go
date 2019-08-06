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

func TestScanQueryFromURL(t *testing.T) {
	tr := evdb.TimeRange{
		Start: tm("2019-08-01"),
		End:   tm("2019-08-02"),
		Step:  time.Hour,
	}
	tests := []struct {
		values  string
		wantQ   evdb.ScanQuery
		wantErr bool
	}{
		{"", evdb.ScanQuery{TimeRange: evdb.TimeRange{Step: -1}}, true},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.foo=bar&match.foo=baz", evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchAny("bar", "baz"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.suffix.foo=bar", evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchSuffix("bar"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.prefix.foo=bar", evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchPrefix("bar"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.equals.foo=bar", evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchString("bar"),
			},
		}, false},
		{"start=2019-08-01&end=2019-08-02&step=1h0m0s&event=win&match.regexp.foo=bar.*", evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": regexp.MustCompile("bar.*"),
			},
		}, false},
		{`start=2019-08-01&end=2019-08-02&step=1h&event=win&match.regexp.foo=bar%28foo`, evdb.ScanQuery{
			TimeRange: tr,
		}, true},
		{`start=2019-08-01&end=2019-08-02&step=1h&event=win&match.invalid.foo=bar%28foo`, evdb.ScanQuery{
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
			gotQ, err := ScanQueryFromURL(values)
			if (err != nil) != tt.wantErr {
				t.Errorf("ScanQueryFromURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotQ, tt.wantQ) {
				t.Errorf("ScanQueryFromURL() = %v, want %v", gotQ, tt.wantQ)
			}
		})
	}
}

func TestEncodeScanQuery(t *testing.T) {
	tr := evdb.TimeRange{
		Start: tm("2019-08-01"),
		End:   tm("2019-08-02"),
		Step:  time.Hour,
	}
	tests := []struct {
		wantStr string
		q       *evdb.ScanQuery
		wantErr bool
	}{
		{"", nil, true},
		{"end=1564704000&event=win&match.regexp.foo=%5E%28bar%7Cbaz%29%24&start=1564617600&step=1h0m0s", &evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchAny("bar", "baz"),
			},
		}, false},
		{"end=1564704000&event=win&match.suffix.foo=bar&start=1564617600&step=1h0m0s", &evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchSuffix("bar"),
			},
		}, false},
		{"end=1564704000&event=win&match.prefix.foo=bar&start=1564617600&step=1h0m0s", &evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchPrefix("bar"),
			},
		}, false},
		{"end=1564704000&event=win&match.foo=bar&start=1564617600&step=1h0m0s", &evdb.ScanQuery{
			TimeRange: tr,
			Event:     "win",
			Fields: evdb.MatchFields{
				"foo": evdb.MatchString("bar"),
			},
		}, false},
		{"end=1564704000&event=win&match.regexp.foo=bar.%2A&start=1564617600&step=1h0m0s", &evdb.ScanQuery{
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
			if err := EncodeScanQuery(values, tt.q); (err != nil) != tt.wantErr {
				t.Errorf("EncodeScanQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if s := values.Encode(); s != tt.wantStr {
				t.Errorf("EncodeScanQuery() = %s, want %s", s, tt.wantStr)

			}
		})
	}
}
