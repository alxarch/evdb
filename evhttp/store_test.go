package evhttp_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/events"
	"github.com/alxarch/evdb/evhttp"
	"github.com/alxarch/evdb/evutil"
	"github.com/alxarch/evdb/internal/assert"
)

type mockHTTPClient struct {
	Handler http.Handler
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	rr := httptest.NewRecorder()
	m.Handler.ServeHTTP(rr, req)
	return rr.Result(), nil

}

var _ evhttp.HTTPClient = (*mockHTTPClient)(nil)

func TestStore(t *testing.T) {
	s := evutil.NewMemoryStore("foo", "bar")
	h := evhttp.InflateRequest(evhttp.StoreHandler(s, "/events"))
	client := evhttp.Store{
		HTTPClient: &mockHTTPClient{h},
		BaseURL:    "http://example.com/events",
	}
	fooStore := client.Storer("foo")
	snap := &evdb.Snapshot{
		Labels: []string{"color", "taste"},
		Counters: []events.Counter{
			{Count: 112, Values: []string{"blue", "bitter"}},
			{Count: 34, Values: []string{"red", "sweet"}},
		},
	}
	err := fooStore.Store(snap)
	assert.NoError(t, err)
	st, _ := s.Storer("foo")
	ss := st.(*evutil.MemoryStorer).Last()
	assert.Equal(t, ss.Labels, snap.Labels)
	assert.Equal(t, ss.Counters, snap.Counters)
}
