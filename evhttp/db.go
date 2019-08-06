package evhttp

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"time"

	"github.com/alxarch/evdb/evutil"
	errors "golang.org/x/xerrors"

	"github.com/alxarch/evdb/evql"

	"github.com/alxarch/evdb"
)

type db struct {
	url     string
	execer  evql.Execer
	scanner evdb.Scanner
	store   evdb.Store
}

func (db *db) String() string {
	return db.url
}

// Close implements evdb.DB
func (*db) Close() error {
	return nil
}

var _ evdb.DB = (*db)(nil)
var _ evql.Execer = (*db)(nil)

// Storer implements evdb.Store
func (db *db) Storer(event string) (evdb.Storer, error) {
	return db.store.Storer(event)
}

// Exec implements evql.Execer
func (db *db) Exec(ctx context.Context, t evdb.TimeRange, q string) ([]evdb.Results, error) {
	return db.execer.Exec(ctx, t, q)
}

// Scan implements evdb.Scanner
func (db *db) Scan(ctx context.Context, queries ...evdb.Query) (evdb.Results, error) {
	return db.scanner.Scan(ctx, queries...)
}

type opener struct{}

// Open implements evdb.Opener
func (opener) Open(baseURL string) (evdb.DB, error) {
	// TODO: [evhttp] dialer and transport options as url query params
	dialer := net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}
	transport := http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 0,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
	hc := http.Client{
		Transport: &transport,
	}
	var c HTTPClient = &hc
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return nil, errors.Errorf("Invalid URL scheme %q", u.Scheme)
	}
	db := new(db)
	db.url = baseURL

	scanURL := *u
	scanURL.Path = path.Join(u.Path, "scan")
	sq := Querier{
		URL:        scanURL.String(),
		HTTPClient: c,
	}
	db.scanner = evdb.NewScanner(&sq)

	execURL := *u
	execURL.Path = path.Join(u.Path, "query")
	db.execer = &Execer{
		URL:        execURL.String(),
		HTTPClient: c,
	}

	storeURL := *u
	storeURL.Path = path.Join(u.Path, "store")
	store := Store{
		BaseURL:    storeURL.String(),
		HTTPClient: c,
	}
	// Cache storers
	db.store = evutil.CacheStore(&store)
	return db, nil
}
func init() {
	evdb.Register("https", opener{})
	evdb.Register("http", opener{})
}
