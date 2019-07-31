package evutil

import (
	"context"
	"sync"

	db "github.com/alxarch/evdb"
)

// MuxScanner multiplexes ScanQuerier instances into a Scanner
type MuxScanner map[string]db.ScanQuerier

// Set assigns a ScanQuerier instance to some events
func (m MuxScanner) Set(s db.ScanQuerier, events ...string) MuxScanner {
	if m == nil {
		m = make(map[string]db.ScanQuerier)
	}
	for _, event := range events {
		m[event] = s
	}
	return m

}

// Scan implements Scanner
func (m MuxScanner) Scan(ctx context.Context, queries ...db.ScanQuery) (db.Results, error) {
	queries = db.ScanQueries(queries).Compact()
	wg := new(sync.WaitGroup)
	errc := make(chan error, len(queries))
	var out db.Results
	var mu sync.Mutex
	for i := range queries {
		q := &queries[i]
		s := m[q.Event]
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Merge all overlapping queries
			results, err := s.ScanQuery(ctx, q)
			if err == nil {
				mu.Lock()
				out = append(out, results...)
				mu.Unlock()
			}
			errc <- err
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
