package meter

import (
	"context"
	"sync"
)

// Scanner scans stored data according to a query
type Scanner interface {
	Scan(ctx context.Context, span TimeRange, match Fields) (ScanResults, error)
}

// Scanners provides a Scanner for an event
type Scanners interface {
	Scanner(event string) Scanner
}

// ScannerIndex is an index of scanners that implements Scanners interface
type ScannerIndex map[string]Scanner

// Scanner implements Scanners interface
func (s ScannerIndex) Scanner(event string) Scanner {
	return s[event]
}

// Scan executes a query using scanners
func (q *Query) Scan(ctx context.Context, s Scanners, events ...string) (Results, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	done := ctx.Done()
	errc := make(chan error, len(events))
	ch := make(chan Result, len(events))
	match := q.Match.Sorted()
	wg := new(sync.WaitGroup)
	var agg Results
	go func() {
		defer close(errc)
		for r := range ch {
			agg = append(agg, r)
		}
	}()
	for i := range events {
		event := events[i]
		s := s.Scanner(event)
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := s.Scan(ctx, q.TimeRange, match)
			if err != nil {
				errc <- err
				return
			}
			if len(q.Group) > 0 {
				results = results.GroupBy(q.Group, q.EmptyValue)
			}

			for _, r := range results {
				select {
				case ch <- Result{
					Event:      event,
					ScanResult: r,
				}:
				case <-done:
					errc <- ctx.Err()
					return
				}
			}
		}()
	}
	wg.Wait()
	close(ch)
	for err := range errc {
		if err != nil {
			return nil, err
		}
	}
	return agg, nil
}

// ScanResult is result of a scan
type ScanResult struct {
	Fields Fields     `json:"fields,omitempty"`
	Data   DataPoints `json:"data,omitempty"`
}

func (r *ScanResult) Merge(other *ScanResult, m Merger) {
	r.Data = r.Data.Merge(m, other.Data...)

}

// ScanResults are results from a scan
type ScanResults []ScanResult

// Add adds a result
func (results ScanResults) Add(fields Fields, t int64, v float64) ScanResults {
	for i := range results {
		r := &results[i]
		if r.Fields.Equal(fields) {
			r.Data = r.Data.MergePoint(mergeSum, t, v)
			return results
		}
	}
	return append(results, ScanResult{
		Fields: fields.Copy(),
		Data:   []DataPoint{{t, v}},
	})

}

// Reset resets a result
func (r *ScanResult) Reset() {
	*r = ScanResult{
		Data: r.Data[:0],
	}
}

func (results ScanResults) Merge(fields Fields, data ...DataPoint) ScanResults {
	for i := range results {
		r := &results[i]
		if r.Fields.Equal(fields) {
			r.Data = r.Data.Merge(mergeSum, data...)
			return results
		}
	}
	return append(results, ScanResult{
		Fields: fields.Copy(),
		Data:   data,
	})
}

func (results ScanResults) GroupBy(labels []string, empty string) (grouped ScanResults) {
	for i := range results {
		r := &results[i]
		fields := r.Fields.GroupBy(empty, labels)
		grouped = grouped.Merge(fields, r.Data...)
	}
	return
}

type scanQuerier struct {
	scanners Scanners
}

// Query implements Querier interface
func (sq *scanQuerier) Query(ctx context.Context, q Query, events ...string) (Results, error) {
	return q.Scan(ctx, sq.scanners, events...)
}

// ScanQuerier turns a Scanners to a Querier
func ScanQuerier(s Scanners) Querier {
	return &scanQuerier{scanners: s}
}
