package meter

import (
	"context"
	"sync"
)

// Scanner scans stored data according to a query
type Scanner interface {
	Scan(ctx context.Context, q *Query) ScanIterator
}

// ScanIterator is an iterator over scan results
type ScanIterator interface {
	Next() bool
	Item() ScanItem
	Close() error
}

// ScanItem is a result item for a scan
type ScanItem struct {
	Time   int64
	Count  int64
	Fields Fields
}

// Scanners provides a Scanner for an event
type Scanners interface {
	Scanner(event string) Scanner
}
type scanners struct {
	Scanners
}

// RunQuery implements QueryRunner interface
func (s scanners) RunQuery(ctx context.Context, q *Query, events ...string) (Results, error) {
	errc := make(chan error, len(events))
	ch := make(chan Results, len(events))
	wg := new(sync.WaitGroup)
	wg.Add(len(events))
	go func() {
		defer close(errc)
		defer close(ch)
		wg.Wait()
	}()
	for i := range events {
		event := events[i]
		go func() {
			defer wg.Done()
			s := s.Scanner(event)
			if s == nil {
				// errc <- errMissingEvent(event)
				return
			}
			iter := s.Scan(ctx, q)
			var results Results
			for iter.Next() {
				item := iter.Item()
				results = results.Add(event, item.Fields, item.Count, item.Time)
			}
			if err := iter.Close(); err != nil {
				errc <- err
				return
			}
			ch <- results
		}()
	}

	err, _ := <-errc
	if err != nil {
		return nil, err
	}
	var results Results
	for r := range ch {
		results = append(results, r...)
	}
	return results, nil
}

// ScanQueryRunner creates a QueryRunner from a Scanners instance
func ScanQueryRunner(s Scanners) QueryRunner {
	return scanners{s}
}

type scanIterator struct {
	cancel context.CancelFunc
	errc   <-chan error
	items  <-chan ScanItem
	err    error
	item   ScanItem
}

func (it *scanIterator) Next() bool {
	select {
	case item, ok := <-it.items:
		it.item = item
		return ok
	case e, ok := <-it.errc:
		if ok {
			it.err = e
		}
		return false
	}
}
func (it *scanIterator) Item() ScanItem {
	return it.item
}
func (it *scanIterator) Close() error {
	it.cancel()
	return it.err
}

type emptyScanIterator struct{}

func (emptyScanIterator) Item() ScanItem { return ScanItem{} }
func (emptyScanIterator) Close() error   { return nil }
func (emptyScanIterator) Next() bool     { return false }
