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

func (q *Query) Scan(ctx context.Context, s Scanners, events ...string) (Results, error) {
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
				tm := q.TruncateTimestamp(item.Time)
				n := float64(item.Count)
				results = results.Add(event, item.Fields, n, tm)
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

func NewScanIterator(ctx context.Context, items <-chan ScanItem, errors <-chan error) ScanIterator {
	ctx, cancel := context.WithCancel(ctx)
	iter := scanIterator{
		cancel: cancel,
		errc:   errors,
		items:  items,
	}
	return &iter
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

type EmptyScanIterator struct{}

func (EmptyScanIterator) Item() ScanItem { return ScanItem{} }
func (EmptyScanIterator) Close() error   { return nil }
func (EmptyScanIterator) Next() bool     { return false }
