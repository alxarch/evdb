package meter

import (
	"context"
	"sync"
)

type Scanner interface {
	Scan(ctx context.Context, q *Query) ScanIterator
}
type ScanIterator interface {
	Next() bool
	Item() ScanItem
	Close() error
}
type ScanItem struct {
	Time   int64
	Count  int64
	Fields Fields
}

type Scanners interface {
	Scanner(event string) Scanner
}
type scanners struct {
	Scanners
}

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
		s := s.Scanner(event)
		if s == nil {
			continue
		}
		go func() {
			defer wg.Done()
			iter := s.Scan(ctx, q)
			var results Results
			for iter.Next() {
				item := iter.Item()
				results = results.Add(event, item.Fields, item.Count, item.Time)
				// ts := item.Time.Unix()
			}
			if err := iter.Close(); err != nil {
				errc <- err
				return
			}
			ch <- results
		}()
	}

	var results Results
	for r := range ch {
		results = append(results, r...)
	}
	err, _ := <-errc
	return results, err
}

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
