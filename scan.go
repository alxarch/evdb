package meter

import (
	"context"
)

type Scanner interface {
	Scan(ctx context.Context, queries ...ScanQuery) ([]Result, error)
}

// type PartialScanner interface {
// 	Scan(ctx, TimeRange, Fields) (PartialResults, error)
// }
// type PartialResult struct {
// 	Fields []Fields
// 	Data DataPoints
// }

// // Scanners provides a Scanner for an event
// type Scanners interface {
// 	Scanner(event string) Scanner
// }

// // ScannerIndex is an index of scanners that implements Scanners interface
// type ScannerIndex map[string]Scanner

// // Scanner implements Scanners interface
// func (s ScannerIndex) Scanner(event string) Scanner {
// 	return s[event]
// }

// func (results Results) GroupBy(labels []string, empty string) (grouped ScanResults) {
// 	for i := range results {
// 		r := &results[i]
// 		fields := r.Fields.GroupBy(empty, labels)
// 		grouped = grouped.Merge(fields, r.Data...)
// 	}
// 	return
// }

// type scanQuerier struct {
// 	scanners Scanners
// }

// // Query implements Querier interface
// func (sq *scanQuerier) Query(ctx context.Context, q Query, events ...string) (Results, error) {
// 	return q.Scan(ctx, sq.scanners, events...)
// }

// // ScanQuerier turns a Scanners to a Querier
// func ScanQuerier(s Scanners) Querier {
// 	return &scanQuerier{scanners: s}
// }

type ScanQuery struct {
	Event string
	TimeRange
	Match Fields
}

type ScanQueries []ScanQuery

func (sq ScanQueries) MergeQuery(q *ScanQuery) ScanQueries {
	for i := range sq {
		s := &sq[i]
		if q.Event != s.Event {
			continue
		}
		switch rel := s.TimeRange.Rel(&q.TimeRange); rel {
		case TimeRelBetween, TimeRelEqual:
		case TimeRelOverlapsAfter:
			s.End = q.End
		case TimeRelOverlapsBefore:
			s.Start = q.Start
		case TimeRelAround:
			s.TimeRange = q.TimeRange
		default:
			continue
		}
		s.Match = s.Match.Merge(q.Match...)
		return sq
	}
	return append(sq, ScanQuery{
		Event:     q.Event,
		Match:     q.Match.Copy(),
		TimeRange: q.TimeRange,
	})

}

func (sq ScanQueries) Match(q *ScanQuery) *ScanQuery {
	for i := range sq {
		s := &sq[i]
		if q.Event != s.Event {
			continue
		}
		if !s.Match.Includes(q.Match) {
			continue
		}
		switch rel := s.TimeRange.Rel(&q.TimeRange); rel {
		case TimeRelEqual, TimeRelBetween:
			return s
		}
	}
	return nil
}

func (sq ScanQueries) Merge(queries ...ScanQuery) ScanQueries {
	for i := range queries {
		q := &queries[i]
		sq = sq.MergeQuery(q)
	}
	return sq
}

// // Scan executes a query using scanners
// func (q *Query) Scan(ctx context.Context, s Scanners, events ...string) (Results, error) {
// 	if ctx == nil {
// 		ctx = context.Background()
// 	}
// 	done := ctx.Done()
// 	errc := make(chan error, len(events))
// 	ch := make(chan Result, len(events))
// 	match := q.Match.Sorted()
// 	wg := new(sync.WaitGroup)
// 	var agg Results
// 	go func() {
// 		defer close(errc)
// 		for r := range ch {
// 			agg = append(agg, r)
// 		}
// 	}()
// 	for i := range events {
// 		event := events[i]
// 		s := s.Scanner(event)
// 		if s == nil {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			results, err := s.Scan(ctx, q.TimeRange, match)
// 			if err != nil {
// 				errc <- err
// 				return
// 			}
// 			if len(q.Group) > 0 {
// 				results = results.GroupBy(q.Group, q.EmptyValue)
// 			}

// 			for _, r := range results {
// 				select {
// 				case ch <- Result{
// 					Event:      event,
// 					ScanResult: r,
// 				}:
// 				case <-done:
// 					errc <- ctx.Err()
// 					return
// 				}
// 			}
// 		}()
// 	}
// 	wg.Wait()
// 	close(ch)
// 	for err := range errc {
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return agg, nil
// }
