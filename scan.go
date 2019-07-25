package meter

import (
	"context"
	"sync"
)

type Scanner interface {
	Scan(ctx context.Context, queries ...ScanQuery) (Results, error)
}

type ScanQuery struct {
	Event string
	TimeRange
	Match Fields
}

type ScanQueries []ScanQuery

type ScanQuerier interface {
	ScanQuery(ctx context.Context, q *ScanQuery) (Results, error)
}
type scanner struct {
	q ScanQuerier
}

func NewScanner(q ScanQuerier) Scanner {
	s := scanner{q}
	return &s

}
func (s *scanner) Scan(ctx context.Context, queries ...ScanQuery) (Results, error) {
	// Merge all overlapping queries
	if len(queries) > 1 {
		queries = ScanQueries(nil).Merge(queries...)
	}
	wg := new(sync.WaitGroup)
	var out Results
	var mu sync.Mutex
	errc := make(chan error, len(queries))
	for i := range queries {
		q := &queries[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := s.q.ScanQuery(ctx, q)
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
