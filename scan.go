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
	Match MatchFields
}

type ScanQueries []ScanQuery

type ScanQuerier interface {
	ScanQuery(ctx context.Context, q *ScanQuery) (Results, error)
}
type scanner struct {
	q ScanQuerier
}

// NewScanner convers a ScanQuerier to a Scanner
func NewScanner(q ScanQuerier) Scanner {
	s := scanner{q}
	return &s

}

// Compact merges overlapping queries
func (queries ScanQueries) Compact() ScanQueries {
	if len(queries) > 1 {
		return ScanQueries(nil).Merge(queries...)
	}
	return queries
}

// Scan implements Scanner interface
func (s *scanner) Scan(ctx context.Context, queries ...ScanQuery) (Results, error) {
	// Merge all overlapping queries
	queries = ScanQueries(queries).Compact()
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

// MergeQuery merges a query if it overlaps or appends it to the query list
func (queries ScanQueries) MergeQuery(q *ScanQuery) ScanQueries {
	for i := range queries {
		s := &queries[i]
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
		s.Match.Fields = s.Match.Merge(q.Match.Fields...)
		return queries
	}
	return append(queries, ScanQuery{
		Event: q.Event,
		Match: MatchFields{
			Fields: q.Match.Copy(),
		},
		TimeRange: q.TimeRange,
	})
}

// Match finds a matching query
func (queries ScanQueries) Match(q *ScanQuery) *ScanQuery {
	for i := range queries {
		s := &queries[i]
		if q.Event != s.Event {
			continue
		}
		if !s.Match.Includes(q.Match.Fields) {
			continue
		}
		switch rel := s.TimeRange.Rel(&q.TimeRange); rel {
		case TimeRelEqual, TimeRelBetween:
			return s
		}
	}
	return nil
}

func (queries ScanQueries) Merge(other ...ScanQuery) ScanQueries {
	for i := range other {
		q := &other[i]
		queries = queries.MergeQuery(q)
	}
	return queries
}
