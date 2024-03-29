package evdb

import (
	"context"
	"sync"
)

// Query is a query over a range of time
type Query struct {
	Event string
	TimeRange
	Fields MatchFields
}

type Scanner interface {
	Scan(ctx context.Context, queries ...Query) (Results, error)
}

type Querier interface {
	Query(ctx context.Context, q *Query) (Results, error)
}

// NewScanner convers a Querier to a Scanner
func NewScanner(q Querier) Scanner {
	if s, ok := q.(Scanner); ok {
		return s
	}
	s := scanner{q}
	return &s

}

type scanner struct {
	q Querier
}

// Scan implements Scanner interface
func (s *scanner) Scan(ctx context.Context, queries ...Query) (Results, error) {
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
			results, err := s.q.Query(ctx, q)
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

type ScanQueries []Query

// Compact merges overlapping queries
func (queries ScanQueries) Compact() ScanQueries {
	if len(queries) > 1 {
		return ScanQueries(nil).Merge(queries...)
	}
	return queries
}

// MergeQuery merges a query if it overlaps or appends it to the query list
func (queries ScanQueries) MergeQuery(q *Query) ScanQueries {
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
		s.Fields = s.Fields.Merge(q.Fields)
		// switch m := s.Match.(type) {
		// case MatchAny:
		// 	s.Match = append(m, q.Match)
		// case MatchValues:
		// 	if mv, ok := q.Match.(MatchValues); ok {
		// 		s.Match = m.Merge(mv)
		// 	} else {
		// 		s.Match = MatchAny{m, q.Match}
		// 	}
		// default:
		// 	s.Match = MatchAny{m, q.Match}
		// }
		return queries
	}
	return append(queries, Query{
		Event:     q.Event,
		Fields:    q.Fields.Copy(),
		TimeRange: q.TimeRange,
	})
}

func (queries ScanQueries) Merge(other ...Query) ScanQueries {
	for i := range other {
		q := &other[i]
		queries = queries.MergeQuery(q)
	}
	return queries
}
