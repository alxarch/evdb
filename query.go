package meter

import (
	"net/url"
	"time"
)

type QueryMode uint8

const (
	ModeScan QueryMode = iota
	ModeExact
	ModeValues
)

func (m QueryMode) String() string {
	switch m {
	case ModeExact:
		return "exact"
	case ModeScan:
		return "scan"
	case ModeValues:
		return "values"
	}
	return "querymodeinvalid"
}

type QueryBuilder struct {
	Mode       QueryMode
	Events     []string
	Start, End time.Time
	Group      []string
	Query      url.Values
	Resolution string
}

type Query struct {
	Mode       QueryMode
	Event      Descriptor
	Start, End time.Time
	Group      []string
	Values     []map[string]string
	Resolution Resolution
	err        error
}

func (q *Query) Error() error {
	return q.err
}

func NewQueryBuilder() QueryBuilder {
	return QueryBuilder{Query: url.Values{}}
}
func (q QueryBuilder) Exact() QueryBuilder {
	q.Mode = ModeExact
	return q
}
func (q QueryBuilder) Values() QueryBuilder {
	q.Mode = ModeValues
	return q
}
func (q QueryBuilder) Scan() QueryBuilder {
	q.Mode = ModeScan
	return q
}
func (q QueryBuilder) Between(start, end time.Time) QueryBuilder {
	q.Start, q.End = start, end
	return q
}
func (q QueryBuilder) At(res Resolution) QueryBuilder {
	q.Resolution = res.Name()
	return q
}
func (q QueryBuilder) Where(label string, value ...string) QueryBuilder {
	if q.Query == nil {
		q.Query = url.Values{}
	}
	q.Query[label] = value
	return q
}
func (q QueryBuilder) GroupBy(label ...string) QueryBuilder {
	q.Group = label
	return q
}
func (q QueryBuilder) From(event ...string) QueryBuilder {
	q.Events = event
	return q
}

func (q QueryBuilder) QueryValues(d *Desc) []map[string]string {
	if d == nil || q.Query == nil {
		return nil
	}
	queries := d.MatchingQueries(q.Query)
	if len(q.Group) != 0 {
		for _, g := range q.Group {
			if !d.HasLabel(g) {
				return nil
			}
			delete(queries, g)
		}
	}
	return QueryPermutations(queries)
}

func QueryPermutations(input url.Values) []map[string]string {
	vcount := []int{}
	keys := []string{}
	combinations := [][]int{}
	for k, v := range input {
		if c := len(v); c > 0 {
			keys = append(keys, k)
			vcount = append(vcount, c)
		}
	}
	var generate func([]int)
	generate = func(comb []int) {
		if i := len(comb); i == len(vcount) {
			combinations = append(combinations, comb)
			return
		} else {
			for j := 0; j < vcount[i]; j++ {
				next := make([]int, i+1)
				if i > 0 {
					copy(next[:i], comb)
				}
				next[i] = j
				generate(next)
			}
		}
	}
	generate([]int{})
	results := make([]map[string]string, 0, len(combinations))
	for _, comb := range combinations {
		result := make(map[string]string, len(comb))
		for i, j := range comb {
			key := keys[i]
			result[key] = input[key][j]
		}
		if len(result) > 0 {
			results = append(results, result)
		}
	}
	return results
}

func (qb QueryBuilder) Queries(events Resolver) (queries []Query) {
	q := Query{
		Mode:  qb.Mode,
		Start: qb.Start,
		End:   qb.End,
	}
	if events == nil {
		events = defaultRegistry
	}
eloop:
	for i := 0; i < len(qb.Events); i++ {
		eventName := qb.Events[i]
		event := events.Get(eventName)
		if event == nil {
			q.err = ErrUnregisteredEvent
			queries = append(queries, q)
			continue
		}

		desc := event.Describe()
		if desc == nil {
			q.err = ErrNilDesc
			queries = append(queries, q)
			continue
		}
		if q.err = desc.Error(); q.err != nil {
			queries = append(queries, q)
			continue
		}
		q.Event = event
		res, hasResolution := desc.Resolution(qb.Resolution)
		if !hasResolution {
			q.err = ErrInvalidResolution
			queries = append(queries, q)
			continue
		}
		q.Resolution = res
		if qb.Mode == ModeScan && len(qb.Group) != 0 {
			for _, g := range qb.Group {
				if !desc.HasLabel(g) {
					q.err = ErrInvalidGroupLabel
					queries = append(queries, q)
					continue eloop
				}
			}
		}
		q.Group = qb.Group
		qvs := desc.MatchingQueries(qb.Query)
		if len(q.Group) != 0 {
			for _, g := range q.Group {
				delete(qvs, g)
			}
		}
		if len(qvs) == 0 {
			q.Values = []map[string]string{LabelValues{}}
		} else {
			q.Values = QueryPermutations(qvs)
		}
		queries = append(queries, q)
	}

	return

}

type Queryer interface {
	Query(queries ...Query) (Results, error)
}
