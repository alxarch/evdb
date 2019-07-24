package meter

import (
	"go/ast"
	"go/token"
	"math"
	"strconv"
)

var _ = `
{
	!match{foo: bar}
	!offset[-1: h]
	foo / bar
	bar / baz
	!group{empty: "n/a", foo, bar, baz}
}
`

type noder interface {
	node()
}

type evalNode interface {
	noder
	Eval([]interface{}, Results) []interface{}
}

type resultNode interface {
	noder
	Result(Results) Result
}

type groupNode struct {
	scanNode
	Nodes  []resultNode
	Group  []string
	Empty  string
	groups []groupedResult
}

func (*groupNode) node() {}

func (g *groupNode) reset(results Results) {
	g.groups = nil
	for i := range results {
		r := &results[i]
		g.add(r)
	}
}
func (g *groupNode) add(r *Result) {
	for i := range g.groups {
		group := &g.groups[i]
		if r.Fields.Includes(group.Fields) {
			group.results = append(group.results, *r)
			return
		}
	}
	g.groups = append(g.groups, groupedResult{
		Fields:  r.Fields.GroupBy(g.Empty, g.Group),
		results: Results{*r},
	})
}

type groupedResult struct {
	Fields  Fields
	results Results
}

func (g *groupNode) evalGroup(out []interface{}, group *groupedResult) []interface{} {
	for _, n := range g.Nodes {
		r := n.Result(group.results)
		r.Fields = group.Fields
		out = append(out, &r)
	}
	return out
}

func (g *groupNode) Eval(out []interface{}, raw Results) []interface{} {
	g.reset(raw)
	for i := range g.groups {
		group := &g.groups[i]
		out = g.evalGroup(out, group)
	}
	return out
}

type scanNode struct {
	TimeRange
	Agg   Aggregator
	Match Fields
}

func (*scanNode) node() {}
func (s *scanNode) Result(event string) scanResultNode {
	return scanResultNode{
		ScanQuery: s.Query(event),
		Agg:       s.Agg,
	}
}

type scanResultNode struct {
	ScanQuery
	Agg Aggregator
}

func (*scanResultNode) node() {}

func (s *scanResultNode) Result(results Results) Result {
	data := s.BlankData(s.Agg.Zero())
	agg := blankAgg(s.Agg)
	raw := s.MatchResults(results)
	for i := range data {
		d := &data[i]
		v := agg.Zero()
		for j := range raw {
			r := &raw[j]
			if 0 <= i && i < len(r.Data) {
				p := r.Data[i]
				v = agg.Aggregate(v, p.Value)
			} else {
				v = agg.Aggregate(v, math.NaN())
			}
		}
		d.Value = v
	}
	return Result{
		Fields: s.Match,
		Data:   data,
	}
}

type valueNode struct {
	TimeRange
	Value float64
}

func (*valueNode) node() {}
func (v *valueNode) Result(_ Results) Result {
	return Result{
		Data: v.BlankData(v.Value),
	}
}

type aggNode struct {
	TimeRange
	Agg   Aggregator
	Nodes []resultNode
}

func (*aggNode) node() {}

func (r *Result) Rel(other *Result) TimeRel {
	if r.Event == other.Event && r.Fields.Equal(other.Fields) {
		return r.TimeRange.Rel(&other.TimeRange)
	}
	return TimeRelNone
}

func (results Results) Find(r *Result, rel TimeRel) *Result {
	for i := range results {
		rr := &results[i]
		if rr.Rel(r) == rel {
			return rr
		}
	}
	return nil
}

func (n *aggNode) Result(in Results) Result {
	var els []Result
	for _, el := range n.Nodes {
		els = append(els, el.Result(in))
	}
	if len(els) > 0 {
		out, tail := els[0], els[1:]
		agg := blankAgg(n.Agg)
		for i := range out.Data {
			d := &out.Data[i]
			v := agg.Zero()
			v = agg.Aggregate(v, d.Value)
			for j := range tail {
				el := &tail[j]
				if 0 <= i && i < len(el.Data) {
					d := &el.Data[i]
					v = agg.Aggregate(v, d.Value)
				} else {
					v = agg.Aggregate(v, math.NaN())
				}
			}
			d.Value = v
		}
		return out
	}
	return Result{
		Data: n.BlankData(math.NaN()),
	}
}

type opNode struct {
	X  resultNode
	Y  resultNode
	Op Merger
}

func (op *opNode) node() {}
func (op *opNode) Result(r Results) Result {
	x := op.X.Result(r)
	y := op.Y.Result(r)
	for i := range x.Data {
		p := &x.Data[i]
		if 0 <= i && i < len(y.Data) {
			pp := &y.Data[i]
			p.Value = op.Op.Merge(p.Value, pp.Value)
		}
	}
	return x
}

type blockNode struct {
	scanNode
	nodes []evalNode
}

func (*blockNode) node() {}

func (b *blockNode) Eval(out []interface{}, in Results) []interface{} {
	for _, n := range b.nodes {
		out = n.Eval(out, in)
	}
	return out
}

func expString(exp ast.Expr) (string, error) {
	switch exp := exp.(type) {
	case *ast.BasicLit:
		return unquote(exp)
	case *ast.Ident:
		return exp.Name, nil
	default:
		return "", nil
	}
}

func unquote(lit *ast.BasicLit) (string, error) {
	if lit.Kind == token.STRING {
		return strconv.Unquote(lit.Value)
	}
	return lit.Value, nil
}

func blankAgg(agg Aggregator) Aggregator {
	if _, avg := agg.(*aggAvg); avg {
		return new(aggAvg)
	}
	return agg
}

// MatchResults finds results than match s
func (s *ScanQuery) MatchResults(results Results) []Result {
	start, end := s.Start.Unix(), s.End.Unix()
	raw := Results{}
	tr := &s.TimeRange
	for i := range results {
		r := &results[i]
		if r.Event == s.Event && s.Match.Includes(r.Fields) {
			switch r.TimeRange.Rel(tr) {
			case TimeRelEqual, TimeRelBetween:
				raw = append(raw, *r)
			case TimeRelAround:
				s := *r
				s.Data = r.Data.Slice(start, end)
				if len(s.Data) > 0 {
					raw = append(raw, s)
				}
			default:
				continue
			}
		}
	}
	return raw
}

var _ = `
!match{foo: bar}
!match{foo: baz}
!group{country, status}
foo / !avg{bar}


`
