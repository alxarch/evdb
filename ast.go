package meter

import (
	"math"
	"time"
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
	Eval([]interface{}, *TimeRange, Results) []interface{}
}

type rawResult interface {
	noder
	Results(Results, TimeRange) Results
}

type aggResult interface {
	noder
	Aggregate(r Results, t *TimeRange, agg Aggregator) Result
}

type groupNode struct {
	Nodes  []aggResult
	Group  []string
	Empty  string
	Agg    Aggregator
	groups []resultGroup
}

func (*groupNode) node() {}

func (g *groupNode) reset(results Results) {
	g.groups = nil
	scratch := Fields(make([]Field, 0, len(g.Group)))
	for i := range results {
		r := &results[i]
		scratch = r.Fields.AppendGrouped(scratch[:0], g.Empty, g.Group)
		g.add(scratch, r)
	}
}
func (g *groupNode) add(fields Fields, r *Result) {
	for i := range g.groups {
		group := &g.groups[i]
		if group.Fields.Equal(fields) {
			group.results = append(group.results, *r)
			return
		}
	}
	g.groups = append(g.groups, resultGroup{
		Fields:  fields.Copy(),
		results: Results{*r},
	})
}

type resultGroup struct {
	Fields  Fields
	results Results
}

func (g *groupNode) Aggregator() Aggregator {
	if g.Agg == nil {
		return aggSum{}
	}
	return g.Agg
}

func (g *groupNode) evalGroup(out []interface{}, group *resultGroup, tr *TimeRange) []interface{} {
	agg := g.Aggregator()
	for _, n := range g.Nodes {
		r := n.Aggregate(group.results, tr, agg)
		r.Fields = group.Fields
		out = append(out, &r)
	}
	return out
}

func (g *groupNode) Eval(out []interface{}, tr *TimeRange, results Results) []interface{} {
	g.reset(results)
	for i := range g.groups {
		group := &g.groups[i]
		out = g.evalGroup(out, group, tr)
	}
	return out
}

type scanResultNode struct {
	Offset time.Duration
	Event  string
	Match  Fields
}

func (*scanResultNode) node() {}

func (s *scanResultNode) Query(tr TimeRange) ScanQuery {
	return ScanQuery{
		TimeRange: tr.Offset(s.Offset),
		Event:     s.Event,
		Match:     s.Match,
	}
}

type scanEvalNode struct {
	*scanResultNode
}

func (*scanEvalNode) node() {}
func (s *scanEvalNode) Eval(out []interface{}, t *TimeRange, r Results) []interface{} {
	r = s.Results(r, *t)
	for i := range r {
		out = append(out, &r[i])
	}
	return out
}
func (s *scanResultNode) Results(results Results, tr TimeRange) Results {
	tr = tr.Offset(s.Offset)
	start, end := tr.Start.Unix(), tr.End.Unix()
	out := Results{}
	m := s.Match.Map()
	for i := range results {
		r := &results[i]
		if r.Event == s.Event && r.Fields.MatchValues(m) {
			switch rel := r.TimeRange.Rel(&tr); rel {
			case TimeRelEqual, TimeRelBetween:
				out = append(out, *r)
			case TimeRelAround:
				s := *r
				s.Data = r.Data.Slice(start, end)
				if len(s.Data) > 0 {
					out = append(out, s)
				}
			default:
			}
		}
	}
	return out
}

type scanAggNode struct {
	*scanResultNode
}

func (s *scanAggNode) Aggregate(results Results, tr *TimeRange, agg Aggregator) Result {
	t := *tr
	data := tr.BlankData(agg.Zero())
	results = s.Results(results, t)
	for i := range data {
		d := &data[i]
		v := agg.Zero()
		for j := range results {
			r := &results[j]
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
		TimeRange: t,
		Event:     s.Event,
		Fields:    s.Match,
		Data:      data,
	}
}

type valueNode struct {
	Offset time.Duration
	Value  float64
}

func (*valueNode) node() {}
func (v *valueNode) Aggregate(_ Results, t *TimeRange, _ Aggregator) Result {
	if v.Offset > 0 {
		tt := t.Offset(v.Offset)
		t = &tt
	}
	return Result{
		Data: t.BlankData(v.Value),
	}
}

type aggNode struct {
	Offset time.Duration
	Agg    Aggregator
	Nodes  []aggResult
}

func (*aggNode) node() {}

func (n *aggNode) Aggregate(results Results, tr *TimeRange, agg Aggregator) Result {
	if n.Offset != 0 {
		t := tr.Offset(n.Offset)
		tr = &t
	}
	switch len(n.Nodes) {
	case 0:
		return Result{
			Data: tr.BlankData(math.NaN()),
		}
	case 1:
		return n.Nodes[0].Aggregate(results, tr, agg)
	}
	var els []Result
	if n.Agg != nil {
		agg = n.Agg
	}
	for _, el := range n.Nodes {
		els = append(els, el.Aggregate(results, tr, agg))
	}
	out, tail := els[0], els[1:]
	a := blankAgg(agg)
	for i := range out.Data {
		d := &out.Data[i]
		v := a.Zero()
		v = a.Aggregate(v, d.Value)
		for j := range tail {
			el := &tail[j]
			if 0 <= i && i < len(el.Data) {
				d := &el.Data[i]
				v = a.Aggregate(v, d.Value)
			} else {
				v = a.Aggregate(v, math.NaN())
			}
		}
		d.Value = v
	}
	return out
}

type opNode struct {
	X  aggResult
	Y  aggResult
	Op Merger
}

func (op *opNode) node() {}
func (op *opNode) Aggregate(r Results, tr *TimeRange, agg Aggregator) Result {
	x := op.X.Aggregate(r, tr, agg)
	y := op.Y.Aggregate(r, tr, agg)
	for i := range x.Data {
		p := &x.Data[i]
		if 0 <= i && i < len(y.Data) {
			pp := &y.Data[i]
			p.Value = op.Op.Merge(p.Value, pp.Value)
		}
	}
	return x
}

type blockNode []evalNode

func (blockNode) node() {}

func (b blockNode) Eval(out []interface{}, t *TimeRange, results Results) []interface{} {
	for _, n := range b {
		out = n.Eval(out, t, results)
	}
	return out
}

// func expString(exp ast.Expr) (string, error) {
// 	switch exp := exp.(type) {
// 	case *ast.BasicLit:
// 		return unquote(exp)
// 	case *ast.Ident:
// 		return exp.Name, nil
// 	default:
// 		return "", nil
// 	}
// }

// func unquote(lit *ast.BasicLit) (string, error) {
// 	if lit.Kind == token.STRING {
// 		return strconv.Unquote(lit.Value)
// 	}
// 	return lit.Value, nil
// }

func blankAgg(agg Aggregator) Aggregator {
	if _, avg := agg.(*aggAvg); avg {
		return new(aggAvg)
	}
	return agg
}

var _ = `

!match{country: GR|US|RU}

bid{ex: epom} / win[-1:h] * 2
!group{country}
{
	*match{country: GR}
	!avg{
		bid{color: blue},
		bid{color: green}[-1:week],
		bid{color: red},
	}

}

{
	*match{foo: bar}
	!avg{bid / bid[-1h]} + !sum{foo/bar}
	*by{country, cid, empty: true}
}


!match{foo: baz}
foo / !!avg{bar, goo}  


`
