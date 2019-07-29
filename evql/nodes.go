package evql

import (
	"math"
	"time"

	db "github.com/alxarch/evdb"
)

type noder interface {
	node()
}

type evalNode interface {
	noder
	Eval([]interface{}, *db.TimeRange, db.Results) []interface{}
}

type rawResult interface {
	noder
	Results(db.Results, db.TimeRange) db.Results
}

type aggResult interface {
	noder
	Aggregate(r db.Results, t *db.TimeRange) db.Result
}

type scanResultNode struct {
	Offset time.Duration
	Event  string
	Match  db.MatchFields
}

func (*scanResultNode) node() {}

func (s *scanResultNode) Query(tr db.TimeRange) db.ScanQuery {
	return db.ScanQuery{
		TimeRange: tr.Offset(s.Offset),
		Event:     s.Event,
		Fields:    s.Match,
	}
}

func (s *scanResultNode) Results(results db.Results, tr db.TimeRange) db.Results {
	tr = tr.Offset(s.Offset)
	start, end := tr.Start.Unix(), tr.End.Unix()
	out := db.Results{}
	m := s.Match
	for i := range results {
		r := &results[i]
		if r.Event == s.Event && m.Match(r.Fields) {
			switch rel := r.TimeRange.Rel(&tr); rel {
			case db.TimeRelEqual, db.TimeRelBetween:
				out = append(out, *r)
			case db.TimeRelAround:
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

type scanEvalNode struct {
	*scanResultNode
}

func (s *scanEvalNode) unwrap() noder { return s.scanResultNode }
func (s *scanEvalNode) Eval(out []interface{}, t *db.TimeRange, r db.Results) []interface{} {
	r = s.Results(r, *t)
	for i := range r {
		out = append(out, &r[i])
	}
	return out
}

type scanAggNode struct {
	Agg Aggregator
	*scanResultNode
}

func (s *scanAggNode) unwrap() noder { return s.scanResultNode }
func (s *scanAggNode) Aggregate(results db.Results, tr *db.TimeRange) db.Result {
	agg := BlankAggregator(s.Agg)
	t := *tr
	data := db.BlankData(tr, agg.Zero())
	results = s.scanResultNode.Results(results, *tr)
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
	// No fields group op
	return db.Result{
		TimeRange: t,
		Event:     s.Event,
		// Fields:    s.Match.Fields,
		Data: data,
	}
}

type zipAggNode struct {
	Offset time.Duration
	Agg    Aggregator
	Nodes  []aggResult
}

func (*zipAggNode) node() {}

func (n *zipAggNode) Aggregate(results db.Results, tr *db.TimeRange) db.Result {
	if n.Offset != 0 {
		t := tr.Offset(n.Offset)
		tr = &t
	}
	switch len(n.Nodes) {
	case 0:
		return db.Result{
			Data: db.BlankData(tr, math.NaN()),
		}
	case 1:
		return n.Nodes[0].Aggregate(results, tr)
	}
	var els []db.Result
	for _, el := range n.Nodes {
		els = append(els, el.Aggregate(results, tr))
	}
	out, tail := els[0], els[1:]
	a := BlankAggregator(n.Agg)
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

type blockNode []evalNode

func (blockNode) node() {}
func (b blockNode) Eval(out []interface{}, t *db.TimeRange, results db.Results) []interface{} {
	for _, n := range b {
		out = n.Eval(out, t, results)
	}
	return out
}

type aggOp struct {
	X  aggResult
	Y  aggResult
	Op Merger
}

func (op *aggOp) node() {}
func (op *aggOp) Aggregate(r db.Results, tr *db.TimeRange) db.Result {
	x := op.X.Aggregate(r, tr)
	y := op.Y.Aggregate(r, tr)
	for i := range x.Data {
		p := &x.Data[i]
		if 0 <= i && i < len(y.Data) {
			pp := &y.Data[i]
			p.Value = op.Op.Merge(p.Value, pp.Value)
		}
	}
	return db.Result{
		Data: x.Data,
	}
}

type groupNode struct {
	Nodes  []aggResult
	Group  []string
	Empty  string
	Agg    Aggregator
	groups []resultGroup
}

func (*groupNode) node() {}

func (g *groupNode) reset(results db.Results) {
	g.groups = nil
	scratch := db.Fields(make([]db.Field, 0, len(g.Group)))
	for i := range results {
		r := &results[i]
		scratch = r.Fields.AppendGrouped(scratch[:0], g.Empty, g.Group)
		g.add(scratch, r)
	}
}
func (g *groupNode) add(fields db.Fields, r *db.Result) {
	for i := range g.groups {
		group := &g.groups[i]
		if group.Fields.Equal(fields) {
			group.results = append(group.results, *r)
			return
		}
	}
	g.groups = append(g.groups, resultGroup{
		Fields:  fields.Copy(),
		results: db.Results{*r},
	})
}

type resultGroup struct {
	Fields  db.Fields
	results db.Results
}

func (g resultGroup) Values() map[string]string {
	v := make(map[string]string, len(g.Fields))
	for i := range g.Fields {
		f := &g.Fields[i]
		v[f.Label] = f.Value
	}
	return v
}

func (g *groupNode) Aggregator() Aggregator {
	if g.Agg == nil {
		return aggSum{}
	}
	return g.Agg
}

func (g *groupNode) evalGroup(out []interface{}, group *resultGroup, tr *db.TimeRange) []interface{} {
	for _, n := range g.Nodes {
		r := n.Aggregate(group.results, tr)
		r.Fields = group.Fields
		r.TimeRange = *tr
		out = append(out, &r)
	}
	return out
}

func (g *groupNode) Eval(out []interface{}, tr *db.TimeRange, results db.Results) []interface{} {
	g.reset(results)
	for i := range g.groups {
		group := &g.groups[i]
		out = g.evalGroup(out, group, tr)
	}
	return out
}

type valueNode struct {
	Offset time.Duration
	Value  float64
}

func (*valueNode) node() {}
func (v *valueNode) Aggregate(_ db.Results, t *db.TimeRange) db.Result {
	if v.Offset > 0 {
		tt := t.Offset(v.Offset)
		t = &tt
	}
	return db.Result{
		Data: db.BlankData(t, v.Value),
	}
}
