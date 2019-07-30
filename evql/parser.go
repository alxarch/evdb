package evql

import (
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	db "github.com/alxarch/evdb"
	errors "golang.org/x/xerrors"
)

type noder interface {
	node()
}

type evalNode interface {
	noder
	Eval([]interface{}, *db.TimeRange, db.Results) []interface{}
}

type aggResult interface {
	noder
	Aggregate(r db.Results, t *db.TimeRange) db.Result
}

func parseRoot(exp ast.Expr) (blockNode, error) {
	fn, ok := exp.(*ast.FuncLit)
	if !ok {
		return nil, errorf(exp, "Invalid root expression %s", reflect.TypeOf(exp))
	}

	if len(fn.Body.List) == 0 {
		return nil, errorf(fn.Body, "Empty query body")
	}
	sel, err := newSelectBlock(nil, fn.Body.List...)
	if err != nil {
		return nil, err
	}
	return parseBlock(sel, fn.Body.List...)
}

func parseClause(s *selectBlock, exp *ast.StarExpr) (err error) {
	const op = token.MUL
	fn, args := parseCall(exp.X)
	clause := getName(fn)
	switch strings.ToUpper(clause) {
	case "SELECT":
		return nil
	case "WHERE":
		if s.Match != nil {
			return errorf(exp, "Duplicate WHERE clause %s%s", op, clause)
		}
		s.Match, err = parseMatchArgs(nil, args...)
	case "OFFSET":
		if len(args) == 2 {
			d, err := parseScanOffset(args[0], args[1])
			if err != nil {
				return err
			}
			s.Offset = d
			return nil
		}
		return errorf(exp, "Invalid OFFSET clause")
	case "BY", "GROUP", "GROUPBY":
		if s.Group != nil {
			return errorf(exp, "Duplicate GROUP clause %s%s", op, clause)
		}
		return parseGroupClause(s, args...)
	default:
		return errorf(fn, "Invalid clause %s%s", op, clause)
	}
	return
}
func parseGroupClause(s *selectBlock, args ...ast.Expr) (err error) {
	labels := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg := arg.(type) {
		case *ast.KeyValueExpr:
			switch key := getName(arg.Key); strings.ToLower(key) {
			case "agg":
				agg, err := parseAggregator(arg.Value)
				if err != nil {
					return errorf(arg.Value, "Invalid agg argument: %s", err)
				}
				s.Agg = agg
			case "empty":
				empty, err := parseString(arg.Value)
				if err != nil {
					return errorf(arg.Value, "Invalid empty argument: %s", err)
				}
				s.Empty = empty
			default:
				return errorf(arg.Key, "Invalid keyrord argument: %q", key)
			}
		default:
			v, err := parseString(arg)
			if err != nil {
				return errorf(arg, "Invalid group label arg: %s", err)
			}
			labels = append(labels, v)
		}
	}
	s.Group = labels
	return nil
}

func newSelectBlock(s *selectBlock, stmts ...ast.Stmt) (*selectBlock, error) {
	var b selectBlock
	for _, stmt := range stmts {
		exp, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		switch exp := exp.X.(type) {
		case *ast.StarExpr:
			if err := parseClause(&b, exp); err != nil {
				return nil, err
			}
		}
	}
	if s != nil {
		if b.Agg == nil {
			b.Agg = s.Agg
		}
		if b.Group == nil {
			b.Group = s.Group
		}
		if b.Match == nil {
			b.Match = s.Match
		}
		if b.Empty == "" {
			b.Empty = s.Empty
		}
		if b.Offset == 0 {
			b.Offset = s.Offset
		}
	}
	if b.Group != nil && b.Agg == nil {
		b.Agg = aggSum{}
	}
	return &b, nil
}

func parseBlock(b *selectBlock, stmts ...ast.Stmt) (blockNode, error) {
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *ast.BlockStmt:
			child, err := newSelectBlock(b, stmt.List...)
			if err != nil {
				return nil, err
			}
			block, err := parseBlock(child, stmt.List...)
			if err != nil {
				return nil, err
			}
			b.Select = append(b.Select, block)
		case *ast.ExprStmt:
			e := stmt.X
			var err error
			switch e := e.(type) {
			case *ast.StarExpr:
				err = parseSelectClause(b, e)
			default:
				err = parseSelectExpr(b, e)
			}
			if err != nil {
				return nil, err
			}
		default:
			return nil, errorf(stmt, "Invalid block statement")
		}
	}

	return b.Select, nil
}

func parseSelectClause(s *selectBlock, star *ast.StarExpr) error {
	fn, args := parseCall(star.X)
	clause := getName(fn)
	if strings.ToUpper(clause) != "SELECT" {
		return nil
	}
	if s.Select != nil {
		return errorf(star, "Duplicate SELECT clause")
	}
	s.Select = make([]evalNode, 0, len(args))
	for _, a := range args {
		if err := parseSelectExpr(s, a); err != nil {
			return errorf(a, "Invalid SELECT %s", err)
		}
	}
	return nil
}

func parseSelectExpr(s *selectBlock, e ast.Expr) error {
	a, err := parseResult(s.Agg, s, e)
	if err != nil {
		return errorf(e, "Invalid SELECT expression: %s", err)
	}
	if g := s.GroupNode(); g != nil {
		g.Nodes = append(g.Nodes, &astAggResult{
			Node:      e,
			aggResult: a,
		})
		return nil
	}
	switch r := a.(type) {
	case *scanAggNode:
		n := r.scanResultNode
		e := scanEvalNode{n}
		s.Select = append(s.Select, &e)
		return nil
	default:
		return errorf(e, "Invalid aggregate expresion without GROUP clause")
	}
}

func parseValueNode(s *selectBlock, exp *ast.BasicLit) (*valueNode, error) {
	v := valueNode{
		Offset: s.Offset,
	}
	switch exp.Kind {
	case token.INT:
		n, err := strconv.ParseInt(exp.Value, 10, 64)
		if err != nil {
			return nil, errorf(exp, "Failed to parse value: %s", err)
		}
		v.Value = float64(n)
		return &v, nil
	case token.FLOAT:
		f, err := strconv.ParseFloat(exp.Value, 64)
		if err != nil {
			return nil, errorf(exp, "Failed to parse value: %s", err)
		}
		v.Value = f
		return &v, nil
	default:
		return nil, errorf(exp, "Invalid scalar value %s", reflect.TypeOf(exp))
	}
}

func parseResult(agg Aggregator, s *selectBlock, e ast.Expr) (aggResult, error) {
	switch exp := e.(type) {
	case *ast.ParenExpr:
		return parseResult(agg, s, exp.X)
	case *ast.UnaryExpr:
		return parseAggExpr(agg, s, exp)
	case *ast.BasicLit:
		return parseValueNode(s, exp)
	case *ast.BinaryExpr:
		m := NewMerger(exp.Op)
		if m == nil {
			return nil, errorf(exp, "Invalid result operation %q", exp.Op)
		}
		x, err := parseResult(agg, s, exp.X)
		if err != nil {
			return nil, err
		}
		y, err := parseResult(agg, s, exp.Y)
		if err != nil {
			return nil, err
		}
		op := aggOp{
			X:  x,
			Y:  y,
			Op: m,
		}
		return &op, nil
	default:
		scan, err := parseScanResult(s, e)
		if err != nil {
			return nil, err
		}
		return &scanAggNode{agg, scan}, nil
	}

}
func parseScanResult(s *selectBlock, exp ast.Expr) (*scanResultNode, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		d, err := parseScanOffset(exp.Low, exp.High)
		if err != nil {
			return nil, err
		}
		scan, err := parseScanResult(s, exp.X)
		if err != nil {
			return nil, err
		}
		scan.Offset += d
		return scan, nil
	case *ast.CompositeLit:
		m, err := parseMatch(nil, token.ADD, exp.Elts...)
		if err != nil {
			return nil, err
		}
		scan, err := parseScanResult(s, exp.Type)
		if err != nil {
			return nil, err
		}
		scan.Match = m.Merge(s.Match)
		return scan, nil
	case *ast.Ident:
		return &scanResultNode{
			Event:  exp.Name,
			Match:  s.Match,
			Offset: s.Offset,
		}, nil
	default:
		return nil, errorf(exp, "Invalid scan result")
	}
}

func parseAggExpr(a Aggregator, s *selectBlock, exp *ast.UnaryExpr) (aggResult, error) {
	if exp.Op != token.NOT {
		return nil, errorf(exp, "Invalid aggregator keyword prefix %q", exp.Op)
	}
	prefix, name, args := parseAggFn(exp.X)
	agg := NewAggregator(name)
	if agg == nil {
		return nil, errorf(exp, "Invalid aggregator %s%s", exp.Op, name)
	}
	if prefix == 'Z' {
		z := zipAggNode{
			Offset: s.Offset,
			Agg:    agg,
		}
		for _, arg := range args {
			n, err := parseResult(a, s, arg)
			if err != nil {
				return nil, err
			}
			z.Nodes = append(z.Nodes, n)
		}
		return &z, nil
	}
	if len(args) != 1 {
		if len(args) == 0 {
			return nil, errorf(exp, "No arguments for %s%s", exp.Op, name)
		}
		return nil, errorf(exp, "Too many arguments for %s%s", exp.Op, name)
	}

	if prefix == 'V' {
		a, err := parseResult(agg, s, args[0])
		if err != nil {
			return nil, err
		}
		n := aggNode{
			Agg:       agg,
			aggResult: a,
		}
		return &n, nil
	}
	scan, err := parseScanResult(s, args[0])
	if err != nil {
		return nil, err
	}
	n := scanAggNode{
		Agg:            agg,
		scanResultNode: scan,
	}
	return &n, nil
}

func parseScanOffset(lo, hi ast.Expr) (time.Duration, error) {
	n, ok := parseOffset(lo)
	if !ok {
		return 0, errorf(lo, "Invalid offset")
	}
	name := getName(hi)
	unit := durationUnit(name)
	if unit == 0 {
		return 0, errorf(lo, "Invalid unit %q", name)
	}
	return time.Duration(n) * unit, nil
}

func parseMatchAny(values []string, exp ast.Expr) ([]string, error) {
	switch exp := exp.(type) {
	case *ast.BinaryExpr:
		if exp.Op != token.OR {
			return nil, errorf(exp, "Invalid op %q", exp.Op)
		}
		var err error
		values, err = parseMatchAny(values, exp.X)
		if err != nil {
			return nil, err
		}
		return parseMatchAny(values, exp.Y)
	case *ast.BasicLit:
		switch exp.Kind {
		case token.STRING:
			s, err := unquote(exp)
			if err != nil {
				return nil, errorf(exp, "Invalid match value: %s", err)
			}
			return append(values, s), nil
		default:
			return append(values, exp.Value), nil
		}
	case *ast.Ident:
		return append(values, exp.Name), nil
	default:
		return nil, errorf(exp, "Invalid match any expression %s", exp)
	}
}

func parseMatcher(exp ast.Expr) (db.Matcher, error) {
	switch exp := exp.(type) {
	case *ast.UnaryExpr:
		fn, args := parseCall(exp.X)
		name := getName(fn)
		if len(args) != 1 {
			return nil, errorf(exp, "Invalid matcher %s%s", exp.Op, name)
		}
		if exp.Op != token.NOT {
			return nil, errorf(exp, "Invalid matcher %s%s", exp.Op, name)
		}
		arg := args[0]
		v, err := parseString(arg)
		if err != nil {
			return nil, errorf(exp, "Invalid arg for matcher %s%s: %s", exp.Op, name, err)
		}
		switch strings.ToLower(name) {
		case "regexp":
			return regexp.Compile(v)
		case "prefix":
			return db.MatchPrefix(v), nil
		case "suffix":
			return db.MatchSuffix(v), nil
		default:
			return nil, errorf(fn, "Invalid matcher %s%s", exp.Op, name)
		}
	default:
		values, err := parseMatchAny(nil, exp)
		if err != nil {
			return nil, err
		}
		if len(values) == 1 {
			return db.MatchString(values[0]), nil
		}
		return db.MatchAny(values...), nil
	}
}

func parseMatch(m db.MatchFields, op token.Token, args ...ast.Expr) (db.MatchFields, error) {
	match, err := parseMatchArgs(m, args...)
	if err != nil {
		return nil, err
	}
	switch op {
	case token.ADD:
		return match.Merge(m), nil
	default:
		return match, nil
	}
}

func parseMatchArgs(match db.MatchFields, args ...ast.Expr) (db.MatchFields, error) {
	for _, el := range args {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil, errorf(el, "Invalid match expr type %s", reflect.TypeOf(el))
		}
		label, err := parseString(kv.Key)
		if err != nil {
			return nil, errorf(kv.Key, "Failed to parse match label: %s", err)
		}
		m, err := parseMatcher(kv.Value)
		if err != nil {
			return nil, errors.Errorf("Failed to parse match values for label %q: %s", label, err)
		}
		match = match.Set(label, m)
	}
	return match, nil
}

type selectBlock struct {
	Select []evalNode
	Group  []string
	Empty  string
	Agg    Aggregator
	Offset time.Duration
	Match  db.MatchFields
}

func (s *selectBlock) group() groupNode {
	agg := s.Agg
	if agg == nil {
		agg = aggSum{}
	}
	return groupNode{
		Group: s.Group,
		Empty: s.Empty,
		Agg:   agg,
	}
}
func (s *selectBlock) GroupNode() *groupNode {
	if s.Group == nil {
		return nil
	}
	switch len(s.Select) {
	case 0:
		g := s.group()
		s.Select = append(s.Select, &g)
		return &g
	case 1:
		g := s.Select[0]
		if g, ok := g.(*groupNode); ok {
			return g
		}
		return nil
	default:
		return nil
	}
}

func nodeQueries(dst []db.ScanQuery, t *db.TimeRange, n noder) []db.ScanQuery {
	type queryNode interface {
		noder
		Query(db.TimeRange) db.ScanQuery
	}
	type unwraper interface {
		unwrap() noder
	}

	switch n := n.(type) {
	case queryNode:
		return append(dst, n.Query(*t))
	case unwraper:
		return nodeQueries(dst, t, n.unwrap())
	case blockNode:
		for _, n := range n {
			dst = nodeQueries(dst, t, n)
		}
		return dst
	case *aggOp:
		dst = nodeQueries(dst, t, n.X)
		dst = nodeQueries(dst, t, n.Y)
		return dst
	case *zipAggNode:
		for _, n := range n.Nodes {
			dst = nodeQueries(dst, t, n)
		}
		return dst
	case *groupNode:
		for _, n := range n.Nodes {
			dst = nodeQueries(dst, t, n)
		}
		return dst
	default:
		fmt.Println(reflect.TypeOf(n))
		return dst
	}
}

// func (p *Parser) parseDuration(exp ast.Expr) (time.Duration, error) {
// 	switch exp := exp.(type) {
// 	case *ast.ParenExpr:
// 		return p.parseDuration(exp.X)
// 	case *ast.Ident:
// 		if d := durationUnit(exp.Name); d > 0 {
// 			return d, nil
// 		}
// 		return 0, errorf(exp, "Invalid duration unit %q", exp.Name)
// 	case *ast.BinaryExpr:
// 		x, err := p.parseDuration(exp.X)
// 		if err != nil {
// 			return 0, err
// 		}
// 		y, err := p.parseDuration(exp.Y)
// 		if err != nil {
// 			return 0, err
// 		}
// 		switch exp.Op {
// 		case token.ADD:
// 			return x + y, nil
// 		case token.SUB:
// 			return x - y, nil
// 		case token.MUL:
// 			return x / y, nil
// 		case token.REM:
// 			return x % y, nil
// 		case token.QUO:
// 			return x / y, nil
// 		default:
// 			return 0, errorf(exp, "Invalid duration operand %q", exp.Op)
// 		}
// 	case *ast.BasicLit:
// 		v := exp.Value
// 		switch exp.Kind {
// 		case token.STRING:
// 			s, err := strconv.Unquote(v)
// 			if err != nil {
// 				return 0, err
// 			}
// 			if d := durationUnit(s); d > 0 {
// 				return d, nil
// 			}
// 			return time.ParseDuration(s)
// 		case token.INT:
// 			n, err := strconv.ParseInt(v, 10, 64)
// 			return time.Duration(n), err
// 		default:
// 			return 0, errorf(exp, "Invalid duration literal %s", exp.Kind)
// 		}
// 	default:
// 		return 0, errorf(exp, "Invalid duration expression")
// 	}
// }

type scanResultNode struct {
	Offset time.Duration
	Event  string
	Match  db.MatchFields
}

func (s *scanResultNode) node() {}

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

func (v *valueNode) node() {}
func (v *valueNode) Aggregate(_ db.Results, t *db.TimeRange) db.Result {
	if v.Offset > 0 {
		tt := t.Offset(v.Offset)
		t = &tt
	}
	return db.Result{
		Data: db.BlankData(t, v.Value),
	}
}

type namedAggResult struct {
	aggResult
	Name string
}

func (n *namedAggResult) unwrap() noder { return n.aggResult }

func (n *namedAggResult) Aggregate(r db.Results, t *db.TimeRange) db.Result {
	out := n.aggResult.Aggregate(r, t)
	out.Event = n.Name
	out.TimeRange = *t
	out.Fields = nil
	return out
}

// aggNode reduces a result to a value
type aggNode struct {
	aggResult
	Agg Aggregator
}

// func (n *aggNode) node()         {}
func (n *aggNode) unwrap() noder { return n.aggResult }

func (n *aggNode) Aggregate(r db.Results, t *db.TimeRange) db.Result {
	a := n.aggResult.Aggregate(r, t)
	agg := BlankAggregator(n.Agg)
	v := AggregateData(a.Data, agg)
	a.Data.Fill(v)
	return a
}

type astAggResult struct {
	ast.Node
	aggResult
}

func (block blockNode) nameResults(fset *token.FileSet) {
	for _, n := range block {
		switch n := n.(type) {
		case blockNode:
			n.nameResults(fset)
		case *groupNode:
			group := n
			for i, n := range group.Nodes {
				if n, ok := n.(*astAggResult); ok {
					w := new(strings.Builder)
					printer.Fprint(w, fset, n.Node)
					group.Nodes[i] = &namedAggResult{
						Name:      w.String(),
						aggResult: n.aggResult,
					}
				}
			}
		}
	}
}

func unquote(lit *ast.BasicLit) (string, error) {
	if lit.Kind == token.STRING {
		return strconv.Unquote(lit.Value)
	}
	return lit.Value, nil
}

func getName(e ast.Expr) string {
	if e != nil {
		if id, ok := e.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""

}

func parseStrings(exp ...ast.Expr) ([]string, error) {
	var values []string
	for _, exp := range exp {
		s, err := parseString(exp)
		if err != nil {
			return nil, err
		}
		values = append(values, s)
	}
	return values, nil
}
func parseString(exp ast.Expr) (string, error) {
	switch exp := exp.(type) {
	case *ast.Ident:
		return exp.Name, nil
	case *ast.BasicLit:
		return unquote(exp)
	default:
		return "", errors.Errorf("Invalid string expression type %s", reflect.TypeOf(exp))
	}
}

// func parseClause(exp ast.Expr) (string, []ast.Expr) {
// 	if star, ok := exp.(*ast.StarExpr); ok {
// 		fn, args := parseCall(star.X)
// 		return strings.ToUpper(getName(fn)), args
// 	}
// 	return "", nil
// }

func parseCall(exp ast.Expr) (ast.Expr, []ast.Expr) {
	switch call := exp.(type) {
	case *ast.CompositeLit:
		return call.Type, call.Elts
	case *ast.CallExpr:
		return call.Fun, call.Args
	case *ast.SliceExpr:
		if call.Slice3 {
			return call.X, []ast.Expr{call.Low, call.High, call.Max}
		}
		return call.X, []ast.Expr{call.Low, call.High}
	case *ast.Ident:
		return exp, nil
	default:
		return nil, nil
	}
}

func parseDurationUnit(exp ast.Expr) time.Duration {
	if unit, ok := exp.(*ast.Ident); ok {
		return durationUnit(unit.Name)
	}
	return 0
}

func parseAggFn(exp ast.Expr) (prefix byte, name string, args []ast.Expr) {
	fn, args := parseCall(exp)
	name = getName(fn)
	if len(name) > 0 {
		switch name[0] {
		case 'z', 'Z':
			if len(name) > 3 {
				p, n := name[:3], name[3:]
				if strings.ToLower(p) != "zip" {
					return
				}
				prefix, name = 'Z', n
			}
		case 'v', 'V':
			prefix = 'V'
			name = name[1:]
		}
	}
	return
}

func parseAggregator(exp ast.Expr) (Aggregator, error) {
	name, err := parseString(exp)
	if err != nil {
		return nil, err
	}
	if a := NewAggregator(name); a != nil {
		return a, nil
	}
	return nil, errors.Errorf("Invalid aggregator name %q", name)
}

// func parseTime(exp ast.Expr, now time.Time) (time.Time, error) {
// 	if exp == nil {
// 		return now, nil
// 	}
// 	switch exp := exp.(type) {
// 	case *ast.BasicLit:
// 		switch lit := exp; lit.Kind {
// 		case token.STRING:
// 			v, err := strconv.Unquote(lit.Value)
// 			if err != nil {
// 				return time.Time{}, err
// 			}
// 			if strings.ToLower(v) == "now" {
// 				return now, nil
// 			}
// 			return time.Parse(time.RFC3339Nano, v)
// 		case token.INT:
// 			n, err := strconv.ParseInt(lit.Value, 10, 64)
// 			if err != nil {
// 				return time.Time{}, errors.Errorf("Invalid timestamp value: %s", err)
// 			}
// 			return time.Unix(n, 0), nil
// 		default:
// 			return time.Time{}, errors.Errorf("Invalid time literal %s", exp)
// 		}
// 	case *ast.Ident:
// 		switch strings.ToLower(exp.Name) {
// 		case "now":
// 			return now, nil
// 		default:
// 			return time.Time{}, errors.Errorf("Invalid time ident %s", exp.Name)
// 		}
// 	}
// 	return time.Time{}, errors.Errorf("Invalid time expr: %s", reflect.TypeOf(exp))
// }

// func parseStrings(dst []string, exp ...ast.Expr) ([]string, error) {
// 	for _, exp := range exp {
// 		s, err := parseString(exp)
// 		if err != nil {
// 			return nil, err
// 		}
// 		dst = append(dst, s)
// 	}
// 	return dst, nil
// }

const y2k = 946684800

func parseOffset(exp ast.Expr) (int64, bool) {
	switch e := exp.(type) {
	case *ast.BasicLit:
		if e.Kind != token.INT {
			return 0, false
		}
		n, err := strconv.ParseInt(e.Value, 10, 64)
		if err == nil && n < y2k {
			return n, true
		}
	case *ast.UnaryExpr:
		switch e.Op {
		case token.ADD:
			return parseOffset(e.X)
		case token.SUB:
			n, ok := parseOffset(e.X)
			if ok {
				return -n, true
			}
		}
	}
	return 0, false

}

func durationUnit(unit string) time.Duration {
	switch strings.ToLower(unit) {
	case "s", "sec", "second", "seconds":
		return time.Minute
	case "min", "minute", "m", "minutes":
		return time.Minute
	case "hour", "h":
		return time.Hour
	case "day", "d":
		return 24 * time.Hour
	case "w", "week", "weeks":
		return 24 * 7 * time.Hour
	case "month":
		return 30 * 24 * time.Hour
	default:
		return 0
	}
}
