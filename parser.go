package meter

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	errors "golang.org/x/xerrors"
)

type Querier interface {
	Query(ctx context.Context, r TimeRange, q string) ([]interface{}, error)
}

// Parser parses queries
type Parser struct {
	fset *token.FileSet
	root *selectBlock
}

func (p *Parser) Reset(query string) error {
	fset := token.NewFileSet()
	// Wrap query body
	query = fmt.Sprintf(`func(){%s}`, query)
	exp, err := parser.ParseExprFrom(fset, "", []byte(query), 0)
	if err != nil {
		return err
	}
	var body *ast.BlockStmt
	if fn, ok := exp.(*ast.FuncLit); ok {
		body = fn.Body
	}
	if body == nil {
		return errors.New("Empty query body")
	}

	*p = Parser{
		fset: fset,
	}
	s := selectBlock{
		Agg: aggSum{},
	}
	root, err := p.parseSelectBlock(&s, body.List...)
	if err != nil {
		return err
	}
	p.root = root
	return nil
}

func (p *Parser) parseClause(s, parent *selectBlock, op token.Token, exp ast.Expr) (err error) {
	fn, args := parseCall(exp)
	clause := getName(fn)
	switch strings.ToUpper(clause) {
	case "SELECT":
		if op != token.MUL {
			return p.Errorf(exp, "Invalid SELECT clause")
		}
		return nil
	case "WHERE":
		if s.Match != nil {
			return p.Errorf(exp, "Duplicate WHERE clause")
		}
		switch op {
		case token.SUB:
			// TODO: Fields.Del
			s.Match, err = p.parseMatchArgs(nil, args...)
			// s.Match = parent.Match.Copy().Del(s.Match...)
		case token.ADD:
			m := parent.Match.Copy()
			s.Match, err = p.parseMatchArgs(m, args...)
		case token.MUL:
			s.Match, err = p.parseMatchArgs(nil, args...)
		default:
			return p.Errorf(exp, "Invalid WHERE clause modifier %q", op)
		}
	case "GROUP":
		if s.Agg != nil {
			return p.Errorf(exp, "Duplicate GROUP clause")
		}
		if op != token.MUL || len(args) != 1 {
			return p.Errorf(exp, "Invalid GROUP clause")
		}
		s.Agg, err = parseAggregator(args[0])
	case "OFFSET":
		if len(args) == 2 {
			d, err := p.parseScanOffset(args[0], args[1])
			if err != nil {
				return err
			}
			switch op {
			case token.ADD:
				s.Offset = parent.Offset + d
			case token.SUB:
				s.Offset = parent.Offset - d
			case token.MUL:
				s.Offset = d
			default:
				return p.Errorf(fn, "Invalid clause %s%s", op, clause)
			}
			return nil

		}
		return p.Errorf(exp, "Invalid OFFSET clause")

	case "BY":
		if s.Group != nil {
			return p.Errorf(exp, "Duplicate BY clause")
		}
		if op != token.MUL {
			return p.Errorf(exp, "Invalid BY clause")
		}
		return p.parseGroupClause(s, args...)
	default:
		return p.Errorf(fn, "Invalid clause %s%s", op, clause)
	}
	return
}
func (p *Parser) parseGroupClause(s *selectBlock, args ...ast.Expr) (err error) {
	labels := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg := arg.(type) {
		case *ast.KeyValueExpr:
			switch key := getName(arg.Key); strings.ToLower(key) {
			case "agg":
				agg, err := parseAggregator(arg.Value)
				if err != nil {
					return p.Error(arg.Value, err)
				}
				s.Agg = agg
			case "empty":
				empty, err := parseString(arg.Value)
				if err != nil {
					return p.Error(arg.Value, err)
				}
				s.Empty = empty
			default:
				return p.Errorf(arg.Key, "Invalid keyrord argument: %q", key)
			}
		default:
			v, err := parseString(arg)
			if err != nil {
				return p.Errorf(arg, "Invalid group label arg: %s", err)
			}
			labels = append(labels, v)
		}
	}
	s.Group = labels
	return nil
}

func (p *Parser) parseSelectBlock(s *selectBlock, stmts ...ast.Stmt) (*selectBlock, error) {
	var b selectBlock
	for _, stmt := range stmts {
		exp, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		switch exp := exp.X.(type) {
		case *ast.StarExpr:
			if err := p.parseClause(&b, s, token.MUL, exp.X); err != nil {
				return nil, err
			}
		case *ast.UnaryExpr:
			switch exp.Op {
			case token.ADD, token.SUB:
				if err := p.parseClause(&b, s, exp.Op, exp.X); err != nil {
					return nil, err
				}
			}
		}
	}
	if b.Agg == nil {
		b.Agg = s.Agg
	}
	if b.Group == nil {
		b.Group = s.Group
	}
	if b.Group != nil && b.Agg == nil {
		b.Agg = aggSum{}
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

	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *ast.BlockStmt:
			child, err := p.parseSelectBlock(&b, stmt.List...)
			if err != nil {
				return nil, err
			}
			b.Select = append(b.Select, child)
		case *ast.ExprStmt:
			e := stmt.X
			var err error
			switch e := e.(type) {
			case *ast.StarExpr:
				err = p.parseSelectClause(&b, e)
			default:
				err = p.parseSelectExpr(&b, e)
			}
			if err != nil {
				return nil, err
			}
		default:
			return nil, p.Errorf(stmt, "Invalid block statement")
		}
	}

	return &b, nil
}

func (p *Parser) parseSelectClause(s *selectBlock, star *ast.StarExpr) error {
	fn, args := parseCall(star.X)
	clause := getName(fn)
	if strings.ToUpper(clause) != "SELECT" {
		return nil
	}
	if s.Select != nil {
		return p.Errorf(star, "Duplicate SELECT clause")
	}
	s.Select = make([]evalNode, 0, len(args))
	for _, a := range args {
		if err := p.parseSelectExpr(s, a); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) parseSelectExpr(s *selectBlock, e ast.Expr) error {
	a, err := p.parseAggResult(s, e)
	if err != nil {
		return err
	}
	if g := s.GroupNode(); g != nil {
		// Give a name to the result
		name, err := p.print(e)
		if err != nil {
			return err
		}
		n := namedAggResult{a, name}
		g.Nodes = append(g.Nodes, &n)
		return nil
	}
	switch r := a.(type) {
	case *scanAggNode:
		n := r.scanResultNode
		e := scanEvalNode{n}
		s.Select = append(s.Select, &e)
		return nil
	default:
		return p.Errorf(e, "Invalid aggregate expresion without GROUP clause")
	}
}

func (p *Parser) parseValueNode(s *selectBlock, exp *ast.BasicLit) (*valueNode, error) {
	v := valueNode{
		Offset: s.Offset,
	}
	switch exp.Kind {
	case token.INT:
		n, err := strconv.ParseInt(exp.Value, 10, 64)
		if err != nil {
			return nil, p.Error(exp, err)
		}
		v.Value = float64(n)
		return &v, nil
	case token.FLOAT:
		f, err := strconv.ParseFloat(exp.Value, 64)
		if err != nil {
			return nil, p.Error(exp, err)
		}
		v.Value = f
		return &v, nil
	default:
		return nil, p.Errorf(exp, "Invalid scalar value")
	}
}

func (p *Parser) parseAggResult(s *selectBlock, e ast.Expr) (aggResult, error) {
	switch exp := e.(type) {
	case *ast.ParenExpr:
		return p.parseAggResult(s, exp.X)
	case *ast.UnaryExpr:
		return p.parseAggExpr(s, exp)
	case *ast.BasicLit:
		return p.parseValueNode(s, exp)
	case *ast.BinaryExpr:
		// if opts.Group == nil {
		// 	return nil, p.Errorf(exp, "Cannot use operands without group")
		// }
		m := mergeOp(exp.Op)
		if m == nil {
			return nil, p.Errorf(exp, "Invalid result operation %q", exp.Op)
		}
		x, err := p.parseAggResult(s, exp.X)
		if err != nil {
			return nil, err
		}
		y, err := p.parseAggResult(s, exp.Y)
		if err != nil {
			return nil, err
		}
		op := opNode{
			X:  x,
			Y:  y,
			Op: m,
		}
		return &op, nil
	default:
		s, err := p.parseScanResult(s, e)
		if err != nil {
			return nil, err
		}
		return &scanAggNode{s}, nil
	}

}
func (p *Parser) parseScanResult(s *selectBlock, exp ast.Expr) (*scanResultNode, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		d, err := p.parseScanOffset(exp.Low, exp.High)
		if err != nil {
			return nil, err
		}
		scan, err := p.parseScanResult(s, exp.X)
		if err != nil {
			return nil, err
		}
		scan.Offset += d
		return scan, nil
	case *ast.CompositeLit:
		m, err := p.parseMatch(nil, token.ADD, exp.Elts...)
		if err != nil {
			return nil, err
		}
		scan, err := p.parseScanResult(s, exp.Type)
		if err != nil {
			return nil, err
		}
		scan.Match = scan.Match.Merge(m...)
		return scan, nil
	case *ast.Ident:
		return &scanResultNode{
			Event:  exp.Name,
			Match:  s.Match,
			Offset: s.Offset,
		}, nil
	default:
		return nil, p.Errorf(exp, "Invalid scan result")
	}
}

func (p *Parser) parseAggExpr(s *selectBlock, exp *ast.UnaryExpr) (aggResult, error) {
	if exp.Op != token.NOT {
		return nil, p.Errorf(exp, "Invalid result agg")
	}
	// if opts.Group == nil {
	// 	return nil, p.Errorf(exp, "Cannot use aggregators without group")
	// }
	fn, args := parseCall(exp.X)
	agg, err := parseAggregator(fn)
	if err != nil {
		return nil, p.Error(exp.X, err)
	}
	var nodes []aggResult
	for _, arg := range args {
		n, err := p.parseAggResult(s, arg)
		if err != nil {
			return nil, err
		}
		if n, ok := n.(aggResult); ok {
			nodes = append(nodes, n)
		} else {
			return nil, p.Errorf(arg, "Invalid result")
		}
	}
	a := aggNode{
		Offset: s.Offset,
		Agg:    agg,
		Nodes:  nodes,
	}
	return &a, nil
}

func (p *Parser) Queries(t TimeRange) ScanQueries {
	return nodeQueries(nil, &t, p.root)
}

func (p *Parser) Error(exp ast.Node, err error) error {
	// panic(err)
	pos := p.fset.Position(exp.Pos())
	return errors.Errorf(`Parse error at position %s: %s`, pos, err)
}

func (p *Parser) Errorf(exp ast.Node, msg string, args ...interface{}) error {
	return p.Error(exp, errors.Errorf(msg, args...))
}

func (p *Parser) parseScanOffset(lo, hi ast.Expr) (time.Duration, error) {
	n, ok := parseOffset(lo)
	if !ok {
		return 0, p.Errorf(lo, "Invalid offset")
	}
	name := getName(hi)
	unit := durationUnit(name)
	if unit == 0 {
		return 0, p.Errorf(lo, "Invalid unit %q", name)
	}
	return time.Duration(n) * unit, nil
}

func (p *Parser) parseMatchValues(values []string, exp ast.Expr) ([]string, error) {
	switch exp := exp.(type) {
	case *ast.BinaryExpr:
		if exp.Op != token.OR {
			return nil, errors.Errorf("Failed toInvalid op %q", exp.Op)
		}
		var err error
		values, err = p.parseMatchValues(values, exp.X)
		if err != nil {
			return nil, err
		}
		values, err = p.parseMatchValues(values, exp.Y)
		if err != nil {
			return nil, err
		}
		return values, nil
	default:
		s, err := parseString(exp)
		if err != nil {
			return nil, p.Errorf(exp, "Failed to parse match value: %s", err)
		}
		return append(values, s), nil
	}
}

func (p *Parser) parseDuration(exp ast.Expr) (time.Duration, error) {
	switch exp := exp.(type) {
	case *ast.ParenExpr:
		return p.parseDuration(exp.X)
	case *ast.Ident:
		if d := durationUnit(exp.Name); d > 0 {
			return d, nil
		}
		return 0, p.Errorf(exp, "Invalid duration unit %q", exp.Name)
	case *ast.BinaryExpr:
		x, err := p.parseDuration(exp.X)
		if err != nil {
			return 0, err
		}
		y, err := p.parseDuration(exp.Y)
		if err != nil {
			return 0, err
		}
		switch exp.Op {
		case token.ADD:
			return x + y, nil
		case token.SUB:
			return x - y, nil
		case token.MUL:
			return x / y, nil
		case token.REM:
			return x % y, nil
		case token.QUO:
			return x / y, nil
		default:
			return 0, p.Errorf(exp, "Invalid duration operand %q", exp.Op)
		}
	case *ast.BasicLit:
		v := exp.Value
		switch exp.Kind {
		case token.STRING:
			s, err := strconv.Unquote(v)
			if err != nil {
				return 0, err
			}
			if d := durationUnit(s); d > 0 {
				return d, nil
			}
			return time.ParseDuration(s)
		case token.INT:
			n, err := strconv.ParseInt(v, 10, 64)
			return time.Duration(n), err
		default:
			return 0, p.Errorf(exp, "Invalid duration literal %s", exp.Kind)
		}
	default:
		return 0, p.Errorf(exp, "Invalid duration expression")
	}
}

// Eval executes the query against some results
func (p *Parser) Eval(out []interface{}, t TimeRange, results Results) []interface{} {
	if p.root != nil {
		out = p.root.Eval(out, &t, results)
	}
	return out
}

func (p *Parser) print(x interface{}) (string, error) {
	w := new(strings.Builder)
	if err := printer.Fprint(w, p.fset, x); err != nil {
		return "", err
	}
	return w.String(), nil
}

func (p *Parser) parseMatch(m Fields, op token.Token, args ...ast.Expr) (Fields, error) {
	match, err := p.parseMatchArgs(m, args...)
	if err != nil {
		return nil, err
	}
	switch op {
	case token.ADD:
		return match.Merge(m...), nil
	default:
		return match, nil
	}
}

func (p *Parser) parseMatchArgs(match Fields, args ...ast.Expr) (Fields, error) {
	for _, el := range args {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil, p.Errorf(el, "Invalid match expr type %s", reflect.TypeOf(el))
		}
		key, err := parseString(kv.Key)
		if err != nil {
			return nil, p.Errorf(kv.Key, "Failed to parse match label: %s", err)
		}
		values, err := p.parseMatchValues(nil, kv.Value)
		if err != nil {
			return nil, errors.Errorf("Failed to parse match values for label %q: %s", key, err)
		}
		for _, v := range values {
			match = match.Add(Field{
				Label: key,
				Value: v,
			})
		}
	}
	return match, nil
}

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

type selectBlock struct {
	Select []evalNode
	Group  []string
	Empty  string
	Agg    Aggregator
	Offset time.Duration
	Match  Fields
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

func (*selectBlock) node() {}

func (b *selectBlock) Eval(out []interface{}, t *TimeRange, results Results) []interface{} {
	for _, n := range b.Select {
		out = n.Eval(out, t, results)
	}
	return out
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
	+WHERE{}
	!WHERE{foo: bar}
	!GROUP{avg}
	!BY{foo, bar, baz}
}
{
	*match{foo: bar}
	!avg{bid / bid[-1h]} + !sum{foo/bar}
	*by{country, cid, empty: true}
}
!sum{
	*match{foo: bar},
	goo -
	foo{bar: baz}[-1h],
	*by{goo, bar, baz, empty: "-"},
}


!match{foo: baz}
foo / !!avg{bar, goo}  


`

type namedAggResult struct {
	aggResult
	Name string
}

func (n *namedAggResult) Aggregate(r Results, t *TimeRange, a Aggregator) Result {
	out := n.aggResult.Aggregate(r, t, a)
	out.Event = n.Name
	out.Fields = nil
	return out
}

type queryNode interface {
	noder
	Query(TimeRange) ScanQuery
}

func nodeQueries(dst ScanQueries, t *TimeRange, n interface{}) ScanQueries {
	switch n := n.(type) {
	case queryNode:
		return append(dst, n.Query(*t))
	case *namedAggResult:
		return nodeQueries(dst, t, n.aggResult)
	case *selectBlock:
		for _, n := range n.Select {
			dst = nodeQueries(dst, t, n)
		}
		return dst
	case *aggNode:
		for _, n := range n.Nodes {
			dst = nodeQueries(dst, t, n)
		}
		return dst
	case *opNode:
		dst = nodeQueries(dst, t, n.X)
		dst = nodeQueries(dst, t, n.Y)
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
