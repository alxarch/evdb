package evql

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	db "github.com/alxarch/evdb"
	errors "golang.org/x/xerrors"
)

// Parser parses queries
type Parser struct {
	fset *token.FileSet
	root evalNode
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
		if err, ok := err.(*nodeError); ok {
			return err.ParseError(fset)
		}
		return err
	}
	p.root = blockNode(root.Select)
	return nil
}

func (p *Parser) parseClause(s, parent *selectBlock, op token.Token, exp ast.Expr) (err error) {
	fn, args := parseCall(exp)
	clause := getName(fn)
	switch strings.ToUpper(clause) {
	case "SELECT":
		if op != token.MUL {
			return errorf(exp, "Invalid SELECT clause %s%s", op, clause)
		}
		return nil
	case "WHERE":
		if s.Match != nil {
			return errorf(exp, "Duplicate WHERE clause %s%s", op, clause)
		}
		switch op {
		case token.ADD:
			m := parent.Match.Copy()
			s.Match, err = p.parseMatchArgs(m, args...)
		// case token.SUB:
		// 	m := parent.Match.Copy()
		// 	del, err := parseStrings(args...)
		// 	if err != nil {
		// 		return p.Error(exp, err)
		// 	}
		// 	for _, label := range del {
		// 		delete(m, label)
		// 	}
		// 	s.Match = m
		case token.MUL:
			s.Match, err = p.parseMatchArgs(nil, args...)
		default:
			return errorf(exp, "Invalid WHERE clause %s%s", op, clause)
		}
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
				return errorf(fn, "Invalid clause %s%s", op, clause)
			}
			return nil

		}
		return errorf(exp, "Invalid OFFSET clause")
	// case "GROUP":
	// 	if s.Agg != nil {
	// 		return p.Errorf(exp, "Duplicate GROUP clause")
	// 	}
	// 	if op != token.MUL || len(args) != 1 {
	// 		return p.Errorf(exp, "Invalid GROUP clause")
	// 	}
	// 	s.Agg, err = parseAggregator(args[0])

	case "BY", "GROUP", "GROUPBY":
		if s.Group != nil {
			return errorf(exp, "Duplicate GROUP clause %s%s", op, clause)
		}
		if op != token.MUL {
			return errorf(exp, "Invalid BY clause")
		}
		return p.parseGroupClause(s, args...)
	default:
		return errorf(fn, "Invalid clause %s%s", op, clause)
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
			b.Select = append(b.Select, blockNode(child.Select))
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
			return nil, errorf(stmt, "Invalid block statement")
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
		return errorf(star, "Duplicate SELECT clause")
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
	a, err := p.parseResult(s.Agg, s, e)
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
		return errorf(e, "Invalid aggregate expresion without GROUP clause")
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

func (p *Parser) parseResult(agg Aggregator, s *selectBlock, e ast.Expr) (aggResult, error) {
	switch exp := e.(type) {
	case *ast.ParenExpr:
		return p.parseResult(agg, s, exp.X)
	case *ast.UnaryExpr:
		return p.parseAggExpr(agg, s, exp)
	case *ast.BasicLit:
		return p.parseValueNode(s, exp)
	case *ast.BinaryExpr:
		m := NewMerger(exp.Op)
		if m == nil {
			return nil, errorf(exp, "Invalid result operation %q", exp.Op)
		}
		x, err := p.parseResult(agg, s, exp.X)
		if err != nil {
			return nil, err
		}
		y, err := p.parseResult(agg, s, exp.Y)
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
		scan, err := p.parseScanResult(s, e)
		if err != nil {
			return nil, err
		}
		return &scanAggNode{agg, scan}, nil
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
		scan.Match = scan.Match.Merge(m)
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

func (p *Parser) parseAggExpr(a Aggregator, s *selectBlock, exp *ast.UnaryExpr) (aggResult, error) {
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
			n, err := p.parseResult(a, s, arg)
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
		a, err := p.parseResult(agg, s, args[0])
		if err != nil {
			return nil, err
		}
		n := aggNode{
			Agg:       agg,
			aggResult: a,
		}
		return &n, nil
	}
	scan, err := p.parseScanResult(s, args[0])
	if err != nil {
		return nil, err
	}
	n := scanAggNode{
		Agg:            agg,
		scanResultNode: scan,
	}
	return &n, nil
}

func (p *Parser) Queries(t db.TimeRange) []db.ScanQuery {
	return nodeQueries(nil, &t, p.root)
}

// func (p *Parser) Error(exp ast.Node, err error) error {
// 	// panic(err)
// 	pos := p.fset.Position(exp.Pos())
// 	return errors.Errorf(`Parse error at position %s: %s`, pos, err)
// }

// func (p *Parser) Errorf(exp ast.Node, msg string, args ...interface{}) error {
// 	return p.Error(exp, errors.Errorf(msg, args...))
// }

func (p *Parser) parseScanOffset(lo, hi ast.Expr) (time.Duration, error) {
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

func (p *Parser) parseMatchAny(values []string, exp ast.Expr) ([]string, error) {
	switch exp := exp.(type) {
	case *ast.BinaryExpr:
		if exp.Op != token.OR {
			return nil, errorf(exp, "Invalid op %q", exp.Op)
		}
		var err error
		values, err = p.parseMatchAny(values, exp.X)
		if err != nil {
			return nil, err
		}
		return p.parseMatchAny(values, exp.Y)
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
		return nil, errorf(exp, "Invalid match any expression")
	}
}

func (p *Parser) parseMatcher(exp ast.Expr) (db.Matcher, error) {
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
		values, err := p.parseMatchAny(nil, exp)
		if err != nil {
			return nil, err
		}
		if len(values) == 1 {
			return db.MatchString(values[0]), nil
		}
		return db.MatchAny(values...), nil
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
		return 0, errorf(exp, "Invalid duration unit %q", exp.Name)
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
			return 0, errorf(exp, "Invalid duration operand %q", exp.Op)
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
			return 0, errorf(exp, "Invalid duration literal %s", exp.Kind)
		}
	default:
		return 0, errorf(exp, "Invalid duration expression")
	}
}

// Eval executes the query against some results
func (p *Parser) Eval(out []interface{}, t db.TimeRange, results db.Results) []interface{} {
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

func (p *Parser) parseMatch(m db.MatchFields, op token.Token, args ...ast.Expr) (db.MatchFields, error) {
	match, err := p.parseMatchArgs(m, args...)
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

func (p *Parser) parseMatchArgs(match db.MatchFields, args ...ast.Expr) (db.MatchFields, error) {
	for _, el := range args {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil, errorf(el, "Invalid match expr type %s", reflect.TypeOf(el))
		}
		label, err := parseString(kv.Key)
		if err != nil {
			return nil, errorf(kv.Key, "Failed to parse match label: %s", err)
		}
		m, err := p.parseMatcher(kv.Value)
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

// aggNode reduces a result to a value
type aggNode struct {
	aggResult
	Agg Aggregator
}

func (n *aggNode) node()         {}
func (n *aggNode) unwrap() noder { return n.aggResult }

func (n *aggNode) Aggregate(r db.Results, t *db.TimeRange) db.Result {
	a := n.aggResult.Aggregate(r, t)
	agg := BlankAggregator(n.Agg)
	v := AggregateData(a.Data, agg)
	a.Data.Fill(v)
	return a
}
