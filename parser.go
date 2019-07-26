package meter

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
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

func (p *Parser) parseSelectResult(s *selectBlock, e ast.Expr) error {
	return nil
}

func parseClause(exp ast.Expr) (string, []ast.Expr) {
	if star, ok := exp.(*ast.StarExpr); ok {
		fn, args := parseCall(star.X)
		return strings.ToUpper(getName(fn)), args
	}
	return "", nil
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

// func (p *Parser) parseBlock(s *scanExpr, block *ast.BlockStmt) error {
// 	s = s.Child(block, kindBlock)
// 	for _, stmt := range block.List {
// 		if err := p.parseStmt(s, stmt); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

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

func (p *Parser) Queries(t TimeRange) ScanQueries {
	return nodeQueries(nil, &t, p.root)
}

func (tr *TimeRange) BlankData(v float64) DataPoints {
	start, end, step := tr.Start.Unix(), tr.End.Unix(), int64(tr.Step/time.Second)
	return fillData(v, start, end, step)
}
func fillData(v float64, start, end, step int64) (data DataPoints) {
	if step < 1 {
		step = 1
	}
	n := (end - start) / step
	if n < 0 {
		return
	}
	data = make([]DataPoint, n)
	for i := range data {
		ts := start + int64(i)*step
		data[i] = DataPoint{
			Timestamp: ts,
			Value:     v,
		}
	}
	return
}
func makeData(src DataPoints, start, end, step int64) (data DataPoints) {
	if step < 1 {
		step = 1
	}
	n := (end - start) / step
	if n < 0 {
		return
	}
	src = src.SeekRight(end)
	data = make([]DataPoint, n)
	for i := range data {
		ts := start + int64(i)*step
		src = src.SeekLeft(ts)
		v := src.ValueAt(ts)
		data[i] = DataPoint{
			Timestamp: ts,
			Value:     v,
		}
	}
	return
}

func parseDurationUnit(exp ast.Expr) time.Duration {
	if unit, ok := exp.(*ast.Ident); ok {
		return durationUnit(unit.Name)
	}
	return 0
}

func parseAggregator(exp ast.Expr) (Aggregator, error) {
	fn, err := parseString(exp)
	if err != nil {
		return nil, err
	}
	if a := newAgg(fn); a != nil {
		return a, nil
	}
	return nil, errors.Errorf("Invalid aggregator name: %q", fn)
}

func (p *Parser) Error(exp ast.Node, err error) error {
	// panic(err)
	pos := p.fset.Position(exp.Pos())
	return errors.Errorf(`Parse error at position %s: %s`, pos, err)
}

func (p *Parser) Errorf(exp ast.Node, msg string, args ...interface{}) error {
	return p.Error(exp, errors.Errorf(msg, args...))
}

func parseTime(exp ast.Expr, now time.Time) (time.Time, error) {
	if exp == nil {
		return now, nil
	}
	switch exp := exp.(type) {
	case *ast.BasicLit:
		switch lit := exp; lit.Kind {
		case token.STRING:
			v, err := strconv.Unquote(lit.Value)
			if err != nil {
				return time.Time{}, err
			}
			if strings.ToLower(v) == "now" {
				return now, nil
			}
			return time.Parse(time.RFC3339Nano, v)
		case token.INT:
			n, err := strconv.ParseInt(lit.Value, 10, 64)
			if err != nil {
				return time.Time{}, errors.Errorf("Invalid timestamp value: %s", err)
			}
			return time.Unix(n, 0), nil
		default:
			return time.Time{}, errors.Errorf("Invalid time literal %s", exp)
		}
	case *ast.Ident:
		switch strings.ToLower(exp.Name) {
		case "now":
			return now, nil
		default:
			return time.Time{}, errors.Errorf("Invalid time ident %s", exp.Name)
		}
	}
	return time.Time{}, errors.Errorf("Invalid time expr: %s", reflect.TypeOf(exp))
}

func parseStrings(dst []string, exp ...ast.Expr) ([]string, error) {
	for _, exp := range exp {
		s, err := parseString(exp)
		if err != nil {
			return nil, err
		}
		dst = append(dst, s)
	}
	return dst, nil
}

const y2k = 946684800

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

func parseString(exp ast.Expr) (string, error) {
	switch exp := exp.(type) {
	case *ast.Ident:
		return exp.Name, nil
	case *ast.BasicLit:
		switch exp.Kind {
		case token.STRING:
			return strconv.Unquote(exp.Value)
		default:
			return exp.Value, nil
		}
	default:
		return "", errors.Errorf("Invalid string expression %s", exp)
	}
}

// Eval executes the query against some results
func (p *Parser) Eval(out []interface{}, t TimeRange, results Results) []interface{} {
	if p.root != nil {
		out = p.root.Eval(out, &t, results)
	}
	return out
}

func getName(e ast.Expr) string {
	if e != nil {
		if id, ok := e.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""

}

func (p *Parser) print(x interface{}) (string, error) {
	w := new(strings.Builder)
	if err := printer.Fprint(w, p.fset, x); err != nil {
		return "", err
	}
	return w.String(), nil
}

func newAgg(s string) Aggregator {
	switch strings.ToLower(s) {
	case "count":
		return new(aggSum)
	case "sum":
		return new(aggSum)
	case "avg":
		return new(aggAvg)
	case "min":
		return new(aggMin)
	case "max":
		return new(aggMax)
	default:
		return nil
	}

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

// func (p *Parser) parseGroupNode(args ...ast.Expr) (*groupNode, error) {
// 	g := groupNode{}
// 	var labels []string
// 	for _, arg := range args {
// 		switch arg := arg.(type) {
// 		case *ast.KeyValueExpr:
// 			if err := p.parseGroupArg(&g, arg); err != nil {
// 				return nil, err
// 			}
// 		default:
// 			v, err := parseString(arg)
// 			if err != nil {
// 				return nil, p.Errorf(arg, "Invalid group label: %s", err)
// 			}
// 			labels = append(labels, v)
// 		}
// 	}
// 	g.Group = labels
// 	return &g, nil
// }

// func (p *Parser) parseGroupArg(g *groupNode, arg *ast.KeyValueExpr) error {
// 	key, ok := arg.Key.(*ast.Ident)
// 	if !ok {
// 		return p.Errorf(arg.Key, "Invalid keyword argument for group")
// 	}
// 	switch key.Name {
// 	// case "by":
// 	// 	v, err := p.parseMatchValues(nil, arg.Value)
// 	// 	if err != nil {
// 	// 		return p.Errorf(arg.Value, "Failed to parse %q argument: %s", "by", err)
// 	// 	}
// 	// 	g.Group = v
// 	// 	return nil
// 	// case "agg":
// 	// 	v, err := parseString(arg.Value)
// 	// 	if err != nil {
// 	// 		return p.Errorf(arg.Value, "Failed to parse agg argument: %s", err)
// 	// 	}
// 	// 	agg := newAgg(v)
// 	// 	if agg == nil {
// 	// 		return p.Errorf(arg.Value, "Invalid agg argument: %q", agg)
// 	// 	}
// 	// 	g.Agg = agg
// 	// 	return nil
// 	case "empty":
// 		v, err := parseString(arg.Value)
// 		if err != nil {
// 			return p.Errorf(arg.Value, "Failed to parse empty argument: %s", err)
// 		}
// 		g.Empty = v
// 		return nil
// 	default:
// 		return p.Errorf(arg, "Invalid keyord argument: %q", key.Name)
// 	}
// }

// // ParseTime parses time in various formats
// func ParseTime(v string) (time.Time, error) {
// 	if strings.Contains(v, ":") {
// 		if strings.Contains(v, ".") {
// 			return time.ParseInLocation(time.RFC3339Nano, v, time.UTC)
// 		}
// 		return time.ParseInLocation(time.RFC3339, v, time.UTC)
// 	}
// 	if strings.Contains(v, "-") {
// 		return time.ParseInLocation("2006-01-02", v, time.UTC)
// 	}
// 	n, err := strconv.ParseInt(v, 10, 64)
// 	if err != nil {
// 		return time.Time{}, err
// 	}
// 	return time.Unix(n, 0), nil
// }

// func (p *Parser) parseBlock(s *selectBlock, stmts ...ast.Stmt) (b blockNode, err error) {
// 	opts, err = p.parseBlockOptions(opts, stmts...)
// 	if err != nil {
// 		return
// 	}
// 	group := opts.GroupNode()
// 	for _, stmt := range stmts {
// 		switch stmt := stmt.(type) {
// 		case *ast.ExprStmt:
// 			var x interface{}
// 			x, err = p.parseExpr(opts, stmt.X)
// 			if err != nil {
// 				return
// 			}
// 			switch x := x.(type) {
// 			case *blockOptions:
// 				opts = *x
// 			case time.Duration:
// 				opts.Offset = x
// 			case aggResult:
// 				name, _ := p.print(stmt.X)
// 				if opts.Group != nil {
// 					n := namedAggResult{
// 						aggResult: x,
// 						Name:      name,
// 					}
// 					group.Nodes = append(group.Nodes, &n)
// 					continue
// 				}
// 				// Unwrap single scanResultNode into evaler
// 				a, ok := x.(*scanAggNode)
// 				if !ok {
// 					return nil, p.Errorf(stmt.X, "Aggregator expression without group clause")
// 				}
// 				s := a.scanResultNode
// 				e := scanEvalNode{s}
// 				b = append(b, &e)
// 			case evalNode:
// 				b = append(b, x)
// 			}
// 		default:
// 			return nil, p.Errorf(stmt, "Invalid stmt type: %s", reflect.TypeOf(stmt))
// 		}
// 	}
// 	if len(group.Nodes) > 0 {
// 		b = append(b, &group)
// 	}
// 	return

// }
