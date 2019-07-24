package meter

import (
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

// Parser parses queries
type Parser struct {
	fset *token.FileSet
	now  time.Time
	root *blockNode
}

func (p *Parser) Reset(query string, s TimeRange) error {
	return p.ResetAt(query, s, time.Now())
}

func (p *Parser) ResetAt(query string, s TimeRange, now time.Time) error {
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
		now:  now,
		fset: fset,
	}
	scan := scanNode{
		Agg:       aggSum{},
		TimeRange: s,
	}
	root, err := p.parseBlockNode(&scan, body.List...)
	if err != nil {
		return err
	}
	p.root = root
	return nil
}

func (p *Parser) parseBlockNode(s *scanNode, stmts ...ast.Stmt) (*blockNode, error) {
	b := blockNode{
		scanNode: *s,
	}
	var group *groupNode
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *ast.BlockStmt:
			n, err := p.parseBlockNode(&b.scanNode, stmt.List...)
			if err != nil {
				return nil, err
			}
			b.nodes = append(b.nodes, n)
			group = nil
		case *ast.ExprStmt:
			n, err := p.parseExprNode(&b.scanNode, stmt.X)
			if err != nil {
				return nil, err
			}
			switch n := n.(type) {
			case Fields:
				b.Match = n
			case TimeRange:
				b.TimeRange = n
			case *groupNode:
				group = n
				b.nodes = append(b.nodes, n)
			case resultNode:
				if group != nil {
					group.Nodes = append(group.Nodes, n)
					break
				}
				if e, ok := n.(evalNode); ok {
					b.nodes = append(b.nodes, e)
				}
			case evalNode:
				b.nodes = append(b.nodes, n)
			}
		default:
			return nil, p.Errorf(stmt, "Invalid stmt type: %s", reflect.TypeOf(stmt))
		}
	}
	return &b, nil

}

func (p *Parser) parseExprNode(s *scanNode, e ast.Expr) (interface{}, error) {
	switch e := e.(type) {
	case *ast.StarExpr:
		return p.parseUnaryExpr(s, token.MUL, e.X)
	case *ast.UnaryExpr:
		return p.parseUnaryExpr(s, e.Op, e.X)
	default:
		return p.parseResultExpr(s, e)
	}
}

func (p *Parser) parseUnaryExpr(s *scanNode, op token.Token, exp ast.Expr) (interface{}, error) {
	switch exp := exp.(type) {
	case *ast.CompositeLit:
		return p.parseCommandNode(s, op, exp.Type, exp.Elts...)
	case *ast.SliceExpr:
		return p.parseCommandNode(s, op, exp.X, exp.Low, exp.High, exp.Max)
	default:
		return nil, p.Errorf(exp, "Invalid unary expr")
	}
}
func (p *Parser) parseAggNode(s *scanNode, agg ast.Expr, args ...ast.Expr) (*aggNode, error) {
	name := getName(agg)
	a := aggNode{
		TimeRange: s.TimeRange,
		Agg:       newAgg(name),
	}
	if a.Agg == nil {
		return nil, p.Errorf(agg, "Invalid agg name %q", name)
	}
	for _, arg := range args {
		r, err := p.parseResultExpr(s, arg)
		if err != nil {
			return nil, err
		}
		a.Nodes = append(a.Nodes, r)
	}
	return &a, nil
}

func (p *Parser) parseValueNode(s *scanNode, exp *ast.BasicLit) (*valueNode, error) {
	v := valueNode{
		TimeRange: s.TimeRange,
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
func (s *scanNode) Query(event string) ScanQuery {
	return ScanQuery{
		Event:     event,
		TimeRange: s.TimeRange,
		Match:     s.Match,
	}
}
func (p *Parser) parseResultExpr(s *scanNode, exp ast.Expr) (resultNode, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		d, err := p.parseScanOffset(exp.Low, exp.High)
		if err != nil {
			return nil, err
		}
		s := *s
		s.TimeRange = s.Offset(d)
		return p.parseResultExpr(&s, exp.X)
	case *ast.CompositeLit:
		switch typ := exp.Type.(type) {
		case *ast.ArrayType:
			return p.parseAggNode(s, typ.Elt, exp.Elts...)
		case *ast.Ident:
			match, err := p.parseMatch(nil, exp.Elts...)
			if err != nil {
				return nil, err
			}

			s := *s
			s.Match = match.Merge(s.Match...)
			n := scanResultNode{
				ScanQuery: s.Query(typ.Name),
				Agg:       s.Agg,
			}
			return &n, nil
		default:
			return nil, p.Errorf(exp.Type, "Invalid composite type")
		}
	case *ast.ParenExpr:
		return p.parseResultExpr(s, exp.X)
	case *ast.BasicLit:
		return p.parseValueNode(s, exp)
	case *ast.BinaryExpr:
		m := mergeOp(exp.Op)
		if m == nil {
			return nil, p.Errorf(exp, "Invalid result operation %q", exp.Op)
		}
		x, err := p.parseResultExpr(s, exp.X)
		if err != nil {
			return nil, err
		}
		y, err := p.parseResultExpr(s, exp.Y)
		if err != nil {
			return nil, err
		}
		op := opNode{
			X:  x,
			Y:  y,
			Op: m,
		}
		return &op, nil
	case *ast.Ident:
		n := s.Result(exp.Name)
		return &n, nil
	default:
		return nil, p.Errorf(exp, "Invalid result expr")
	}
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

func nodeQueries(dst ScanQueries, n interface{}) ScanQueries {
	switch n := n.(type) {
	case *scanResultNode:
		return append(dst, n.ScanQuery)
	case *blockNode:
		for _, n := range n.nodes {
			dst = nodeQueries(dst, n)
		}
		return dst
	case *aggNode:
		for _, n := range n.Nodes {
			dst = nodeQueries(dst, n)
		}
		return dst
	case *opNode:
		dst = nodeQueries(dst, n.X)
		dst = nodeQueries(dst, n.Y)
		return dst
	case *groupNode:
		for _, n := range n.Nodes {
			dst = nodeQueries(dst, n)
		}
		return dst
	default:
		return dst
	}
}

func (p *Parser) Queries() ScanQueries {
	return nodeQueries(nil, &p.root)
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
func (p *Parser) parseAggregator(exp ast.Expr) (Aggregator, error) {
	fn := getName(exp)
	if a := newAgg(fn); a != nil {
		return a, nil
	}
	return nil, p.Errorf(exp, "Invalid aggregeator %q", fn)
}

func (p *Parser) Error(exp ast.Node, err error) error {
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
func (p *Parser) Eval(out []interface{}, results Results) []interface{} {
	if p.root != nil {
		out = p.root.Eval(out, results)
	}
	return out
}

// ParseTime parses time in various formats
func ParseTime(v string) (time.Time, error) {
	if strings.Contains(v, ":") {
		if strings.Contains(v, ".") {
			return time.ParseInLocation(time.RFC3339Nano, v, time.UTC)
		}
		return time.ParseInLocation(time.RFC3339, v, time.UTC)
	}
	if strings.Contains(v, "-") {
		return time.ParseInLocation("2006-01-02", v, time.UTC)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(n, 0), nil
}

func getName(e ast.Expr) string {
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
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

func aggData(a Aggregator, grouped Results, start, end, step int64) DataPoints {
	if step < 1 {
		step = 1
	}
	n := (end - start) / step
	if n < 0 {
		return nil
	}
	data := make([]DataPoint, n)
	_, avg := a.(*aggAvg)
	if avg {
		a = aggSum{}
	}
	for i := range data {
		p := data[i]
		v := a.Zero()
		for g := range grouped {
			r := &grouped[g]
			for j := range r.Data {
				pp := &r.Data[j]
				v = a.Aggregate(v, pp.Value)
			}
		}
		if avg {
			p.Value = v / float64(len(grouped))
		} else {
			p.Value = v
		}
	}
	return data
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

func (p *Parser) parseMatchNode(m Fields, op token.Token, args ...ast.Expr) (Fields, error) {
	match, err := p.parseMatch(m, args...)
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

func (p *Parser) parseMatch(match Fields, args ...ast.Expr) (Fields, error) {
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

func (p *Parser) parseGroupNode(s *scanNode, args ...ast.Expr) (*groupNode, error) {
	g := groupNode{
		scanNode: *s,
	}
	var labels []string
	for _, arg := range args {
		switch arg := arg.(type) {
		case *ast.KeyValueExpr:
			if err := p.parseGroupArg(&g, arg); err != nil {
				return nil, err
			}
		default:
			v, err := parseString(arg)
			if err != nil {
				return nil, p.Errorf(arg, "Invalid group label: %s", err)
			}
			labels = append(labels, v)
		}
	}
	g.Group = labels
	return &g, nil
}

func (p *Parser) parseCommandNode(s *scanNode, op token.Token, cmd ast.Expr, args ...ast.Expr) (interface{}, error) {
	name := getName(cmd)
	switch strings.ToLower(name) {
	case "group", "by", "groupby":
		n, err := p.parseGroupNode(s, args...)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "match":
		match, err := p.parseMatchNode(s.Match, op, args...)
		if err != nil {
			return nil, err
		}
		return match, nil
	case "offset":
		if len(args) < 2 {
			return nil, p.Errorf(cmd, "Invalid offset args")
		}
		d, err := p.parseScanOffset(args[0], args[1])
		if err != nil {
			return nil, err
		}
		return s.Offset(d), nil
	default:
		return nil, p.Errorf(cmd, "Invalid command %q", name)
	}
}

func (p *Parser) parseGroupArg(g *groupNode, arg *ast.KeyValueExpr) error {
	key, ok := arg.Key.(*ast.Ident)
	if !ok {
		return p.Errorf(arg.Key, "Invalid keyword argument for group")
	}
	switch key.Name {
	// case "by":
	// 	v, err := p.parseMatchValues(nil, arg.Value)
	// 	if err != nil {
	// 		return p.Errorf(arg.Value, "Failed to parse %q argument: %s", "by", err)
	// 	}
	// 	g.Group = v
	// 	return nil
	case "agg":
		v, err := parseString(arg.Value)
		if err != nil {
			return p.Errorf(arg.Value, "Failed to parse agg argument: %s", err)
		}
		agg := newAgg(v)
		if agg == nil {
			return p.Errorf(arg.Value, "Invalid agg argument: %q", agg)
		}
		g.Agg = agg
		return nil
	case "empty":
		v, err := parseString(arg.Value)
		if err != nil {
			return p.Errorf(arg.Value, "Failed to parse empty argument: %s", err)
		}
		g.Empty = v
		return nil
	default:
		return p.Errorf(arg, "Invalid keyord argument: %q", key.Name)
	}
}
