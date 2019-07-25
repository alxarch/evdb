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
	root blockNode
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
	scan := blockOptions{
		Agg: aggSum{},
	}
	root, err := p.parseBlock(scan, body.List...)
	if err != nil {
		return err
	}
	p.root = root
	return nil
}

type blockOptions struct {
	Group  []string
	Empty  string
	Agg    Aggregator
	Offset time.Duration
	Match  Fields
}

func (p *Parser) parseGroupOptions(opts blockOptions, args ...ast.Expr) (zero blockOptions, err error) {
	labels := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg := arg.(type) {
		case *ast.KeyValueExpr:
			switch key := getName(arg.Key); strings.ToLower(key) {
			case "agg":
				agg, err := parseAggregator(arg.Value)
				if err != nil {
					return zero, p.Error(arg.Value, err)
				}
				opts.Agg = agg
			case "empty":
				empty, err := parseString(arg.Value)
				if err != nil {
					return zero, p.Error(arg.Value, err)
				}
				opts.Empty = empty
			default:
				return zero, p.Errorf(arg.Key, "Invalid keyrord argument: %q", key)
			}
		default:
			v, err := parseString(arg)
			if err != nil {
				return zero, p.Errorf(arg, "Invalid group label arg: %s", err)
			}
			labels = append(labels, v)
		}
	}
	opts.Group = labels
	return opts, nil
}

func (p *Parser) parseBlockOption(opts blockOptions, op token.Token, exp ast.Expr) (zero blockOptions, err error) {
	fn, args := parseCall(exp)
	name := getName(fn)
	switch strings.ToLower(name) {
	// case "mode":
	// 	if len(args) == 1 {
	// 		var mode string
	// 		mode, err = parseString(args[0])
	// 		if err != nil {
	// 			err = p.Error(args[0], err)
	// 			return
	// 		}
	// 		switch strings.ToLower(mode) {
	// 		case "scan":
	// 			opts.Mode = modeScan
	// 		case "eval":
	// 			opts.Mode = modeEval
	// 		default:
	// 			err = p.Errorf(args[0], "Invalid mode %q", mode)
	// 			return
	// 		}
	// 		return opts, nil
	// 	}
	// 	err = p.Errorf(exp, "Invalid mode arg")
	case "match":
		if op == token.MUL && opts.Match != nil {
			err = p.Errorf(exp, "Duplicate *%s option", name)
			return
		}
		opts.Match, err = p.parseMatchArgs(nil, args...)
	case "offset":
		// if opts.Offset != 0 {
		// 	err = p.Errorf(exp, "Duplicate match arg")
		// }
		if len(args) != 2 {
			err = p.Errorf(exp, "Invalid offset args")
		} else {
			opts.Offset, err = p.parseScanOffset(args[0], args[1])
		}
	case "group", "by", "groupBy":
		if op == token.MUL {
			if opts.Group != nil {
				err = p.Errorf(exp, "Duplicate *%s option", name)
				return
			}
			opts, err = p.parseGroupOptions(opts, args...)
			break
		}
		fallthrough
	default:
		err = p.Errorf(fn, "Invalid block option %s%s", op, name)
	}
	if err != nil {
		return
	}
	return opts, nil
}
func (p *Parser) parseBlockOptions(opts blockOptions, stmts ...ast.Stmt) (zero blockOptions, err error) {
	for _, stmt := range stmts {
		exp, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		star, ok := exp.X.(*ast.StarExpr)
		if !ok {
			continue
		}
		zero, err = p.parseBlockOption(zero, token.MUL, star.X)
		if err != nil {
			return
		}
	}
	if zero.Group == nil {
		zero.Group = opts.Group
	}
	if zero.Match == nil {
		zero.Match = opts.Match
	}
	if zero.Empty == "" {
		zero.Empty = opts.Empty
	}
	if zero.Agg == nil {
		zero.Agg = opts.Agg
	}
	if zero.Offset == 0 {
		zero.Offset = opts.Offset
	}
	return
}

func (opts blockOptions) GroupNode() groupNode {
	agg := opts.Agg
	if agg == nil {
		agg = aggSum{}
	}
	return groupNode{
		Group: opts.Group,
		Empty: opts.Empty,
		Agg:   agg,
	}
}
func (p *Parser) parseBlock(opts blockOptions, stmts ...ast.Stmt) (b blockNode, err error) {
	opts, err = p.parseBlockOptions(opts, stmts...)
	if err != nil {
		return
	}
	group := opts.GroupNode()
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *ast.BlockStmt:
			var child blockNode
			child, err = p.parseBlock(opts, stmt.List...)
			if err != nil {
				return
			}
			b = append(b, child)
		case *ast.ExprStmt:
			var x interface{}
			x, err = p.parseExpr(opts, stmt.X)
			if err != nil {
				return
			}
			switch x := x.(type) {
			case *blockOptions:
				opts = *x
			case time.Duration:
				opts.Offset = x
			case aggResult:
				if opts.Group != nil {
					group.Nodes = append(group.Nodes, x)
					continue
				}
				// Unwrap single scanResultNode into evaler
				a, ok := x.(*scanAggNode)
				if !ok {
					return nil, p.Errorf(stmt.X, "Aggregator expression without group clause")
				}
				s := a.scanResultNode
				e := scanEvalNode{s}
				b = append(b, &e)
			case evalNode:
				b = append(b, x)
			}
		default:
			return nil, p.Errorf(stmt, "Invalid stmt type: %s", reflect.TypeOf(stmt))
		}
	}
	if len(group.Nodes) > 0 {
		b = append(b, &group)
	}
	return

}

func (p *Parser) parseExpr(opts blockOptions, e ast.Expr) (interface{}, error) {
	switch e := e.(type) {
	case *ast.StarExpr:
		// Parsed at parseBlockOptions
		return nil, nil
	case *ast.UnaryExpr:
		switch e.Op {
		case token.ADD, token.SUB:
			opts, err := p.parseBlockOption(opts, e.Op, e.X)
			if err != nil {
				return nil, err
			}
			return &opts, nil
		case token.NOT:
			return p.parseAggExpr(opts, e)
		default:
			return nil, p.Errorf(e, "Invalid block expr")
		}
	default:
		// if opts.Group == nil {
		// 	return p.parseScanResult(opts, e)
		// }
		return p.parseAggResult(opts, e)
	}
}

func (p *Parser) parseValueNode(opts blockOptions, exp *ast.BasicLit) (*valueNode, error) {
	v := valueNode{
		Offset: opts.Offset,
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
	default:
		return nil, nil
	}
}

func (p *Parser) parseAggResult(opts blockOptions, e ast.Expr) (aggResult, error) {
	switch exp := e.(type) {
	case *ast.ParenExpr:
		// if opts.Group == nil {
		// 	return nil, p.Errorf(exp, "Cannot use arithmetic expr without group")
		// }
		return p.parseAggResult(opts, exp.X)
	case *ast.UnaryExpr:
		return p.parseAggExpr(opts, exp)
	case *ast.BasicLit:
		// if opts.Group == nil {
		// 	return nil, p.Errorf(exp, "Cannot use scalar values without group")
		// }
		return p.parseValueNode(opts, exp)
	case *ast.BinaryExpr:
		// if opts.Group == nil {
		// 	return nil, p.Errorf(exp, "Cannot use operands without group")
		// }
		m := mergeOp(exp.Op)
		if m == nil {
			return nil, p.Errorf(exp, "Invalid result operation %q", exp.Op)
		}
		x, err := p.parseAggResult(opts, exp.X)
		if err != nil {
			return nil, err
		}
		y, err := p.parseAggResult(opts, exp.Y)
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
		s, err := p.parseScanResult(opts, e)
		if err != nil {
			return nil, err
		}
		return &scanAggNode{s}, nil
	}

}
func (p *Parser) parseScanResult(opts blockOptions, exp ast.Expr) (*scanResultNode, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		d, err := p.parseScanOffset(exp.Low, exp.High)
		if err != nil {
			return nil, err
		}
		opts.Offset = d
		return p.parseScanResult(opts, exp.X)
	// case *ast.UnaryExpr:
	// 	return p.parseAggResultExpr(opts, exp)

	case *ast.CompositeLit:
		var err error
		opts.Match, err = p.parseMatch(opts.Match, token.ADD, exp.Elts...)
		if err != nil {
			return nil, err
		}
		return p.parseScanResult(opts, exp.Type)
	case *ast.Ident:
		return &scanResultNode{
			Event:  exp.Name,
			Match:  opts.Match,
			Offset: opts.Offset,
		}, nil
	default:
		return nil, p.Errorf(exp, "Invalid scan result")
	}
}

func (p *Parser) parseAggExpr(opts blockOptions, exp *ast.UnaryExpr) (aggResult, error) {
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
		n, err := p.parseAggResult(opts, arg)
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
		Offset: opts.Offset,
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

func nodeQueries(dst ScanQueries, t *TimeRange, n interface{}) ScanQueries {
	switch n := n.(type) {
	case *scanAggNode:
		return append(dst, n.Query(*t))
	case *scanEvalNode:
		return append(dst, n.Query(*t))
	case *scanResultNode:
		return append(dst, n.Query(*t))
	case blockNode:
		for _, n := range n {
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
