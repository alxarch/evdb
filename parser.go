package meter

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"time"

	errors "golang.org/x/xerrors"
)

// Parser parses queries
type Parser struct {
	fset  *token.FileSet
	body  *ast.BlockStmt
	now   time.Time
	state ScanExpr
	scans []ScanExpr
	index map[ast.Expr]int
}

func (p *Parser) Find(exp ast.Expr) *ScanExpr {
	if i := p.index[exp]; 0 <= i && i < len(p.scans) {
		return &p.scans[i]
	}
	return nil
}
func (p *Parser) Reset(query string) error {
	return p.ResetAt(query, time.Now())
}

func (p *Parser) ResetAt(query string, now time.Time) error {
	fset := token.NewFileSet()
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
		now:   now,
		fset:  fset,
		index: make(map[ast.Expr]int),
		body:  body,
	}

	return p.init()
}

func (p *Parser) init() error {
	for _, stmt := range p.body.List {
		if err := p.parseStmt(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) Queries() ScanQueries {
	queries := make([]ScanQuery, len(p.scans))
	for i := range p.scans {
		s := &p.scans[i]
		queries[i] = s.ScanQuery
	}
	return queries
}

type ScanExpr struct {
	ScanQuery
	Group   []string
	results Results
}

func makeData(src DataPoints, start, end, step int64) (data DataPoints) {
	if step < 1 {
		step = 1
	}
	n := (end - start) / step
	if n < 0 {
		return
	}
	src = src.Slice(start, end)
	if len(src) == 0 {
		return
	}
	data = make([]DataPoint, n)
	for i := range data {
		ts := start + int64(i)*step
		v := src.ValueAt(ts)
		data[i] = DataPoint{
			Timestamp: ts,
			Value:     v,
		}
	}
	return
}

func (s *ScanExpr) Results(results Results) Results {
	if s.results != nil {
		return s.results
	}
	start, end, step := s.Start.Unix(), s.End.Unix(), int64(s.Step/time.Second)
	for i := range results {
		r := &results[i]

		if r.Event != s.Event {
			continue
		}
		if !s.Match.Includes(r.Fields) {
			continue
		}
		data := makeData(r.Data, start, end, step)
		if data == nil {
			continue
		}
		var fields Fields
		if len(s.Group) > 0 {
			fields = r.Fields.GroupBy("", s.Group)
		} else {
			fields = r.Fields.Copy()
		}
		s.results = append(s.results, Result{
			Event:  s.Event,
			Fields: fields,
			Data:   data,
		})
	}
	if s.results == nil {
		s.results = Results{}
	}
	return s.results
}
func (s ScanExpr) Copy() ScanExpr {
	s.Match = s.Match.Copy()
	s.Group = append(([]string)(nil), s.Group...)
	return s
}

func (p *Parser) parseStmt(s ast.Stmt) error {
	switch s := s.(type) {
	case *ast.ExprStmt:
		return p.parseExpr(s.X)
	default:
		return p.Errorf(s, "Invalid statement type %s", reflect.TypeOf(s))
	}
	// case *ast.AssignStmt:
	// 	if s.Tok != token.ASSIGN {
	// 		return p.Errorf(s, "Invalid token %q", s.Tok)
	// 	}
	// 	if len(s.Lhs) != 1 {
	// 		return p.Errorf(s, "Invalid multiple assign")
	// 	}
	// 	if len(s.Rhs) != 1 {
	// 		return p.Errorf(s, "Invalid assign values")
	// 	}
	// 	switch name := getName(s.Lhs[0]); name {
	// 	case "", "_":
	// 		scan := p.state.Copy()
	// 		if err := p.parseScanExpr(&scan, s.Rhs[0]); err != nil {
	// 			return err
	// 		}
	// 		if scan.Event != "scan" {
	// 			return p.Errorf(s, "Invalid scan expression ")

	// 		}
	// 	default:
	// 		return p.parseExpr(s.Rhs[0])
	// 	}
}
func (p *Parser) parseScanExpr(s *ScanExpr, exp ast.Expr) error {
	switch exp := exp.(type) {
	case *ast.CallExpr:
		call := exp
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel == nil {
			return p.Errorf(exp, "Invalid scan expression")
		}
		switch sel.Sel.Name {
		case "by", "group":
			group, err := parseStrings(nil, call.Args...)
			if err != nil {
				return err
			}
			s.Group = group
		default:
			return p.Errorf(sel.Sel, "Unknown scan method: %q", sel.Sel.Name)
		}
		return p.parseScanExpr(s, sel.X)
	case *ast.SliceExpr:
		tr, x, err := parseTimeRange(s.TimeRange, exp, p.now)
		if err != nil {
			return p.Error(x, err)
		}
		s.TimeRange = tr
		return p.parseScanExpr(s, exp.X)
	case *ast.IndexExpr:
		step, err := parseDuration(exp)
		if err != nil {
			return err
		}
		s.Step = step
		return p.parseScanExpr(s, exp.X)
	case *ast.CompositeLit:
		for _, el := range exp.Elts {
			var err error
			s.Match, err = p.parseMatch(s.Match, el)
			if err != nil {
				return err
			}
		}
		return p.parseScanExpr(s, exp.Type)
	case *ast.Ident:
		s.Event = exp.Name
		return nil
	default:
		return p.Errorf(exp, "Invalid scan expression")
	}
}

func (p *Parser) Error(exp ast.Node, err error) error {
	pos := p.fset.Position(exp.Pos())
	return errors.Errorf(`Parse error at position %s: %s`, pos, err)
}

func (p *Parser) Errorf(exp ast.Node, msg string, args ...interface{}) error {
	return p.Error(exp, errors.Errorf(msg, args...))
}

// func (p *Parser) sprint(exp ast.Node) string {
// 	w := new(strings.Builder)
// 	printer.Fprint(w, p.fset, exp)
// 	return w.String()
// }

func (p *Parser) parseExpr(e ast.Expr) error {
	switch exp := e.(type) {
	case *ast.ParenExpr:
		return p.parseExpr(exp.X)
	case *ast.BinaryExpr:
		if err := p.parseExpr(exp.X); err != nil {
			return err
		}
		if err := p.parseExpr(exp.Y); err != nil {
			return err
		}
		return nil
	case *ast.CallExpr:
		call := exp
		switch fn := call.Fun.(type) {
		case *ast.SelectorExpr:
			// postfix to scan expr
			return p.parseScan(call)
		case *ast.Ident:
			// query function
			for _, arg := range call.Args {
				if err := p.parseExpr(arg); err != nil {
					return err
				}
			}
			return nil
		default:
			return p.Errorf(fn, "Invalid function call")
		}
	default:
		return p.parseScan(e)
	}
}

func (p *Parser) parseScan(exp ast.Expr) error {
	s := p.state.Copy()
	if err := p.parseScanExpr(&s, exp); err != nil {
		return err
	}
	switch s.Event {
	case "_":
		p.state = s
	default:
		p.index[exp] = len(p.scans)
		p.scans = append(p.scans, s)
	}
	return nil

}

// case *ast.Ident:
// 	s := p.state.Copy()
// 	s.Event = exp.Name
// 	p.index[]
// 		s := new(scanExpr)
// 		s.Event = exp.Name
// 		s.Match = scan.Match.Copy()
// 		s.TimeRange = scan.TimeRange
// 		s.Group = append(s.Group[:0], scan.Group...)
// 		s.query = p.registerScan(s.Event, s.TimeRange, s.Match)
// 		p.index[e] = s
// 		return scan, nil

// 	case *ast.ParenExpr:
// 		return p.parse(scan, exp.X)
// 	case *ast.BinaryExpr:
// 		var err error
// 		scanX, err := p.parse(scan, exp.X)
// 		if err != nil {
// 			return scan, err
// 		}
// 		scanY, err := p.parse(scan, exp.Y)
// 		if err != nil {
// 			return scan, err
// 		}
// 		_, _ = scanX, scanY
// 		return scan, nil
// 	default:
// 		return scan, p.Errorf(exp, "Invalid exp %s", reflect.TypeOf(exp))
// 	}
// }
// func (p *Parser) registerScan(event string, tr TimeRange, match Fields) (q *evalQuery) {
// 	if p.queries == nil {
// 		p.queries = make(map[TimeRange]*evalQuery)
// 	}
// 	if q = p.queries[tr]; q == nil {
// 		for t := range p.queries {
// 			if t.Contains(&tr) {
// 				q = p.queries[t]
// 				break
// 			}
// 		}
// 	}
// 	if p != nil {
// 		q.Match = q.Match.Merge(match...)
// 		q.Events = appendDistinct(q.Events, event)
// 		return
// 	}
// 	q = &evalQuery{
// 		TimeRange: tr,
// 		Match:     match.Copy(),
// 		Events:    []string{event},
// 	}
// 	p.queries[tr] = q
// 	return q
// }

func parseTime(exp ast.Expr, now time.Time) (time.Time, error) {
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
			return ParseTime(v)
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

func parseTimeRange(tr TimeRange, exp ast.Expr, now time.Time) (TimeRange, ast.Expr, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		slice := exp
		if low := slice.Low; low != nil {
			tm, err := parseTime(low, now)
			if err != nil {
				return TimeRange{}, low, err
			}
			tr.Start = tm
		}
		if hi := slice.High; hi != nil {
			tm, err := parseTime(hi, now)
			if err != nil {
				return TimeRange{}, hi, err
			}
			tr.End = tm
		}
		if max := slice.Max; max != nil {
			step, err := parseDuration(max)
			if err != nil {
				return TimeRange{}, max, err
			}
			tr.Step = step
		}
		return tr, exp.X, nil
	case *ast.IndexExpr:
		d, err := parseDuration(exp.Index)
		if err != nil {
			return TimeRange{}, exp.Index, err
		}
		tr.Step = d
		return tr, exp.X, nil
	default:
		return TimeRange{}, exp, errors.New("Invalid timerange expression")
	}
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
func (p *Parser) parseMatch(match Fields, exp ast.Expr) (Fields, error) {
	kv, ok := exp.(*ast.KeyValueExpr)
	if !ok {
		return nil, p.Errorf(kv, "Invalid match expression %s", reflect.TypeOf(kv))
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
	return match, nil
}

func parseDuration(exp ast.Expr) (time.Duration, error) {
	lit, ok := exp.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return 0, errors.New("Invalid duration expression")
	}
	v, err := strconv.Unquote(lit.Value)
	if err != nil {
		return 0, errors.Errorf("Invalid duration string: %s", err)
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, errors.Errorf("Failed to parse duration: %s", err)
	}
	return d, nil
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

// func (p *Parser) evalExp(exp ast.Expr) (interface{}, error) {
// 	if exp == nil {
// 		exp = p.root
// 	}
// 	switch exp := exp.(type) {
// 	case *ast.UnaryExpr:
// 		return p.Results(exp.X)
// 	case *ast.IndexExpr:
// 		return p.Results(exp.X)
// 	case *ast.SliceExpr:
// 		return p.Results(exp.X)
// 	case *ast.Ident:
// 		s := p.index[exp]
// 		if s != nil {
// 			return s.Result(), nil
// 		}
// 		return nil, nil
// 	case *ast.BinaryExpr:
// 		m := mergeOp(exp.Op)
// 		if m == nil {
// 			return nil, p.Errorf(exp, "Invalid op %q", exp.Op)
// 		}
// 		x, err := p.Results(exp.X)
// 		if err != nil {
// 			return nil, err
// 		}
// 		y, err := p.Results(exp.Y)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return mergeResults(m, x, y)
// 	default:
// 		return nil, nil
// 	}
// }

// Eval executes the query against some results
func (p *Parser) Eval(results Results) ([]interface{}, error) {
	var out []interface{}
	for _, stmt := range p.body.List {
		e, ok := stmt.(*ast.ExprStmt)
		if !ok {
			return nil, p.Errorf(stmt, "Invalid stmt")
		}
		x, err := p.evalExpr(e.X, results)
		if err != nil {
			return nil, err
		}
		if x != nil {
			out = append(out, x)
		}
	}
	return out, nil

}
func (p *Parser) evalExpr(exp ast.Expr, r Results) (interface{}, error) {
	if s := p.Find(exp); s != nil {
		return s.Results(r), nil
	}
	switch exp := exp.(type) {
	case *ast.BinaryExpr:
		m := mergeOp(exp.Op)
		if m == nil {
			return nil, p.Errorf(exp, "Invalid operator %s", exp.Op)
		}
		x, err := p.evalExpr(exp.X, r)
		if err != nil {
			return nil, err
		}
		y, err := p.evalExpr(exp.X, r)
		if err != nil {
			return nil, err
		}
		return mergeResults(m, x, y)
	case *ast.CallExpr:
		fn, ok := exp.Fun.(*ast.Ident)
		if !ok {
			return nil, p.Errorf(exp, "Invalid expression")
		}
		var args []interface{}
		for _, arg := range exp.Args {
			a, err := p.evalExpr(arg, r)
			if err != nil {
				return nil, err
			}
			args = append(args, a)
		}
		return applyFn(fn.Name, args)
	default:
		return nil, p.Errorf(exp, "Invalid expression")
	}

}
func applyFn(name string, args []interface{}) (interface{}, error) {
	return nil, errors.New("Not implemented")
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

// type exprError struct {
// 	exp ast.Expr
// 	err error
// }

// func (e *exprError) Expr() ast.Expr {
// 	return e.exp
// }
// func (e *exprError) Unwrap() error {
// 	return e.err
// }
// func newExpressionError(exp ast.Expr, err error) *exprError {
// 	return &exprError{exp, err}
// }
// func (e *exprError) Error() string {
// 	return fmt.Sprintf("Expression error: %s", e.err)
// }
