package meter

import (
	"go/ast"
	"go/parser"
	"testing"
)

// func Test_Eval(t *testing.T) {
// 	e, err := ParseEval("a + (b + c)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	events := e.Events()
// 	if len(events) != 3 {
// 		t.Errorf("Invalid events %v", events)
// 		return

// 	}
// 	if events[0] != "a" {
// 		t.Errorf("Invalid events %v", events)
// 	}
// 	if events[1] != "b" {
// 		t.Errorf("Invalid events %v", events)
// 	}
// 	if events[2] != "c" {
// 		t.Errorf("Invalid events %v", events)
// 	}

// }

// // Reset resets a result
// func (r *ScanResult) Reset() {
// 	*r = ScanResult{
// 		Fields: r.Fields.Reset(),
// 		Data:   r.Data.Reset(),
// 	}
// }

// func (results ScanResults) Reset() ScanResults {
// 	for i := range results {
// 		r := &results[i]
// 		r.Reset()
// 	}
// 	return results[:0]
// }

// func normalizeQuery(s string) string {
// 	// Join lines
// 	s = strings.ReplaceAll(s, "\n", " ")
// 	// Wrap in eval() call to allow comma separating
// 	s = fmt.Sprintf("eval(%s)", s)
// 	return s
// }

// func firstExp(exp ...ast.Expr) ast.Expr {
// 	if len(exp) == 1 {
// 		return exp[0]
// 	}
// 	return nil

// }

// func (q *QueryPlanner) callid(exp ast.Expr) (string, []ast.Expr, error) {
// 	call, ok := exp.(*ast.CallExpr)
// 	if !ok {
// 		return "", nil, errors.Errorf("Invalid call expression %s", exp)
// 	}
// 	id, ok := call.Fun.(*ast.Ident)
// 	if !ok {
// 		return "", nil, errors.Errorf("Invalid call expression %s", call.Fun)
// 	}
// 	return id.Name, call.Args, nil
// }

// func (e *Eval2) parseRoot(exp ast.Expr) error {
// 	switch exp := exp.(type) {
// 		case *ast.Ident:
// 			switch strings.ToLower(fun.Name) {
// 			case "eval":
// 				for _, arg := range exp.Args {
// 					if err := e.parseRoot(arg); err != nil {
// 						return err
// 					}
// 				}
// 				return nil
// 			default:
// 				return errors.Errorf("Invalid root call expr %q %s", fun.Name, exp)
// 				// TODO: root more modifiers
// 			}
// 		default:
// 			return errors.Errorf("Invalid root call expr %s", exp)
// 		}
// 	case *ast.ParenExpr:
// 		return e.parseRoot(exp.X)
// 	case *ast.BinaryExpr:
// 		if m := mergeOp(exp.Op); m == nil {
// 			return errors.Errorf("Invalid result operator %q", exp.Op)
// 		}
// 		if err := e.parseRoot(exp.X); err != nil {
// 			return err
// 		}
// 		if err := e.parseRoot(exp.Y); err != nil {
// 			return err
// 		}
// 		return nil
// 	default:
// 		return errors.Errorf("Invalid roor expression %s", exp)
// 	}

// }

//
//
//
//
//
//

// func ParseQuery(src string) (*QueryPlanner, error) {
// 	q := new(QueryPlanner)
// 	if err := q.Reset(src); err != nil {
// 		return nil, err
// 	}
// 	return q, nil
// }

func Test_Syntax(t *testing.T) {
	src := `
	foo.bar()
	`
	_ = `
	eval{
		bar: "baz" | "bar" | "baz",
	}[start:end:foo]( foo/bar )`
	exp, err := parser.ParseExpr(src)
	if err != nil {
		t.Fatal("failed to parse", err)
	}
	switch exp := exp.(type) {
	case *ast.CallExpr:
		switch fun := exp.Fun.(type) {
		case *ast.SliceExpr:
			t.Errorf("slice %v", fun)
		default:
			t.Errorf("WTFun %v", exp.Fun)

		}
	default:
		t.Error("WTF", exp)

	}

}

// func Test_Parser(t *testing.T) {
// 	src := `
// 	&scan{
// 		bar: "baz" | "bar" | "baz",
// 	}[now:now:"1s"](
// 		foo / bar,
// 	)
// 	`
// 	q, err := ParseQuery(src)
// 	if err != nil {
// 		t.Fatalf("Parse failed %s", err)

// 	}
// 	if len(q.index) != 2 {
// 		t.Errorf("Invalid queries size %d", len(q.index))
// 	}
// 	t.Error(q)

// }

// func (e *Eval2) parseDuration(exp ast.Expr) (time.Duration, error) {
// 	const (
// 		day   = 24 * time.Hour
// 		week  = 7 * day
// 		month = 7 * day
// 	)
// 	switch exp := exp.(type) {
// 	case *ast.BasicLit:
// 		switch exp.Kind {
// 		case token.INT:
// 			n, err := strconv.ParseInt(exp.Value, 10, 64)
// 			if err != nil {
// 				return 0, err
// 			}
// 			return time.Duration(n), nil
// 		case token.STRING:
// 			t, err := ParseTime(exp.Value)
// 			if err != nil {
// 				return 0, err
// 			}
// 			return time.Duration(t.UnixNano()), nil
// 		default:
// 			return 0, errors.Errorf("Invalid literal %q", exp.Value)
// 		}

// 	case *ast.BinaryExpr:
// 		x, err := e.parseDuration(exp.X)
// 		if err != nil {
// 			return 0, err
// 		}

// 		y, err := e.parseDuration(exp.Y)
// 		if err != nil {
// 			return 0, err
// 		}
// 		switch exp.Op {
// 		case token.ADD:
// 			return x + y, nil
// 		case token.SUB:
// 			return x - y, nil
// 		case token.MUL:
// 			return x * y, nil
// 		case token.QUO:
// 			return x / y, nil
// 		case token.REM:
// 			return x % y, nil
// 		default:
// 			return 0, errors.Errorf("Unsupported duration operator %q", exp.Op)
// 		}

// 	case *ast.Ident:
// 		switch name := strings.ToLower(exp.Name); name {
// 		case "lastmonth":
// 			return time.Duration(e.now.AddDate(0, -1, 0).UnixNano()), nil
// 		case "lastweek":
// 			return time.Duration(e.now.AddDate(0, 0, -7).UnixNano()), nil
// 		case "yesterday":
// 			return time.Duration(e.now.AddDate(0, 0, -1).UnixNano()), nil
// 		case "now":
// 			return time.Duration(e.now.UnixNano()), nil
// 		case "month", "months":
// 			return month, nil
// 		case "week", "w", "weeks":
// 			return week, nil
// 		case "day", "d", "days":
// 			return day, nil
// 		case "hour", "h", "hours":
// 			return time.Hour, nil
// 		case "minute", "min", "minutes":
// 			return time.Minute, nil
// 		case "seconds", "sec", "second":
// 			return time.Second, nil
// 		}

// 	}
// }

// func (e *Eval2) parseEvalQuery(q *EvalQuery, slice *ast.SliceExpr) error {
// 	match, ok := slice.X.(*ast.CompositeLit)
// 	if !ok {
// 		return errors.Errorf("Invalid match expression: %s", slice.X)
// 	}
// 	// match{...}
// 	id, ok := match.Type.(*ast.Ident)
// 	if !ok {
// 		return errors.Errorf("Invalid match type %s", slice)
// 	}
// 	if id.Name != "match" {
// 		return errors.Errorf("Invalid match type id %s", id)
// 	}
// 	for _, el := range match.Elts {
// 		var err error
// 		q.Match, err = e.parseMatch(q.Match, el)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	if low := slice.Low; low != nil {
// 		tm, err := e.parseTime(low)
// 		if err != nil {
// 			return err
// 		}
// 		q.Start = tm
// 	}
// 	if hi := slice.High; hi != nil {
// 		tm, err := e.parseTime(hi)
// 		if err != nil {
// 			return err
// 		}
// 		q.End = tm
// 	}
// 	if max := slice.Max; max != nil {
// 		step, err := e.parseDuration(max)
// 		if err != nil {
// 			return err
// 		}
// 		q.Step = step
// 	}
// 	return nil
// }
