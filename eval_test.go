package meter

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	errors "golang.org/x/xerrors"
)

func Test_Eval(t *testing.T) {
	e, err := ParseEval("a + (b + c)")
	if err != nil {
		t.Fatal(err)
	}
	events := e.Events()
	if len(events) != 3 {
		t.Errorf("Invalid events %v", events)
		return

	}
	if events[0] != "a" {
		t.Errorf("Invalid events %v", events)
	}
	if events[1] != "b" {
		t.Errorf("Invalid events %v", events)
	}
	if events[2] != "c" {
		t.Errorf("Invalid events %v", events)
	}

}
func Test_EvalQuery(t *testing.T) {
	var (
		now       = time.Now()
		labels    = []string{"color", "taste"}
		snapshots = []Snapshot{
			{
				Time:   now.Add(-1 * time.Minute),
				Labels: labels,
				Counters: CounterSlice{
					{
						Values: []string{"red", "bitter"},
						Count:  42,
					},
					{
						Values: []string{"yellow", "bitter"},
						Count:  8,
					},
					{
						Values: []string{"red", "sweet"},
						Count:  64,
					},
				},
			},
			{
				Time:   now.Add(-1 * time.Second),
				Labels: labels,
				Counters: CounterSlice{
					{
						Values: []string{"red", "bitter"},
						Count:  42,
					},
					{
						Values: []string{"yellow", "bitter"},
						Count:  8,
					},
					{
						Values: []string{"yellow", "sour"},
						Count:  9,
					},
				},
			},
			{
				Time:   now,
				Labels: labels,
				Counters: CounterSlice{
					{
						Values: []string{"red", "bitter"},
						Count:  24,
					},
					{
						Values: []string{"yellow", "bitter"},
						Count:  11,
					},
					{
						Values: []string{"yellow", "sour"},
						Count:  100,
					},
					{
						Values: []string{"green", "sweet"},
						Count:  2,
					},
				},
			},
		}
		fooStore = new(MemoryStore)
		barStore = new(MemoryStore)
		store    = TeeStore(fooStore, barStore)
		scanners = ScannerIndex{
			"foo": fooStore,
			"bar": barStore,
		}
		querier = ScanQuerier(scanners)
		evaler  = QueryEvaler(querier)
		ctx     = context.Background()
		q       = Query{
			TimeRange: TimeRange{
				Start: now.Add(-1 * time.Hour),
				End:   now,
				Step:  time.Minute,
			},
			Match: Fields{
				{
					Label: "color",
					Value: "red",
				},
			},
		}
	)
	for i := range snapshots {
		s := &snapshots[i]
		if err := store.Store(s); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
	}
	{
		results, err := querier.Query(ctx, q, "foo", "bar")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(results) != 4 {
			t.Errorf("Invalid results size %d != 4", len(results))
		}
		if len(results[0].Data) != 2 {
			t.Error(results[0].Data)
		}

	}
	{
		results, err := evaler.Eval(ctx, q, "foo / bar")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(results) != 1 {
			t.Error(results)
		}
		if len(results[0].Data) != 2 {
			t.Errorf("Invalid results size %d != 2", len(results))
		}

	}

}

type ScanExpr struct {
	Match Fields
	TimeRange
	Event string
	Group []string
}

type QueryPlan struct {
	TimeRange
	Match  Fields
	Events []string
	ScanResults
}

func (tr TimeRange) NumSteps() int {
	start := tr.Start.Truncate(tr.Step)
	end := tr.End.Truncate(tr.Step)
	return int(end.Sub(start) / tr.Step)
}
func (tr TimeRange) Match(other TimeRange) bool {
	return tr.Step == other.Step && tr.NumSteps() == other.NumSteps()
}

type QueryPlans []QueryPlan

func (q *QueryPlanner) Scan(s *ScanExpr) *QueryPlan {
	for i := range q.plans {
		p := &q.plans[i]
		if p.TimeRange.Match(s.TimeRange) && p.Match.Equal(s.Match) {
			p.Events = appendDistinct(p.Events, s.Event)
			return p
		}
	}
	q.plans = append(q.plans, QueryPlan{
		TimeRange: s.TimeRange,
		Match:     s.Match.Copy(),
		Events:    []string{s.Event},
	})
	return &q.plans[len(q.plans)-1]
}

type QueryPlanner struct {
	fset  *token.FileSet
	group []string
	src   ast.Expr
	index map[ast.Expr]*QueryPlan
	plans QueryPlans
	eval  ast.Expr
}

func (s DataPoints) Reset() DataPoints {
	for i := range s {
		s[i] = DataPoint{}
	}
	return s[:0]
}

// Reset resets a result
func (r *ScanResult) Reset() {
	*r = ScanResult{
		Fields: r.Fields.Reset(),
		Data:   r.Data.Reset(),
	}
}

func (results ScanResults) Reset() ScanResults {
	for i := range results {
		r := &results[i]
		r.Reset()
	}
	return results[:0]
}

func (p *QueryPlan) Reset() {
	*p = QueryPlan{
		Match:       p.Match.Reset(),
		Events:      p.Events[:0],
		ScanResults: p.ScanResults.Reset(),
	}
}

func (plans QueryPlans) Reset() QueryPlans {
	for i := range plans {
		p := &plans[i]
		p.Reset()
	}
	return plans[:0]
}

func (q *QueryPlanner) Reset(src string) error {
	q.fset = token.NewFileSet()
	q.index = make(map[ast.Expr]*QueryPlan)
	q.plans = q.plans.Reset()
	root, err := parser.ParseExpr(src)
	if err != nil {
		return err
	}
	if err := q.parseRoot(root); err != nil {
		return err
	}

	return nil
}

func (s *ScanExpr) String() string {
	var (
		scratch []byte
		w       = new(strings.Builder)
	)
	scratch = strconv.AppendQuote(scratch[:0], s.Event)
	w.Write(scratch)
	if len(s.Match) > 0 {
		w.WriteByte('{')
		for i := range s.Match {
			if i > 0 {
				w.WriteByte(',')
			}
			f := &s.Match[i]
			scratch = strconv.AppendQuote(scratch[:0], f.Label)
			w.Write(scratch)
			w.WriteByte(':')
			scratch = strconv.AppendQuote(scratch[:0], f.Value)
			w.Write(scratch)
		}
		w.WriteByte('}')
	}
	w.WriteByte('[')
	scratch = s.Start.AppendFormat(scratch[:0], time.RFC3339Nano)
	w.Write(scratch)
	w.WriteByte(':')
	scratch = s.End.AppendFormat(scratch[:0], time.RFC3339Nano)
	w.Write(scratch)
	w.WriteByte(':')
	w.WriteString(s.Step.String())
	w.WriteByte(']')
	if len(s.Group) > 0 {
		w.WriteString(".by(")
		for i, g := range s.Group {
			if i > 0 {
				w.WriteByte(',')
			}
			scratch = strconv.AppendQuote(scratch[:0], g)
			w.Write(scratch)
		}
		w.WriteByte(')')
	}
	return w.String()
}

func normalizeQuery(s string) string {
	// Join lines
	s = strings.ReplaceAll(s, "\n", " ")
	// Wrap in eval() call to allow comma separating
	s = fmt.Sprintf("eval(%s)", s)
	return s
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

func (q *QueryPlanner) parseMatchValue(values []string, v ast.Expr) []string {
	switch v := v.(type) {
	case *ast.BinaryExpr:
		if v.Op != token.OR {
			q.Fatalf(v, "Invalid match expression concat %s", v.Op)
		}
		values = q.parseMatchValue(values, v.X)
		values = q.parseMatchValue(values, v.Y)
		return values
	default:
		s, err := q.string(v)
		if err != nil {
			q.Fatal(v, err)
		}
		return append(values, s)
	}
}

func (q *QueryPlanner) Errorf(exp ast.Expr, msg string, args ...interface{}) error {
	return q.Error(exp, errors.Errorf(msg, args...))
}

func (q *QueryPlanner) Fatal(exp ast.Expr, err error) error {
	panic(q.Error(exp, err))
}

func (q *QueryPlanner) Fatalf(exp ast.Expr, msg string, args ...interface{}) {
	panic(q.Errorf(exp, msg, args...))
}

func (q *QueryPlanner) parseMatch(match Fields, exp ast.Expr) Fields {
	kv, ok := exp.(*ast.KeyValueExpr)
	if !ok {
		q.Fatalf(exp, "Invalid match expression %s", exp)
	}
	key, err := q.string(kv.Key)
	if err != nil {
		q.Fatal(kv.Key, err)
	}
	values := q.parseMatchValue(nil, kv.Value)
	for _, v := range values {
		match = match.Add(Field{
			Label: key,
			Value: v,
		})
	}
	return match
}
func (q *QueryPlanner) Error(exp ast.Expr, err error) error {
	pos := q.fset.Position(exp.Pos())
	return errors.Errorf(`Parse error at position %s: %s`, pos, err)
}

func firstExp(exp ...ast.Expr) ast.Expr {
	if len(exp) == 1 {
		return exp[0]
	}
	return nil

}

func (q *QueryPlanner) duration(exp ast.Expr) (time.Duration, error) {
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

func (q *QueryPlanner) Now() time.Time {
	return time.Now()
}

func (q *QueryPlanner) time(exp ast.Expr) (time.Time, error) {
	switch exp := exp.(type) {
	case *ast.BasicLit:
		switch lit := exp; lit.Kind {
		case token.STRING:
			v, err := strconv.Unquote(lit.Value)
			if err != nil {
				return time.Time{}, q.Errorf(exp, "Invalid time string: %s", err)
			}
			if strings.ToLower(v) == "now" {
				return q.Now(), nil
			}
			return ParseTime(v)
		case token.INT:
			n, err := strconv.ParseInt(lit.Value, 10, 64)
			if err != nil {
				return time.Time{}, errors.Errorf("Invalid time value: %s", err)
			}
			return time.Unix(n, 0), nil
		default:
			return time.Time{}, errors.Errorf("Invalid time literal %s", exp)
		}
	case *ast.Ident:
		switch strings.ToLower(exp.Name) {
		case "now":
			return q.Now(), nil
		default:
			return time.Time{}, errors.Errorf("Invalid time ident %s", exp.Name)
		}
	}
	return time.Time{}, errors.Errorf("Invalid time expr %s", exp)
}

func (q *QueryPlanner) string(exp ast.Expr) (string, error) {
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

func (q *QueryPlanner) callid(exp ast.Expr) (string, []ast.Expr, error) {
	call, ok := exp.(*ast.CallExpr)
	if !ok {
		return "", nil, errors.Errorf("Invalid call expression %s", exp)
	}
	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return "", nil, errors.Errorf("Invalid call expression %s", call.Fun)
	}
	return id.Name, call.Args, nil
}

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
func (q *QueryPlanner) parseRoot(root ast.Expr) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if e, ok := p.(error); ok {
				err = e
			}
			err = errors.Errorf("Parse panic: %v", p)
		}
	}()
	_, err = q.parse(ScanExpr{}, root)
	return
}

type parseContext struct {
	Unary bool
}

func (q *QueryPlanner) strings(dst []string, exp ...ast.Expr) ([]string, error) {
	for _, exp := range exp {
		s, err := q.string(exp)
		if err != nil {
			return dst, nil
		}
		dst = append(dst, s)
	}
	return dst, nil
}

func (q *QueryPlanner) parseScan(scan ScanExpr, exp ast.Expr) (*ScanExpr, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		tr, x, err := q.timerange(scan.TimeRange, exp)
		if err != nil {
			return nil, q.Error(x, err)
		}
		scan.TimeRange = tr
		return q.parseScan(scan, exp.X)
	case *ast.IndexExpr:
		step, err := q.duration(exp)
		if err != nil {
			return nil, err
		}
		scan.Step = step
		return q.parseScan(scan, exp.X)
	case *ast.CompositeLit:
		scan := scan
		match := scan.Match.Copy()
		for _, el := range exp.Elts {
			match = q.parseMatch(match, el)
		}
		sort.Sort(match)
		scan.Match = match
		return q.parseScan(scan, exp.Type)
	case *ast.Ident:
		scan := scan
		scan.Event = exp.Name
		return &scan, nil
	default:
		return nil, q.Errorf(exp, "Invalid scan expression")
	}

}
func (q *QueryPlanner) parseCall(scan ScanExpr, call *ast.CallExpr) (ScanExpr, error) {
	var (
		s   *ScanExpr
		err error
	)
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		// .foo(...)
		switch strings.ToLower(fn.Sel.Name) {
		case "by":
			group, err := q.strings(nil, call.Args...)
			if err != nil {
				return scan, err
			}
			scan.Group = group
			// postfix op
			_, err = q.parse(scan, fn.X)
			return scan, err

		default:
			return scan, q.Errorf(fn.Sel, "Invalid postfix op: %q", fn.Sel.Name)
		}
	case *ast.SliceExpr:
		// foo[a:b](...)
		s, err = q.parseScan(scan, fn)
	case *ast.IndexExpr:
		// foo[a](...)
		s, err = q.parseScan(scan, fn)
	case *ast.Ident:
		// foo(...)
		s, err = q.parseScan(scan, fn)
	case *ast.CompositeLit:
		// foo{}(...)
		s, err = q.parseScan(scan, fn)
	default:
		return scan, q.Errorf(fn, "Invalid call expression")
	}
	if err != nil {
		return scan, err
	}
	if s != nil {
		scan = *s
	}
	for _, arg := range call.Args {
		_, err := q.parse(scan, arg)
		if err != nil {
			return scan, err
		}
	}
	return scan, nil
}

func (q *QueryPlanner) parse(scan ScanExpr, e ast.Expr) (ScanExpr, error) {
	_ = `
	&scan{
		foo: bar,
	}[a:b:c](
		win{
			bar: baz,
		},

	).by(

	)
	event1{label: v1|v2}[start:end:step].by(foo)
	|
	event2{label: v1|v2}[start:end:step].by(foo)

	`

	switch exp := e.(type) {
	case *ast.UnaryExpr:
		if exp.Op != token.AND {
			return scan, q.Errorf(exp, "Invalid root expression")
		}
		call, ok := exp.X.(*ast.CallExpr)
		if !ok {
			return scan, q.Errorf(exp, "Invalid root expression")
		}
		scan, err := q.parseCall(scan, call)
		if err != nil {
			return scan, err
		}
		if scan.Event != "scan" {
			return scan, q.Errorf(exp, "Invalid root expression")
		}
		scan.Event = ""
		return scan, nil
	case *ast.CallExpr:
		return q.parseCall(scan, exp)
	case *ast.Ident:
		s := scan
		s.Event = exp.Name
		q.index[e] = q.Scan(&s)
		return scan, nil
	case *ast.ParenExpr:
		return q.parse(scan, exp.X)
	case *ast.BinaryExpr:
		if m := mergeOp(exp.Op); m == nil {
			return scan, q.Errorf(exp, "Invalid operator %s", exp.Op)
		}
		var err error
		scanX, err := q.parse(scan, exp.X)
		if err != nil {
			return scan, err
		}
		scanY, err := q.parse(scan, exp.Y)
		if err != nil {
			return scan, err
		}
		_, _ = scanX, scanY
		return scan, nil
	default:
		return scan, errors.Errorf("Invalid exp %s", exp)

	}
}

func (q *QueryPlanner) timerange(tr TimeRange, exp ast.Expr) (TimeRange, ast.Expr, error) {
	switch exp := exp.(type) {
	case *ast.SliceExpr:
		slice := exp
		if low := slice.Low; low != nil {
			tm, err := q.time(low)
			if err != nil {
				return TimeRange{}, low, err
			}
			tr.Start = tm
		}
		if hi := slice.High; hi != nil {
			tm, err := q.time(hi)
			if err != nil {
				return TimeRange{}, hi, err
			}
			tr.End = tm
		}
		if max := slice.Max; max != nil {
			step, err := q.duration(max)
			if err != nil {
				return TimeRange{}, max, err
			}
			tr.Step = step
		}
		return tr, exp.X, nil
	case *ast.IndexExpr:
		d, err := q.duration(exp.Index)
		if err != nil {
			return TimeRange{}, exp.Index, err
		}
		tr.Step = d
		return tr, exp.X, nil
	default:
		return TimeRange{}, exp, errors.New("Invalid timerange expression")
	}
}

func ParseQuery(src string) (*QueryPlanner, error) {
	q := new(QueryPlanner)
	if err := q.Reset(src); err != nil {
		return nil, err
	}
	return q, nil
}

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

func Test_Parser(t *testing.T) {
	src := `
	&scan{
		bar: "baz" | "bar" | "baz",
	}[now:now:"1s"](
		foo / bar,
	)
	`
	q, err := ParseQuery(src)
	if err != nil {
		t.Fatalf("Parse failed %s", err)

	}
	if len(q.index) != 2 {
		t.Errorf("Invalid queries size %d", len(q.index))
	}
	t.Error(q)

}

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
