package evql

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
	"time"

	errors "golang.org/x/xerrors"
)

// func unquote(lit *ast.BasicLit) (string, error) {
// 	if lit.Kind == token.STRING {
// 		return strconv.Unquote(lit.Value)
// 	}
// 	return lit.Value, nil
// }

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

func parseClause(exp ast.Expr) (string, []ast.Expr) {
	if star, ok := exp.(*ast.StarExpr); ok {
		fn, args := parseCall(star.X)
		return strings.ToUpper(getName(fn)), args
	}
	return "", nil
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
	fn, err := parseString(exp)
	if err != nil {
		return nil, err
	}
	if a := NewAggregator(fn); a != nil {
		return a, nil
	}
	return nil, errors.Errorf("Invalid aggregator name: %q", fn)
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
