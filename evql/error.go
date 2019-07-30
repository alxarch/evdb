package evql

import (
	"go/ast"
	"go/token"

	errors "golang.org/x/xerrors"
)

type nodeError struct {
	ast.Node
	err error
}

func newError(n ast.Node, err error) error {
	e := nodeError{
		Node: n,
		err:  err,
	}
	return &e
}

func (e *nodeError) Error() string {
	return e.err.Error()
}

func (e *nodeError) ParseError(fset *token.FileSet) error {
	pos := fset.Position(e.Pos())
	return errors.Errorf("Parse error at line %d, col %d: %s", pos.Line, pos.Column, e.err)
}

func errorf(n ast.Node, msg string, args ...interface{}) error {
	for i, arg := range args {
		if e, ok := arg.(*nodeError); ok {
			n = e.Node
			args[i] = e.err
			break
		}
	}
	return newError(n, errors.Errorf(msg, args...))
}
