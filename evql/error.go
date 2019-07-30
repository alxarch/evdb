package evql

import (
	"fmt"
	"go/ast"
	"go/token"

	errors "golang.org/x/xerrors"
)

type nodeError struct {
	ast.Node
	err error
}

func err(n ast.Node, err error) error {
	e := nodeError{
		Node: n,
		err:  err,
	}
	return &e
}

func (e *nodeError) Error() string {
	return fmt.Sprintf("Parse error at pos %d: %s", e.Pos(), e.err)
}

func (e *nodeError) ParseError(fset *token.FileSet) error {
	pos := fset.Position(e.Pos())
	return errors.Errorf("Parse error at %d.%d: %s", pos.Line, pos.Column, e.err)
}

func errorf(n ast.Node, msg string, args ...interface{}) error {
	return err(n, errors.Errorf(msg, args...))
}
