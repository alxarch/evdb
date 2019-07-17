package mdbhttp_test

import (
	meter "github.com/alxarch/go-meter/v2"
	"github.com/alxarch/go-meter/v2/mdbhttp"
)

var _ meter.Evaler = (*mdbhttp.Querier)(nil)
