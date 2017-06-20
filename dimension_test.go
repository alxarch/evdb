package meter_test

import (
	"testing"

	meter "github.com/alxarch/go-meter"
	"github.com/stretchr/testify/assert"
)

func Test_LabelDimensions(t *testing.T) {
	dims := meter.LabelDimensions("bar", "foo")
	assert.Equal(t, dims, []meter.Dimension{
		meter.Dim("bar"),
		meter.Dim("bar", "foo"),
		meter.Dim("foo"),
	})
	dims = meter.LabelDimensions("foo")
	assert.Equal(t, dims, []meter.Dimension{
		meter.Dim("foo"),
	})
	dims = meter.LabelDimensions()
	assert.Equal(t, len(dims), 0)

}
