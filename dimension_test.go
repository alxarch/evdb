package meter_test

import (
	"log"
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
	log.Println(dims)

}
