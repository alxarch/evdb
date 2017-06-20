package meter_test

import (
	"log"
	"testing"

	meter "github.com/alxarch/go-meter"
)

func Test_LabelDimensions(t *testing.T) {
	dims := meter.LabelDimensions("foo", "bar")
	log.Println(dims)
	dims = meter.LabelDimensions("foo")
	log.Println(dims)

}
