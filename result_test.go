package evdb_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	db "github.com/alxarch/evdb"
	"github.com/alxarch/evdb/internal/assert"
)

func Test_Results(t *testing.T) {
	var results db.Results
	now := time.Now().Unix()
	results = results.Add("foo", db.Fields{
		{Label: "foo", Value: "bar"},
	}, now, 42)
	assert.Equal(t, len(results), 1)
	results = results.Add("foo", db.Fields{
		{Label: "foo", Value: "bar"},
	}, now, 42)
	assert.Equal(t, len(results), 1)
	results = results.Add("foo", db.Fields{
		{Label: "foo", Value: "baz"},
	}, now, 42)
	assert.Equal(t, len(results), 2)

	data, err := json.Marshal(&results[0])
	assert.NoError(t, err)
	s := fmt.Sprintf(`{"time":[0,0,0],"event":"foo","fields":{"foo":"bar"},"data":[[%d,84]]}`, now)
	assert.Equal(t, string(data), s)
}
