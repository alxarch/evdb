package evdb_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/internal/assert"
)

func Test_DataPoints(t *testing.T) {
	now := time.Now()
	var data evdb.DataPoints
	{
		b, err := json.Marshal(data)
		assert.NoError(t, err)
		assert.Equal(t, string(b), "null")
	}
	{
		data = data.Add(now.Unix(), 42)
		b, err := json.Marshal(data)
		assert.NoError(t, err)
		assert.Equal(t, string(b), fmt.Sprintf("[[%d,%d]]", now.Unix(), 42))
	}
	{
		var data evdb.DataPoints
		s := "[[12345678900,42],[12345678901,-32]]"
		err := json.Unmarshal([]byte(s), &data)
		assert.NoError(t, err)
		assert.Equal(t, data, evdb.DataPoints{
			{12345678900, 42},
			{12345678901, -32},
		})
		assert.Equal(t, data.Get(0), &data[0])
		assert.Nil(t, data.Get(-1))
		assert.Nil(t, data.Get(2))
	}

}

func Test_DataPointsSeek(t *testing.T) {
	all := evdb.DataPoints{
		{12345678900, 42},
		{12345678901, 43},
		{12345678902, 43},
		{12345678903, 43},
		{12345678904, 43},
		{12345678905, 43},
	}
	assert.Equal(t, all.SeekLeft(12345678900), all)
	assert.Equal(t, all.SeekLeft(0), all)
	assert.Equal(t, all.SeekLeft(12345678902), all[2:])
	assert.Nil(t, all.SeekLeft(12345678910))
	assert.Equal(t, all.SeekRight(12345678906), all)
	assert.Nil(t, all.SeekRight(0))
	assert.Nil(t, all.SeekRight(12345678899))
	assert.Equal(t, all.SeekRight(12345678902), all[:3])
	assert.Nil(t, all.Slice(0, 0))
	assert.Equal(t, all.Last(), &all[len(all)-1])
	assert.Equal(t, all.First(), &all[0])

	assert.Equal(t, all.Slice(12345678900, 12345678906), all)
	assert.Equal(t, all.Slice(12345678902, 12345678904), all[2:5])
	cp := all.Copy()
	cp.Fill(42)
	assert.Equal(t, cp.Sum(), 42.0*float64(len(all)))
	assert.Equal(t, cp.Avg(), 42.0)
	tr := evdb.TimeRange{Start: time.Unix(12345678900, 0), End: time.Unix(12345678905, 0), Step: time.Second}
	z := evdb.BlankData(&tr, 42)
	assert.Equal(t, cp, z)
	z = z.Add(12345678901, 42)
	assert.Equal(t, z[1].Value, float64(84))

	z = z.Reset()
	assert.Equal(t, z, evdb.DataPoints{})
	assert.Nil(t, z.First())
	assert.Nil(t, z.Last())

}
