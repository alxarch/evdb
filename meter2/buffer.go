package meter2

import (
	"bytes"
	"sync"
)

// Buffer pool for event field building
const minBufferSize = 1024

var bpool = &sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, minBufferSize))
	},
}

func bget() *bytes.Buffer {
	return bpool.Get().(*bytes.Buffer)
}
func bput(b *bytes.Buffer) {
	if b == nil || b.Cap() < minBufferSize {
		return
	}
	b.Reset()
	bpool.Put(b)
}
