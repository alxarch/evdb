package evbadger

import (
	"errors"
	"net/url"
	"sync"

	"github.com/dgraph-io/badger/v2"
)

const (
	offset32 = 2166136261
	prime32  = 16777619
)

func newFNVa32() uint32 {
	return offset32

}
func addFNVa32(h, b uint32) uint32 {
	h ^= b
	h *= prime32
	return h
}

func hashFNVa32(data []byte) uint32 {
	h := newFNVa32()
	for _, b := range data {
		h = addFNVa32(h, uint32(b))
	}
	return h
}

func distinctSorted(ss []string) []string {
	var (
		i    int
		last string
	)
	for _, s := range ss {
		if i == 0 || s != last {
			last = s
			ss[i] = s
			i++
		}
	}
	return ss[:i]
}

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

var buffers sync.Pool

const kiB = 1024

func getBuffer() []byte {
	if x, ok := buffers.Get().([]byte); ok {
		return x
	}
	return make([]byte, 4*kiB)
}

func putBuffer(buf []byte) {
	buffers.Put(buf)
}

func parseURL(optionsURL string) (badger.Options, []string, error) {
	u, err := url.Parse(optionsURL)
	if err != nil {
		return badger.Options{}, nil, err
	}
	if u.Scheme != "badger" {
		return badger.Options{}, nil, errors.New(`Invalid scheme`)
	}
	options := badger.DefaultOptions
	// options.Logger = nil
	options.Dir = u.Path
	q := u.Query()
	options.ValueDir = q.Get("ValueDir")
	if options.ValueDir == "" {
		options.ValueDir = options.Dir
	}
	_, ok := q["ReadOnly"]
	if ok {
		switch q.Get("ReadOnly") {
		case "FALSE", "false", "off", "no", "0":
		default:
			options.ReadOnly = true
		}
	}

	return options, q["event"], nil
}
