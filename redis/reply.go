package redis

import (
	"bufio"
	"errors"
	"sync"

	"github.com/alxarch/go-meter/redis/repl"
)

// Reply is a reply for a redis command.
type Reply struct {
	values []value
	buffer []byte
	n      int // number of read values
}

// Value is a value in a redis reply.
type Value struct {
	id    int
	reply *Reply
}

type value struct {
	start int
	end   int
	num   int64
	typ   byte
	arr   []int
}

var replyPool sync.Pool

// BlankReply returns a blank reply from the pool
func BlankReply() *Reply {
	x := replyPool.Get()
	if x == nil {
		return new(Reply)
	}
	return x.(*Reply)
}

// Close resets and returns a Reply to the pool.
func (reply *Reply) Close() {
	if reply != nil {
		reply.Reset()
		replyPool.Put(reply)
	}
}

// Reset resets a reply invalidating any Value pointing to it.
func (reply *Reply) Reset() {
	reply.n = 0
	reply.buffer = reply.buffer[:0]
}

// Value returns the root Value of a reply or a NullValue
func (reply *Reply) Value() Value {
	if reply.n == 0 {
		return NullValue()
	}
	return Value{id: 0, reply: reply}
}

func (reply *Reply) value() (v *value) {
	if 0 <= reply.n && reply.n < len(reply.values) {
		v = &reply.values[reply.n]
		reply.n++
		return
	}
	tmp := make([]value, 2*len(reply.values)+1)
	copy(tmp, reply.values)
	reply.values = tmp
	if 0 <= reply.n && reply.n < len(reply.values) {
		v = &reply.values[reply.n]
		reply.n++
	}
	return
}

// Reply returns the parent Reply for the Value.
func (v Value) Reply() *Reply {
	return v.reply
}

func (v Value) get() *value {
	if v.reply != nil && 0 <= v.id && v.id < len(v.reply.values) {
		return &v.reply.values[v.id]
	}
	return nil
}

// Get returns the i-th element of an array reply.
func (v Value) Get(i int) Value {
	if vv := v.get(); vv != nil && vv.typ == repl.Array && 0 <= i && i < len(vv.arr) {
		return Value{id: vv.arr[i], reply: v.reply}
	}
	return NullValue()
}

// Bytes returns the slice of bytes for a value.
func (v Value) Bytes() []byte {
	if vv := v.get(); vv != nil && (vv.typ == repl.String || vv.typ == repl.BulkString) {
		return vv.slice(v.reply.buffer)
	}
	return nil
}

// Err returns an error if the value is an error value.
func (v Value) Err() error {
	if vv := v.get(); vv != nil && vv.typ == repl.Error {
		return errors.New(string(vv.slice(v.reply.buffer)))
	}
	return nil
}

func (v *value) slice(buf []byte) []byte {
	if 0 <= v.start && v.start <= v.end && v.end <= len(buf) {
		return buf[v.start:v.end]
	}
	return nil
}

// Type returns the type of the value.
func (v Value) Type() byte {
	if vv := v.get(); vv != nil {
		return vv.typ
	}
	return 0
}

// Int retuns the reply as int.
func (v Value) Int() (int64, bool) {
	if vv := v.get(); vv != nil {
		switch vv.typ {
		case repl.Integer:
			return vv.num, true
		case repl.String, repl.BulkString:
			return btoi(vv.slice(v.reply.buffer))
		}
	}
	return 0, false
}

// IsNull checks if a value is the NullValue.
func (v Value) IsNull() bool {
	if vv := v.get(); vv != nil {
		return vv.num == -1 && (vv.typ == repl.BulkString || vv.typ == repl.Array)
	}
	return v.id == -1
}

// Len returns the number of an array value's elements.
func (v Value) Len() int {
	if vv := v.get(); vv != nil {
		return len(vv.arr)
	}
	return 0
}

func btoi(buf []byte) (int64, bool) {
	var (
		signed bool
		n      int64
	)
	if len(buf) > 0 && buf[0] == '-' {
		signed = true
		buf = buf[1:]
	}
	for _, c := range buf {
		c -= '0'
		if 0 <= c && c <= 9 {
			n = n*10 + int64(c)
		} else {
			return 0, false
		}
	}
	if signed {
		return -n, true
	}
	return n, true
}

// ReadFromN reads n replies from a redis stream.
func (reply *Reply) ReadFromN(r *bufio.Reader, n int64) (Value, error) {
	id := reply.n
	reply.values = reply.values[:cap(reply.values)]
	err := reply.readArray(r, n)
	reply.values = reply.values[:reply.n]
	return Value{id: id, reply: reply}, err
}

func (reply *Reply) readArray(r *bufio.Reader, n int64) error {
	if n < -1 {
		return repl.ProtocolError
	}
	id := reply.n
	v := reply.value()
	v.typ = repl.Array
	v.num = n
	v.start = -1
	v.end = -1
	v.arr = v.arr[:0]
	if n == -1 {
		return nil
	}
	for i := int64(0); i < n; i++ {
		v.arr = append(v.arr, reply.n)
		if err := reply.read(r); err != nil {
			return err
		}
	}
	reply.values[id] = *v
	return nil
}

// ReadFrom reads a single reply from a redis stream.
func (reply *Reply) ReadFrom(r *bufio.Reader) (Value, error) {
	id := reply.n
	reply.values = reply.values[:cap(reply.values)]
	err := reply.read(r)
	reply.values = reply.values[:reply.n]
	return Value{id: id, reply: reply}, err
}

func (reply *Reply) read(r *bufio.Reader) error {
	typ, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch typ {
	case repl.Error, repl.String:
		start := len(reply.buffer)
		reply.buffer, err = repl.ReadLine(reply.buffer, r)
		if err != nil {
			return err
		}
		v := reply.value()
		v.typ = typ
		v.num = 0
		v.arr = v.arr[:0]
		v.start = start
		v.end = len(reply.buffer)
		return nil
	case repl.Integer:
		var n int64
		n, err = repl.ReadInt(r)
		if err != nil {
			return err
		}
		v := reply.value()
		v.typ = typ
		v.arr = v.arr[:0]
		v.start = -1
		v.num = n
		return nil
	case repl.BulkString:
		var n int64
		n, err = repl.ReadInt(r)
		if err != nil {
			return err
		}
		start := len(reply.buffer)
		reply.buffer, err = repl.ReadBulkString(reply.buffer, n, r)
		if err != nil {
			return err
		}
		v := reply.value()
		v.start = start
		v.num = n
		v.typ = typ
		v.arr = v.arr[:0]
		v.end = len(reply.buffer)
		return nil
	case repl.Array:
		var n int64
		n, err = repl.ReadInt(r)
		if err != nil {
			return err
		}
		return reply.readArray(r, n)
	default:
		return repl.ProtocolError
	}
}

func (reply *Reply) get(id int) *value {
	if reply != nil && 0 <= id && id < len(reply.values) {
		return &reply.values[id]
	}
	return nil
}

// ForEach iterates each value in a BulkStringArray reply
func (v Value) ForEach(fn func(v Value)) {
	if fn == nil {
		return
	}
	if vv := v.reply.get(v.id); vv != nil || vv.typ == repl.Array {
		for _, id := range vv.arr {
			fn(Value{id: id, reply: v.reply})
		}
	}
}

// ForEachKV iterates each key value pair in a BulkStringArray reply
func (v Value) ForEachKV(fn func(k []byte, v Value)) {
	if fn == nil {
		return
	}
	if vv := v.reply.get(v.id); vv != nil && vv.typ == repl.Array {
		var k *value
		for i, id := range vv.arr {
			if i%2 == 0 {
				k = v.reply.get(id)
			} else if k != nil {
				fn(k.slice(v.reply.buffer), Value{id: id, reply: v.reply})
				k = nil
			}
		}
		if k != nil {
			fn(k.slice(v.reply.buffer), NullValue())
		}
	}

}

type ScanIterator struct {
	cmd   string
	match string
	key   string
	cur   int64
	val   Value
	err   error
	count int64
}

func HSCAN(key, match string, count int64) *ScanIterator {
	s := ScanIterator{
		cmd:   "HSCAN",
		key:   key,
		match: match,
		count: count,
	}
	return &s
}

func NullValue() Value {
	return Value{-1, nil}
}

func (s *ScanIterator) Err() error {
	return s.err
}

func (s *ScanIterator) Next(conn *Conn) Value {
	if s.err != nil {
		return NullValue()
	}
	reply := s.val.Reply()
	if reply == nil {
		if s.val.IsNull() {
			return s.val
		}
		reply = BlankReply()
	} else if s.cur == 0 {
		reply.Close()
		s.val = NullValue()
		return s.val
	} else {
		reply.Reset()
	}

	p := Get()
	switch s.cmd {
	case "HSCAN":
		p.HScan(s.key, s.cur, s.match, s.count)
	case "ZSCAN":
		p.ZScan(s.key, s.cur, s.match, s.count)
	case "SSCAN":
		p.SScan(s.key, s.cur, s.match, s.count)
	default:
		p.Scan(s.cur, s.match, s.count)
	}
	s.err = conn.Do(p, reply)
	Put(p)
	if s.err != nil {
		s.val = NullValue()
		reply.Close()
		return s.val
	}
	v := reply.Value()
	s.err = v.Err()
	if s.err != nil {
		s.val = NullValue()
		reply.Close()
		return s.val
	}
	s.cur, _ = v.Get(0).Int()
	s.val = v.Get(1)
	if s.val.Len() > 0 {
		return s.val
	}
	return s.Next(conn)
}
