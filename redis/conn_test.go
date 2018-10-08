package redis

import (
	"testing"
	"time"

	"github.com/alxarch/go-meter/redis/repl"
)

func TestConn(t *testing.T) {
	conn, err := Dial(nil)
	if err != nil {
		t.Error(err)
		return
	}
	p := Get()
	defer Put(p)
	r := BlankReply()
	defer r.Close()
	p.Select(1)
	p.Set("foo", String("bar"), 0)
	p.Keys("*")
	p.FlushDB()
	if err := conn.Do(p, r); err != nil {
		t.Error(err)
		return
	}
	v := r.Value()
	if v.Len() != 4 {
		t.Errorf("Invalid reply length: %d", v.Len())
	}
	if v.Type() != repl.Array {
		t.Errorf("Invalid reply type: %d", v.Type())
	}
	if ok := v.Get(0); string(ok.Bytes()) != "OK" {
		t.Errorf("Invalid select reply: %s", ok.Bytes())
	}

	if ok := v.Get(1); string(ok.Bytes()) != "OK" {
		t.Errorf("Invalid set reply: %s", ok.Bytes())
	}
	if keys := v.Get(2); keys.Len() != 1 {
		t.Errorf("Invalid keys reply size: %d", keys.Len())
	}
	if ok := v.Get(3); string(ok.Bytes()) != "OK" {
		t.Errorf("Invalid flushdb reply: %s", ok.Bytes())
	}
}

func Test_Pool(t *testing.T) {
	pool := NewPool(&PoolOptions{})
	conn, err := pool.Get(time.Time{})
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	defer pool.Put(conn)
	p := Get()
	p.HSet("foo", "bar", String("baz"))
	defer Put(p)
	conn.Do(p, nil)

}

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()
	p := Get()
	for i := 0; i < b.N; i++ {
		p.Reset()
		p.HIncrBy("foo", "bar", 1)
	}
	Put(p)
}
