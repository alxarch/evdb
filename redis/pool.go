package redis

import (
	"bufio"
	"errors"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type PoolOptions struct {
	ConnOptions
	MaxConnections    int
	MaxIdleTime       time.Duration
	MaxConnectionAge  time.Duration
	ClockFrequency    time.Duration
	CheckIdleInterval time.Duration
	Dial              func(network, address string) (net.Conn, error)
}

type PoolStats struct {
	Hits     uint32
	Misses   uint32
	Timeouts uint32
}

type Pool struct {
	options *PoolOptions

	numOpen int32
	numIdle int32

	mu        sync.Mutex
	cond      sync.Cond
	closed    bool
	idle      []*Conn
	closeChan chan struct{}
	clock     int64

	stats PoolStats
}

func NewPool(options *PoolOptions) *Pool {
	if options == nil {
		options = new(PoolOptions)
	}
	if options.Dial == nil {
		options.Dial = net.Dial
	}
	pool := Pool{
		options:   options,
		closeChan: make(chan struct{}),
	}
	pool.cond.L = &pool.mu
	go pool.runClock()
	if options.CheckIdleInterval > 0 {
		go pool.runCleaner()
	}
	return &pool
}

func (pool *Pool) Clean() (int, error) {
	size := atomic.LoadInt32(&pool.numIdle)
	scratch := make([]*Conn, size)
	pool.clean(&scratch, time.Now())
	return pool.clean(&scratch, time.Now())
}

func (pool *Pool) clean(scratch *[]*Conn, now time.Time) (int, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return 0, errPoolClosed
	}
	i := 0
	idle := pool.idle
	for 0 <= i && i < len(idle) && now.Sub(idle[i].lastUsedAt) > pool.options.MaxIdleTime {
		i++
	}
	if i == 0 {
		pool.mu.Unlock()
		return 0, nil
	}
	*scratch = append((*scratch)[:0], idle[:i]...)
	j := copy(pool.idle, idle[i:])
	if len(pool.idle) > j {
		for i := range pool.idle[j:] {
			pool.idle[i] = nil
		}
		pool.idle = pool.idle[:j]
	}
	pool.mu.Unlock()
	atomic.StoreInt32(&pool.numIdle, int32(j))
	tmp := *scratch
	for i, c := range tmp {
		c.Close()
		tmp[i] = nil
	}
	n := len(tmp)
	*scratch = tmp[:0]
	return n, nil
}

func (pool *Pool) Stop() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	atomic.StoreInt32(&pool.numIdle, math.MinInt32)
	atomic.StoreInt32(&pool.numOpen, math.MinInt32)
	for i, c := range pool.idle {
		pool.idle[i] = nil
		c.closeWithError(errPoolClosed)
	}
	pool.idle = pool.idle[:0]
	close(pool.closeChan)
	pool.cond.Broadcast()
	pool.mu.Unlock()
}

const poolClockInterval = 100 * time.Millisecond

func (pool *Pool) newConn(conn net.Conn) (c *Conn) {
	now := time.Now()
	x := connPool.Get()
	if x == nil {
		c = new(Conn)
		size := pool.options.ReadBufferSize
		if size < minBufferSize {
			size = minBufferSize
		}
		c.r = bufio.NewReaderSize(c, size)
	} else {
		c = x.(*Conn)
		c.r.Reset(c)
	}
	c.options = &pool.options.ConnOptions
	c.conn = conn
	c.createdAt = now
	c.lastUsedAt = now
	return
}

func (pool *Pool) dial() {
	addr := pool.options.Addr()
	dialer := pool.options.Dial
	if dialer == nil {
		dialer = net.Dial
	}
	conn, err := dialer(addr.Network(), addr.String())
	if err != nil {
		atomic.AddInt32(&pool.numOpen, -1)
		return
	}
	pool.put(pool.newConn(conn))
}

func (pool *Pool) runCleaner() {
	interval := pool.options.MaxIdleTime
	if interval < time.Second {
		interval = time.Second
	}
	var scratch []*Conn
	tick := time.NewTicker(interval)
	for {
		select {
		case <-pool.closeChan:
			tick.Stop()
			return
		case t := <-tick.C:
			pool.clean(&scratch, t)
		}
	}
}
func (pool *Pool) setClock(t time.Time) {
	n := t.UnixNano()
	pool.mu.Lock()
	pool.clock = n
	if len(pool.idle) == 0 {
		pool.cond.Broadcast()
	}
	pool.mu.Unlock()

}
func (pool *Pool) runClock() {
	pool.setClock(time.Now())
	tick := time.NewTicker(poolClockInterval)
	for {
		select {
		case <-pool.closeChan:
			tick.Stop()
			return
		case now := <-tick.C:
			pool.setClock(now)
		}
	}
}

// Pool to hold *Conn objects
var connPool sync.Pool

func (pool *Pool) closeConn(c *Conn) {
	c.conn.Close()
	c.conn = nil
	c.r.Reset(c)
	c.err = nil
	connPool.Put(c)
	for {
		n := atomic.LoadInt32(&pool.numOpen)
		if n <= 0 || atomic.CompareAndSwapInt32(&pool.numOpen, n, n-1) {
			return
		}
	}
}

func (pool *Pool) Put(c *Conn) {
	if c == nil {
		return
	}

	if c.err != nil {
		pool.closeConn(c)
		return
	}
	now := time.Now()
	if pool.options.MaxConnectionAge > 0 && now.Sub(c.createdAt) > pool.options.MaxConnectionAge {
		pool.closeConn(c)
		return
	}
	c.lastUsedAt = now
	pool.put(c)
}

func (pool *Pool) put(c *Conn) {
	// c.lastUsedAt = now
	// n := now.UnixNano()
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		pool.closeConn(c)
		return
	}
	// pool.clock = n
	pool.idle = append(pool.idle, c)
	if pool.cond.L == nil {
		pool.cond.L = &pool.mu
	}
	pool.cond.Signal()
	pool.mu.Unlock()
	atomic.AddInt32(&pool.numIdle, 1)
}

func (pool *Pool) get(deadline int64) (conn *Conn, err error) {
	miss := false
	pool.mu.Lock()
	for len(pool.idle) == 0 {
		miss = true
		if pool.closed {
			pool.mu.Unlock()
			err = errPoolClosed
			return
		}
		if 0 < deadline && deadline < pool.clock {
			pool.mu.Unlock()
			atomic.AddUint32(&pool.stats.Timeouts, 1)
			err = errDeadlineExceeded
			return
		}
		pool.cond.Wait()
	}

	if n := len(pool.idle) - 1; 0 <= n && n < len(pool.idle) {
		conn = pool.idle[n]
		pool.idle[n] = nil
		pool.idle = pool.idle[:n]
	}
	pool.mu.Unlock()
	if miss {
		atomic.AddUint32(&pool.stats.Misses, 1)
	} else {
		atomic.AddUint32(&pool.stats.Hits, 1)
	}
	return
}

var (
	errConnWriteOnly    = errors.New("Write only connection")
	errConnClosed       = errors.New("Connection closed")
	errPoolClosed       = errors.New("Pool closed")
	errDeadlineExceeded = errors.New("Deadline exceeded")
)

func (pool *Pool) maxConnections() int32 {
	max := int32(pool.options.MaxConnections)
	if max <= 0 {
		max = math.MaxInt32
	}
	return max
}

func (pool *Pool) Get(deadline time.Time) (*Conn, error) {
	for {
		if n := atomic.LoadInt32(&pool.numIdle); n > 0 {
			if atomic.CompareAndSwapInt32(&pool.numIdle, n, n-1) {
				return pool.get(deadline.UnixNano())
			}
		} else if n < 0 {
			return nil, errPoolClosed
		} else {
			break
		}
	}
	max := pool.maxConnections()
	for {
		if n := atomic.LoadInt32(&pool.numOpen); 0 <= n && n < max {
			if atomic.CompareAndSwapInt32(&pool.numOpen, n, n+1) {
				go pool.dial()
				break
			}
		} else if n < 0 {
			return nil, errPoolClosed
		} else {
			break
		}
	}
	return pool.get(deadline.UnixNano())
}

func (pool *Pool) Do(p *Pipeline, r *Reply) error {
	conn, err := pool.Get(time.Time{})
	if err != nil {
		return err
	}
	err = conn.Do(p, r)
	pool.Put(conn)
	return err
}
