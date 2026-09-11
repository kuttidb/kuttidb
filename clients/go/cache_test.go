package kuttidb

// Deterministic connection-lifecycle tests (fake transports, no sleeps in
// interleaving control — bounded waits only as failure guards). Run these
// under the race detector: go test -race -run 'TestPool|TestState|TestNewClient'.

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is a controllable net.Conn: response chunks are queued, reads
// block until Close when the queue is empty, and Close is observable.
type fakeConn struct {
	mu      sync.Mutex
	chunks  [][]byte // queued response chunks
	pending []byte   // remainder of the chunk being consumed
	closed  bool
	closeCh chan struct{}
	onRead  func() // invoked when a queued chunk is fully consumed

	writeGate   chan struct{} // optional: Write blocks until it closes
	writeStart  chan struct{} // closed once, when a Write begins
	writes      int
	onWriteDone func()
}

func newFakeConn() *fakeConn {
	return &fakeConn{closeCh: make(chan struct{})}
}

// respondRaw queues a raw response chunk without framing.
func (f *fakeConn) respondRaw(chunk []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chunks = append(f.chunks, chunk)
}

// respond queues a complete [status][vlen:4][payload] response chunk.
func (f *fakeConn) respond(status byte, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	chunk := append([]byte{status, byte(len(payload)), byte(len(payload) >> 8),
		byte(len(payload) >> 16), byte(len(payload) >> 24)}, payload...)
	f.chunks = append(f.chunks, chunk)
}

func (f *fakeConn) Read(p []byte) (int, error) {
	f.mu.Lock()
	if len(f.pending) == 0 && len(f.chunks) > 0 {
		f.pending = f.chunks[0]
		f.chunks = f.chunks[1:]
	}
	if len(f.pending) > 0 {
		n := copy(p, f.pending)
		f.pending = f.pending[n:]
		done := len(f.pending) == 0
		f.mu.Unlock()
		if done && f.onRead != nil {
			f.onRead()
		}
		return n, nil
	}
	closedCh := f.closeCh
	f.mu.Unlock()
	<-closedCh // a blocked read ends when the socket is closed
	return 0, net.ErrClosed
}

func (f *fakeConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, net.ErrClosed
	}
	gate := f.writeGate
	start := f.writeStart
	writes := f.writes
	f.writes = writes + 1
	f.mu.Unlock()
	if start != nil {
		select {
		case <-start:
		default:
			close(start)
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-f.closeCh:
			return 0, net.ErrClosed
		}
	}
	if f.onWriteDone != nil {
		f.onWriteDone()
	}
	return len(p), nil
}

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.closeCh)
	}
	return nil
}

func (f *fakeConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeConn) LocalAddr() net.Addr                { return fakeAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr               { return fakeAddr{} }
func (f *fakeConn) SetDeadline(t time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// instantConn is a fake transport that answers every request with a bare
// OK frame — used where only connect/return behavior matters.
func instantConn() *fakeConn {
	fc := newFakeConn()
	fc.respond(statusOK, nil)
	return fc
}

// newFakeClient builds a client whose transports come from dial and fills
// the eager pool exactly like newClientNetwork does.
func newFakeClient(poolSize int, dial func(n int) (net.Conn, error)) (*Client, *atomic.Int32) {
	if poolSize <= 0 {
		poolSize = 1
	}
	var count atomic.Int32
	c := &Client{
		network:     "fake",
		addr:        "fake:1",
		active:      make(map[*conn]struct{}),
		done:        make(chan struct{}),
		stateGate:   make(chan struct{}, 1),
		pool:        make(chan *conn, poolSize),
		dialTimeout: time.Second,
		opTimeout:   2 * time.Second,
		dialOverride: func(context.Context) (net.Conn, error) {
			return dial(int(count.Add(1)))
		},
	}
	c.stateGate <- struct{}{}
	for i := 0; i < poolSize; i++ {
		cn, err := c.dial()
		if err != nil {
			c.Close()
			return nil, &count
		}
		c.pool <- cn
	}
	return c, &count
}

// assertEventually polls cond until it holds or a bounded deadline passes.
func assertEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// drainClosed waits until every tracked fakeConn reports closed.
func drainClosed(t *testing.T, conns <-chan *fakeConn) {
	t.Helper()
	assertEventually(t, "all fake connections closed", func() bool {
		for {
			select {
			case fc := <-conns:
				if !fc.isClosed() {
					return false
				}
			default:
				return true
			}
		}
	})
}

func TestClientCloseIdempotentAndRejects(t *testing.T) {
	c, _ := newFakeClient(2, func(int) (net.Conn, error) { return instantConn(), nil })
	if err := c.Put("k", []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	c.Close()
	c.Close() // idempotent and concurrency-safe
	if err := c.Put("k", []byte("v")); !errors.Is(err, ErrClosed) {
		t.Fatalf("request begun after closure: %v", err)
	}
}

func TestPoolReturnAfterClose(t *testing.T) {
	// Return-after-close: acquire a connection, close the client, return
	// it; the return must discard, never panic and never re-enter the pool.
	c, _ := newFakeClient(1, func(int) (net.Conn, error) { return newFakeConn(), nil })
	cn, err := c.get()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	c.Close()
	c.put(cn)
	if !cn.c.(*fakeConn).isClosed() {
		t.Fatal("returned connection was not discarded/closed")
	}
}

func TestPoolSuccessReturnDuringClose(t *testing.T) {
	// A response fully received just before Close still completes
	// successfully; the connection is discarded at return. The read hook
	// makes the interleaving deterministic: Close happens after the last
	// byte of the response was consumed, before the deferred return.
	fc := newFakeConn()
	fc.mu.Lock()
	fc.chunks = append(fc.chunks, append([]byte{statusOK, 1, 0, 0, 0}, 'x'))
	fc.mu.Unlock()
	var c *Client
	fc.onRead = func() { c.Close() }
	c, _ = newFakeClient(1, func(int) (net.Conn, error) { return fc, nil })
	done := make(chan error, 1)
	go func() {
		_, _, err := c.requestFrame([]byte{opGet, 0, 0, 0, 0, 0, 0})
		done <- err
	}()
	assertEventually(t, "close via read hook", func() bool { return c.isClosed() })
	if err := <-done; err != nil {
		t.Fatalf("a fully received response must succeed during shutdown: %v", err)
	}
	assertEventually(t, "connection discard at return", func() bool { return fc.isClosed() })
}

func TestPoolConcurrentRequestsAndClose(t *testing.T) {
	conns := make(chan *fakeConn, 128)
	c, _ := newFakeClient(4, func(int) (net.Conn, error) {
		fc := newFakeConn()
		fc.respond(statusOK, nil)
		conns <- fc
		return fc, nil
	})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	req := []byte{opGet, 0, 0, 0, 0, 0, 0}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _, err := c.requestFrame(req)
				if err != nil && (errors.Is(err, ErrClosed) || c.isClosed()) {
					return
				}
			}
		}()
	}
	var closers sync.WaitGroup
	for i := 0; i < 8; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			c.Close()
		}()
	}
	closers.Wait()
	close(stop)
	wg.Wait()
	drainClosed(t, conns)
}

func TestPoolCloseRacingOverflowDial(t *testing.T) {
	// Close racing with overflow dials: late connections either register
	// before Close's snapshot and are closed by it, or observe closure and
	// close themselves. No late dial may leave a live socket behind.
	conns := make(chan *fakeConn, 128)
	unblocked := make(chan struct{})
	c, _ := newFakeClient(1, func(n int) (net.Conn, error) {
		if n == 1 { // the eager pool dial must not wait on the gate
			fc := newFakeConn()
			fc.respond(statusOK, nil)
			return fc, nil
		}
		<-unblocked
		fc := newFakeConn()
		conns <- fc
		return fc, nil
	})
	close(unblocked) // every get() overflow-dials from here on
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn, err := c.get()
			if err != nil {
				return // observed closure: acceptable
			}
			c.put(cn)
		}()
	}
	go func() {
		time.Sleep(20 * time.Millisecond) // let dials begin first
		c.Close()
	}()
	wg.Wait()
	c.Close()
	drainClosed(t, conns)
}

func TestStateRequestsTerminateOnClose(t *testing.T) {
	// One stateful request blocked in its read and another waiting on the
	// state operation lock must both terminate promptly on Close, and new
	// operations must be rejected afterwards.
	fc := newFakeConn() // no queued response: the read blocks until close
	c, _ := newFakeClient(1, func(int) (net.Conn, error) { return fc, nil })

	first := make(chan error, 1)
	go func() {
		_, _, err := c.stateRequest(opQueueStats, "q", nil)
		first <- err
	}()
	second := make(chan error, 1)
	go func() {
		_, _, err := c.stateRequest(opQueueStats, "q", nil)
		second <- err
	}()
	// Bounded guards only: wait until the state connection exists, then Close.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.lifeMu.Lock()
		sc := c.stateConn
		c.lifeMu.Unlock()
		if sc != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.lifeMu.Lock()
	sc := c.stateConn
	c.lifeMu.Unlock()
	if sc == nil {
		t.Fatal("state connection was not created")
	}
	c.Close()
	select {
	case err := <-first:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked state request: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked state request did not terminate on Close")
	}
	select {
	case err := <-second:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiting state request: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting state request did not terminate on Close")
	}
	if _, err := c.QueueStats("q"); !errors.Is(err, ErrClosed) {
		t.Fatalf("stats after close: %v", err)
	}
}

func TestNewClientFailureClosesOpened(t *testing.T) {
	// Constructor failure partway through eager pool initialization closes
	// every connection already opened.
	conns := make(chan *fakeConn, 8)
	c, count := newFakeClient(3, func(n int) (net.Conn, error) {
		if n == 3 { // the third eager dial fails
			return nil, errors.New("dial refused")
		}
		fc := newFakeConn()
		conns <- fc
		return fc, nil
	})
	if c != nil || count.Load() != 3 {
		t.Fatalf("expected nil client after failure, got %v (dials=%d)", c, count.Load())
	}
	close(conns)
	for fc := range conns {
		if !fc.isClosed() {
			t.Fatal("constructor failure left a connection open")
		}
	}
}

func TestPoolIsAnIdleCacheNotALimit(t *testing.T) {
	// The pool is an idle cache: when it is empty, get() dials overflow
	// connections instead of blocking — pool size is not a concurrency cap.
	c, _ := newFakeClient(1, func(int) (net.Conn, error) { return newFakeConn(), nil })
	defer c.Close()
	const workers = 8
	var leased sync.WaitGroup
	var release = make(chan *conn, workers)
	leased.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer leased.Done()
			cn, err := c.get()
			if err != nil {
				t.Error(err)
				return
			}
			release <- cn
		}()
	}
	leased.Wait()
	close(release)
	distinct := make(map[*conn]struct{})
	for cn := range release {
		distinct[cn] = struct{}{}
		c.put(cn)
	}
	if len(distinct) != workers {
		t.Fatalf("expected %d concurrent leases (overflow dials), got %d", workers, len(distinct))
	}
}