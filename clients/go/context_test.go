package kuttidb

// Context/cancellation tests: controlled fake transports, bounded waits
// only as failure guards, all runnable under the race detector.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakePool is a dial-counting fake client for context tests.
type fakePool struct {
	c    *Client
	dial *atomic.Int32
	seen chan *fakeConn
}

func newFakePool(poolSize int, makeConn func(n int) *fakeConn) *fakePool {
	fp := &fakePool{dial: &atomic.Int32{}, seen: make(chan *fakeConn, 128)}
	c := &Client{
		network:     "fake",
		addr:        "fake:1",
		active:      make(map[*conn]struct{}),
		done:        make(chan struct{}),
		stateGate:   make(chan struct{}, 1),
		pool:        make(chan *conn, maxInt(poolSize, 1)),
		dialTimeout: time.Second,
		opTimeout:   30 * time.Second,
		dialOverride: func(ctx context.Context) (net.Conn, error) {
			n := int(fp.dial.Add(1))
			fc := makeConn(n)
			fp.seen <- fc
			return fc, nil
		},
	}
	c.stateGate <- struct{}{}
	for i := 0; i < poolSize; i++ { // 0 = dial on demand (overflow) only
		fc := makeConn(i + 1)
		fp.seen <- fc
		c.pool <- &conn{c: fc}
	}
	fp.c = c
	return fp
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// waitBounded polls cond until it holds or the bound passes (failure guard
// only — interleaving is controlled by explicit channels elsewhere).
func waitBounded(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestContextPreCanceledSendsNothing(t *testing.T) {
	fp := newFakePool(2, func(int) *fakeConn { return instantConn() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fp.c.PutContext(ctx, "k", []byte("v")); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutContext: %v", err)
	}
	if _, err := fp.c.GetContext(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetContext: %v", err)
	}
	if _, err := fp.c.QueueStatsContext(ctx, "q"); !errors.Is(err, context.Canceled) {
		t.Fatalf("QueueStatsContext: %v", err)
	}
	if _, err := fp.c.StreamListContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("StreamListContext: %v", err)
	}
	if _, err := fp.c.CapabilitiesContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CapabilitiesContext: %v", err)
	}
	if n := fp.dial.Load(); n != 0 {
		t.Fatalf("pre-canceled requests dialed %d connections", n)
	}
}

func TestContextCancelDuringWriteHeaderBody(t *testing.T) {
	// during write: the write is blocked on a gate
	gated := newFakeConn()
	gated.respond(statusOK, nil)
	gated.writeGate = make(chan struct{})
	gated.writeStart = make(chan struct{})
	fp := newFakePool(1, func(int) *fakeConn { return gated })
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- fp.c.PutContext(ctx, "k", []byte("v")) }()
	<-gated.writeStart
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel during write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel during write did not terminate the request")
	}

	// during response header: written, blocked reading the header
	header := newFakeConn() // no response queued
	fp2 := newFakePool(1, func(int) *fakeConn { return header })
	ctx2, cancel2 := context.WithCancel(context.Background())
	errCh2 := make(chan error, 1)
	go func() {
		_, err := fp2.c.GetContext(ctx2, "k")
		errCh2 <- err
	}()
	waitBounded(t, "request written", func() bool {
		header.mu.Lock()
		defer header.mu.Unlock()
		return header.writes >= 1
	})
	time.Sleep(20 * time.Millisecond) // header read is blocked
	cancel2()
	select {
	case err := <-errCh2:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel during header: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel during header did not terminate the request")
	}
	if !header.isClosed() {
		t.Fatal("connection blocked in the header read was not discarded")
	}
}

func TestContextCancelDuringResponseBody(t *testing.T) {
	// header arrives, body never does: cancel while the body read blocks
	fc := newFakeConn()
	fc.mu.Lock()
	fc.chunks = append(fc.chunks, []byte{statusOK, 8, 0, 0, 0}) // vlen 8, no body
	fc.mu.Unlock()
	fp := newFakePool(1, func(int) *fakeConn { return fc })
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := fp.c.GetContext(ctx, "k"); errCh <- err }()
	waitBounded(t, "request written", func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.writes >= 1
	})
	time.Sleep(20 * time.Millisecond) // let the header be consumed
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel during body: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel during body did not terminate the request")
	}
	if !fc.isClosed() {
		t.Fatal("connection with a partial response was not discarded")
	}
}

func TestContextShortDeadlineOverridesTimeout(t *testing.T) {
	// The context deadline (50ms) overrides the configured operation
	// timeout (30s): a blocked read ends with DeadlineExceeded, fast.
	fp := newFakePool(1, func(int) *fakeConn { return newFakeConn() })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := fp.c.GetContext(ctx, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short deadline: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("short deadline took %v — the 30s timeout was not overridden", elapsed)
	}
}

func TestContextStateGateCancellation(t *testing.T) {
	// A canceled caller waiting for the state gate returns promptly; the
	// in-flight exchange keeps the gate until it ends.
	fc := newFakeConn() // blocks in read once written
	fp := newFakePool(1, func(int) *fakeConn { return fc })
	first := make(chan error, 1)
	go func() {
		_, _, err := fp.c.stateRequestCtx(context.Background(), opQueueStats, "q", nil)
		first <- err
	}()
	waitBounded(t, "first state exchange written", func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.writes >= 1
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fp.c.QueueStatsContext(ctx, "q"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled state gate wait: %v", err)
	}
	// The first exchange still owns the gate; shutdown ends it.
	fp.c.Close()
	select {
	case err := <-first:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked state exchange on close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked state exchange did not terminate on close")
	}
}

func TestContextDialCancellation(t *testing.T) {
	c := &Client{
		network: "fake", addr: "fake:1",
		active: make(map[*conn]struct{}), done: make(chan struct{}),
		stateGate: make(chan struct{}, 1), pool: make(chan *conn, 1),
		dialTimeout: time.Second, opTimeout: 10 * time.Second,
		dialOverride: func(ctx context.Context) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	c.stateGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.PutContext(ctx, "k", []byte("v")) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("dial cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dial cancellation did not terminate the request")
	}
}

func TestContextAuthCancellation(t *testing.T) {
	fc := newFakeConn() // AUTH response never arrives
	c := &Client{
		network: "fake", addr: "fake:1",
		active: make(map[*conn]struct{}), done: make(chan struct{}),
		stateGate: make(chan struct{}, 1), pool: make(chan *conn, 1),
		dialTimeout: time.Second, opTimeout: 30 * time.Second,
		authToken: []byte("token"),
		dialOverride: func(context.Context) (net.Conn, error) {
			return fc, nil
		},
	}
	c.stateGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.PutContext(ctx, "k", []byte("v")) }()
	waitBounded(t, "auth written", func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.writes >= 1
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("auth cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("auth cancellation did not terminate the request")
	}
	if !fc.isClosed() {
		t.Fatal("connection abandoned mid-AUTH was not discarded")
	}
}

func TestContextTLSHandshakeCancellation(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	release := make(chan struct{})
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		defer raw.Close()
		tc := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
		<-release // hold the handshake while the test cancels
		_ = tc.HandshakeContext(context.Background())
	}()
	c := &Client{
		network: "tcp", addr: ln.Addr().String(),
		active: make(map[*conn]struct{}), done: make(chan struct{}),
		stateGate: make(chan struct{}, 1), pool: make(chan *conn, 1),
		dialTimeout: time.Second, opTimeout: 30 * time.Second,
		useTLS:    true,
		tlsConfig: &tls.Config{InsecureSkipVerify: true},
	}
	c.stateGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(30 * time.Second)
		_, err := c.getCtx(ctx, deadline)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond) // handshake blocked server-side
	cancel()
	select {
	case err := <-errCh:
		if err == nil || !(errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed)) {
			t.Fatalf("TLS handshake cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TLS handshake cancellation did not terminate")
	}
	close(release)
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}
	return cert
}

func TestContextCloseDuringWriteHeaderBody(t *testing.T) {
	// write phase
	gated := newFakeConn()
	gated.writeGate = make(chan struct{})
	gated.writeStart = make(chan struct{})
	fp := newFakePool(1, func(int) *fakeConn { return gated })
	errCh := make(chan error, 1)
	go func() { errCh <- fp.c.PutContext(context.Background(), "k", []byte("v")) }()
	<-gated.writeStart
	fp.c.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("close during write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close during write did not terminate the request")
	}

	// response header phase
	blocked := newFakeConn()
	fp2 := newFakePool(1, func(int) *fakeConn { return blocked })
	errCh = make(chan error, 1)
	go func() { _, err := fp2.c.GetContext(context.Background(), "k"); errCh <- err }()
	waitBounded(t, "request written", func() bool {
		blocked.mu.Lock()
		defer blocked.mu.Unlock()
		return blocked.writes >= 1
	})
	fp2.c.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("close during header: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close during header did not terminate the request")
	}
}

func TestContextPartialResponseDiscarded(t *testing.T) {
	// A partial response must be discarded: the next healthy request gets a
	// safe connection and cannot observe the stale bytes.
	partial := newFakeConn()
	partial.mu.Lock()
	partial.chunks = append(partial.chunks, []byte{statusOK, 8, 0, 0, 0}) // header only
	partial.mu.Unlock()
	fp := newFakePool(0, func(n int) *fakeConn {
		if n == 1 {
			return partial
		}
		fc := newFakeConn()
		fc.respond(statusOK, []byte("ok"))
		return fc
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := fp.c.GetContext(ctx, "k"); errCh <- err }()
	waitBounded(t, "partial request written", func() bool {
		partial.mu.Lock()
		defer partial.mu.Unlock()
		return partial.writes >= 1
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("partial response: %v", err)
	}
	if !partial.isClosed() {
		t.Fatal("partial-response connection was not discarded")
	}
	// The next request uses a fresh, healthy connection.
	v, err := fp.c.GetContext(context.Background(), "k")
	if err != nil || string(v) != "ok" {
		t.Fatalf("healthy request after partial response: %q %v", v, err)
	}
}

func TestContextCancelAtCompletionBoundary(t *testing.T) {
	// Cancellation forced at the completion/return boundary: either the
	// response wins or the context wins, but the watcher can never close a
	// subsequently leased connection and nothing is retried.
	fc := newFakeConn()
	fc.mu.Lock()
	fc.chunks = append(fc.chunks, append([]byte{statusOK, 2, 0, 0, 0}, []byte("hi")...))
	fc.mu.Unlock()
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(context.Background())
	fc.onRead = func() { cancel() } // fires after the last body byte
	fp := newFakePool(0, func(n int) *fakeConn {
		if n == 1 {
			return fc
		}
		fresh := newFakeConn()
		fresh.respond(statusOK, []byte("ok"))
		return fresh
	})
	errCh := make(chan error, 1)
	go func() {
		_, err := fp.c.GetContext(ctx, "k")
		errCh <- err
	}()
	var err error
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("completion-boundary request did not finish")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("boundary outcome must be success or a context error: %v", err)
	}
	assertEventually(t, "boundary connection discarded", func() bool { return fc.isClosed() })
	// A subsequent request gets a fresh socket and works: the stopped
	// watcher cannot close it.
	fc2 := newFakeConn()
	fc2.mu.Lock()
	fc2.chunks = append(fc2.chunks, append([]byte{statusOK, 2, 0, 0, 0}, []byte("ok")...))
	fc2.mu.Unlock()
	fp.seen <- fc2
	v, err := fp.c.GetContext(context.Background(), "k")
	if err != nil || string(v) != "ok" {
		t.Fatalf("subsequent lease poisoned by a stopped watcher: %q %v", v, err)
	}
}

func TestContextConcurrentDistinct(t *testing.T) {
	fp := newFakePool(0, func(int) *fakeConn {
		fc := newFakeConn()
		// The pool reuses connections, so every request needs an answer.
		fc.onWriteDone = func() { fc.respond(statusOK, nil) }
		return fc
	})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			if i%2 == 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
			}
			if err := fp.c.PutContext(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
				t.Errorf("put %d: %v", i, err)
			}
			if v, err := fp.c.GetContext(ctx, fmt.Sprintf("k%d", i)); err != nil || len(v) != 0 {
				t.Errorf("get %d: %q %v", i, v, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestContextDeadlineSharedAcrossProbeAndFetch(t *testing.T) {
	// The capability probe and the fetch share one budget: a deadline that
	// expires during the probe ends the operation quickly — it is never
	// restarted per segment.
	probeBlocked := newFakeConn() // CAPABILITIES response never arrives
	fp := newFakePool(1, func(int) *fakeConn { return probeBlocked })
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := fp.c.StreamFetchContext(ctx, "t", 0, 0, 10)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared deadline across probe: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("deadline not shared across capability probing: %v", elapsed)
	}
}

func TestContextDeadlineSharedAcrossChunks(t *testing.T) {
	// PutManyContext with two chunks: the deadline set for the operation
	// governs the second chunk too — a blocked second-chunk read ends at
	// the shared deadline instead of restarting 30 seconds.
	okConn := instantConn()
	blocked := newFakeConn() // second chunk's response never arrives
	n := 0
	var mu sync.Mutex
	fp := newFakePool(0, func(int) *fakeConn {
		mu.Lock()
		defer mu.Unlock()
		n++
		if n == 1 {
			return okConn
		}
		return blocked
	})
	pairs := make(map[string][]byte, 300)
	for i := 0; i < 300; i++ { // BatchSize is 256 → two chunks
		pairs[fmt.Sprintf("key-%03d", i)] = []byte("v")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := fp.c.PutManyContext(ctx, pairs)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared deadline across chunks: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("deadline restarted per chunk: %v", elapsed)
	}
}

func TestContextNoMutationRetryAfterCancel(t *testing.T) {
	// A mutation whose response was lost after the frame was sent is never
	// resent: cancellation bounds waiting, it does not retry.
	fc := newFakeConn() // no response: blocks after the write
	fp := newFakePool(1, func(int) *fakeConn { return fc })
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := fp.c.QueuePublishContext(ctx, "q", []byte("payload"), 0); errCh <- err }()
	waitBounded(t, "mutation written", func() bool {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		return fc.writes >= 1
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish: %v", err)
	}
	fc.mu.Lock()
	n := fc.writes
	fc.mu.Unlock()
	if n != 1 {
		t.Fatalf("mutation was sent %d times; transparent retry is forbidden", n)
	}
}