package kuttidb

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	statusError   = 0x02
	protocolMajor = 1
	protocolMinor = 8
)

func appendU16(dst []byte, v uint16) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	return append(dst, b[:]...)
}

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

func milliseconds(d time.Duration, allowZero bool) (uint64, error) {
	if d < 0 {
		return 0, fmt.Errorf("kuttidb: duration must be non-negative")
	}
	ms := d.Milliseconds()
	if ms == 0 && d > 0 && !allowZero {
		ms = 1
	}
	return uint64(ms), nil
}

func frame(op byte, key string, value []byte) ([]byte, error) {
	if len(key) > maxKey {
		return nil, ErrKeyTooLarge
	}
	if len(value) > maxValue {
		return nil, ErrValueTooLarge
	}
	req := make([]byte, 7, 7+len(key)+len(value))
	req[0] = op
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(req[3:7], uint32(len(value)))
	req = append(req, key...)
	req = append(req, value...)
	return req, nil
}

// opDeadline returns the absolute deadline for one client operation: the
// earlier of the caller's context deadline and the configured operation
// timeout, measured from a single start. Every subrequest of an operation
// (capability probes, batch chunks, response segments) shares this budget —
// it is never restarted per segment.
func (c *Client) opDeadline(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	deadline := time.Now().Add(c.opTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return deadline, nil
}

// readFull reads exactly len(buf) bytes. The caller owns the read deadline;
// readFull never overwrites one.
func readFull(cn *conn, buf []byte) error {
	n := 0
	for n < len(buf) {
		r, err := cn.c.Read(buf[n:])
		if err != nil {
			return err
		}
		n += r
	}
	return nil
}

// writeFull writes the whole frame or fails. A short write is never treated
// as success; the caller discards the connection after any write error.
func writeFull(cn *conn, buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := cn.c.Write(buf[written:])
		written += n
		if err != nil {
			return err
		}
	}
	return nil
}

// watchIO runs fn under a cancellation watcher that aborts blocked I/O by
// closing the socket when ctx is canceled. The watcher is always stopped
// and joined before returning, so it can never close a socket after a later
// borrower leased it. Cancellation wins error recognition; an I/O failure
// while the client was closing maps to ErrClosed.
func (c *Client) watchIO(ctx context.Context, cn *conn, fn func() error) error {
	done := ctx.Done()
	if done == nil {
		err := fn()
		if err != nil && c.isClosed() {
			return fmt.Errorf("%w: request interrupted by client shutdown: %v", ErrClosed, err)
		}
		return err
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-done:
			_ = cn.c.Close() // abort a blocked write/read
		case <-stop:
		}
	}()
	err := fn()
	close(stop)
	wg.Wait()
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr // cancellation stays recognizable as a context error
		}
		if c.isClosed() {
			return fmt.Errorf("%w: request interrupted by client shutdown: %v", ErrClosed, err)
		}
		return err
	}
	// A cancel racing with a completed exchange: the watcher may already
	// have closed the socket, so the outcome is treated as canceled.
	return ctx.Err()
}

// getCtx leases a connection, honoring the caller's context and client
// shutdown. The lifecycle mutex is released before dialing: dial, TLS, and
// AUTH run without it. A connection opened concurrently with Close either
// registers before Close's snapshot or observes closure and closes itself.
func (c *Client) getCtx(ctx context.Context, deadline time.Time) (*conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.lifeMu.Lock()
	if c.closed {
		c.lifeMu.Unlock()
		return nil, ErrClosed
	}
	select {
	case cn := <-c.pool:
		c.active[cn] = struct{}{}
		c.lifeMu.Unlock()
		return cn, nil
	default:
		c.lifeMu.Unlock()
	}
	cn, err := c.dialCtx(ctx, deadline)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if c.isClosed() {
			return nil, fmt.Errorf("%w: dial interrupted by client shutdown: %v", ErrClosed, err)
		}
		return nil, err
	}
	c.lifeMu.Lock()
	if c.closed {
		c.lifeMu.Unlock()
		cn.c.Close()
		return nil, ErrClosed
	}
	c.active[cn] = struct{}{}
	c.lifeMu.Unlock()
	return cn, nil
}

// dial opens a connection with the default (no-context) path.
func (c *Client) dial() (*conn, error) {
	deadline, err := c.opDeadline(context.Background())
	if err != nil {
		return nil, err
	}
	return c.dialCtx(context.Background(), deadline)
}

// dialCtx opens a connection: transport dial, TLS handshake, and AUTH all
// honor the caller's context and the operation's absolute deadline.
func (c *Client) dialCtx(ctx context.Context, deadline time.Time) (*conn, error) {
	var nc net.Conn
	var err error
	switch {
	case c.dialOverride != nil:
		nc, err = c.dialOverride(ctx)
	case c.useTLS:
		d := &net.Dialer{Timeout: c.dialTimeout}
		nc, err = d.DialContext(ctx, "tcp", c.addr)
	default:
		d := &net.Dialer{Timeout: c.dialTimeout}
		nc, err = d.DialContext(ctx, c.network, c.addr)
	}
	if err != nil {
		return nil, err
	}
	if c.useTLS {
		if t, ok := nc.(*net.TCPConn); ok {
			_ = t.SetNoDelay(true)
		}
		tc := tls.Client(nc, c.tlsConfig)
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, err
		}
		nc = tc
	} else if t, ok := nc.(*net.TCPConn); ok {
		_ = t.SetNoDelay(true)
	}
	cn := &conn{c: nc}
	if len(c.authToken) > 0 {
		if err := c.authConn(ctx, cn, deadline); err != nil {
			cn.c.Close()
			return nil, err
		}
	}
	return cn, nil
}

// authConn performs the AUTH exchange on a fresh connection under the
// operation deadline and the caller's cancellation.
func (c *Client) authConn(ctx context.Context, cn *conn, deadline time.Time) error {
	req := make([]byte, 7, 7+len(c.authToken))
	req[0] = opAuth
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(c.authToken)))
	req = append(req, c.authToken...)
	_ = cn.c.SetDeadline(deadline)
	var resp [5]byte
	err := c.watchIO(ctx, cn, func() error {
		if err := writeFull(cn, req); err != nil {
			return err
		}
		return readFull(cn, resp[:])
	})
	if err != nil {
		return err
	}
	if resp[0] != statusOK {
		return ErrAuth
	}
	return nil
}

func (c *Client) request(op byte, key string, value []byte) (byte, []byte, error) {
	return c.requestCtx(context.Background(), op, key, value)
}

func (c *Client) requestCtx(ctx context.Context, op byte, key string, value []byte) (byte, []byte, error) {
	req, err := frame(op, key, value)
	if err != nil {
		return 0, nil, err
	}
	deadline, err := c.opDeadline(ctx)
	if err != nil {
		return 0, nil, err
	}
	return c.requestAt(ctx, deadline, req)
}

func (c *Client) requestFrame(req []byte) (byte, []byte, error) {
	deadline, err := c.opDeadline(context.Background())
	if err != nil {
		return 0, nil, err
	}
	return c.requestAt(context.Background(), deadline, req)
}

// requestAt runs one framed request/response exchange on a pooled
// connection under an absolute deadline. On success the connection is
// returned to the pool only after cancellation cleanup finished; any
// canceled, timed-out, partial, oversized, or otherwise uncertain exchange
// discards the socket, so unread response bytes can never become the next
// operation's response. A complete server-error frame is a normal outcome,
// not broken framing.
func (c *Client) requestAt(ctx context.Context, deadline time.Time, req []byte) (byte, []byte, error) {
	cn, err := c.getCtx(ctx, deadline)
	if err != nil {
		return 0, nil, err
	}
	keep := false
	defer func() {
		if keep {
			c.put(cn)
		} else {
			c.discard(cn)
		}
	}()
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	_ = cn.c.SetDeadline(deadline)
	status, payload, err := watchFrame(ctx, c, cn, func() (byte, []byte, error) {
		if err := writeFull(cn, req); err != nil {
			return 0, nil, err
		}
		var head [5]byte
		if err := readFull(cn, head[:]); err != nil {
			return 0, nil, err
		}
		n := binary.LittleEndian.Uint32(head[1:])
		if n > maxValue {
			return 0, nil, ErrResponseTooLarge
		}
		payload := make([]byte, n)
		if err := readFull(cn, payload); err != nil {
			return 0, nil, err
		}
		return head[0], payload, nil
	})
	if err != nil {
		return 0, nil, err
	}
	keep = true
	return status, payload, nil
}

// exchange runs one full framed exchange (write, head, payload) on an
// already-leased connection, wrapped in the cancellation watcher.
func watchFrame(ctx context.Context, c *Client, cn *conn, fn func() (byte, []byte, error)) (byte, []byte, error) {
	done := ctx.Done()
	if done == nil {
		status, payload, err := fn()
		if err != nil {
			if c.isClosed() {
				return 0, nil, fmt.Errorf("%w: request interrupted by client shutdown: %v", ErrClosed, err)
			}
			return status, payload, err
		}
		return status, payload, nil
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-done:
			_ = cn.c.Close() // abort a blocked write/read
		case <-stop:
		}
	}()
	status, payload, err := fn()
	close(stop)
	wg.Wait()
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return 0, nil, cerr
		}
		if c.isClosed() {
			return 0, nil, fmt.Errorf("%w: request interrupted by client shutdown: %v", ErrClosed, err)
		}
		return status, payload, err
	}
	// A cancel racing with a completed exchange: the watcher may have
	// closed the socket, so the outcome is treated as canceled — never as
	// a reusable success. Tests must not require a particular winner.
	return status, payload, ctx.Err()
}

// stateRequest serializes operations whose server-side ownership is tied to
// one native connection (queue deliveries, single-flight leases, and stream
// group membership) through the legacy no-context path.
func (c *Client) stateRequest(op byte, key string, value []byte) (byte, []byte, error) {
	return c.stateRequestCtx(context.Background(), op, key, value)
}

func (c *Client) stateRequestCtx(ctx context.Context, op byte, key string, value []byte) (byte, []byte, error) {
	deadline, err := c.opDeadline(ctx)
	if err != nil {
		return 0, nil, err
	}
	return c.stateRequestAt(ctx, deadline, op, key, value)
}

// stateRequestAt runs one exchange on the dedicated state connection under
// a shared absolute deadline. Queue disposition, named-consumer attachment,
// single-flight leases, and stream group membership must stay on this path:
// cancellation that discards the state socket invalidates connection-owned
// deliveries, claims, and memberships, so callers must reestablish that
// state (consume/ACK/NACK/join are never replayed automatically).
func (c *Client) stateRequestAt(ctx context.Context, deadline time.Time, op byte, key string, value []byte) (byte, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	req, err := frame(op, key, value)
	if err != nil {
		return 0, nil, err
	}
	if err := c.acquireState(ctx); err != nil {
		return 0, nil, err
	}
	defer c.releaseState()
	cn, err := c.stateLease(ctx, deadline)
	if err != nil {
		return 0, nil, err
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	_ = cn.c.SetDeadline(deadline)
	status, payload, err := watchFrame(ctx, c, cn, func() (byte, []byte, error) {
		if err := writeFull(cn, req); err != nil {
			return 0, nil, err
		}
		var head [5]byte
		if err := readFull(cn, head[:]); err != nil {
			return 0, nil, err
		}
		n := binary.LittleEndian.Uint32(head[1:])
		if n > maxValue {
			return 0, nil, ErrResponseTooLarge
		}
		p := make([]byte, n)
		if err := readFull(cn, p); err != nil {
			return 0, nil, err
		}
		return head[0], p, nil
	})
	if err != nil {
		c.stateDrop(cn)
		return 0, nil, err
	}
	return status, payload, nil
}

// acquireState takes the single state-exchange token. The wait honors the
// caller's context and client shutdown — it is never an uncancellable lock
// wait — and only one state exchange runs at a time.
func (c *Client) acquireState(ctx context.Context) error {
	select {
	case <-c.stateGate:
		return nil
	default:
	}
	select {
	case <-c.stateGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return fmt.Errorf("%w: client closed while waiting for the state connection", ErrClosed)
	}
}

// releaseState returns the state-exchange token.
func (c *Client) releaseState() {
	c.stateGate <- struct{}{}
}

// stateLease snapshots the dedicated state connection, dialing one when
// absent. Dialing runs without the lifecycle mutex; a dial racing with
// Close either registers before Close's snapshot or observes closure and
// closes itself.
func (c *Client) stateLease(ctx context.Context, deadline time.Time) (*conn, error) {
	c.lifeMu.Lock()
	if c.closed {
		c.lifeMu.Unlock()
		return nil, ErrClosed
	}
	if cn := c.stateConn; cn != nil {
		c.lifeMu.Unlock()
		return cn, nil
	}
	c.lifeMu.Unlock()
	cn, err := c.dialCtx(ctx, deadline)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if c.isClosed() {
			return nil, fmt.Errorf("%w: state dial interrupted by client shutdown: %v", ErrClosed, err)
		}
		return nil, err
	}
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	if c.closed {
		cn.c.Close()
		return nil, ErrClosed
	}
	if c.stateConn == nil {
		c.stateConn = cn
		c.active[cn] = struct{}{}
		return cn, nil
	}
	// Unreachable while the state token serializes callers; keep the
	// spare closed.
	cn.c.Close()
	return c.stateConn, nil
}

// stateDrop discards the dedicated state connection after a failure. Only
// the exchange holding the state token may replace it, so the snapshot is
// compared before clearing.
func (c *Client) stateDrop(cn *conn) {
	c.lifeMu.Lock()
	if c.stateConn == cn {
		c.stateConn = nil
	}
	delete(c.active, cn)
	c.lifeMu.Unlock()
	cn.c.Close()
}

func requireOK(status byte, what string) error {
	if status != statusOK {
		return fmt.Errorf("kuttidb: %s failed: %w", what, ErrServer)
	}
	return nil
}

type decoder struct {
	b []byte
	i int
}

func (d *decoder) bytes(n int) ([]byte, error) {
	if n < 0 || d.i+n > len(d.b) {
		return nil, fmt.Errorf("kuttidb: malformed response")
	}
	v := d.b[d.i : d.i+n]
	d.i += n
	return v, nil
}

func (d *decoder) u16() (uint16, error) {
	b, err := d.bytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}
func (d *decoder) u32() (uint32, error) {
	b, err := d.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}
func (d *decoder) u64() (uint64, error) {
	b, err := d.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}
func (d *decoder) done() error {
	if d.i != len(d.b) {
		return fmt.Errorf("kuttidb: malformed response")
	}
	return nil
}