// Package kuttidb is a Go client for the KuttiDB binary protocol.
//
// connPool shares connections across goroutines; batched ops (PutMany /
// GetMany) group up to BatchSize operations per round trip.
package kuttidb

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	opPut          = 0x01
	opGet          = 0x02
	opDelete       = 0x03
	opStats        = 0x04
	opPutTTL       = 0x05
	opAuth         = 0x06
	opCapabilities = 0x0a
	opServerInfo   = 0x0c
	opPutBatch     = 0x11
	opGetBatch     = 0x12
	opPutBatchTTL  = 0x13
	statusOK       = 0x00
	statusMiss     = 0x01
	maxKey         = (1 << 16) - 1
	maxValue       = 64 << 20
	BatchSize      = 256
	defaultPool    = 4
)

var (
	ErrKeyTooLarge      = errors.New("kuttidb: key too large")
	ErrValueTooLarge    = errors.New("kuttidb: value too large")
	ErrServer           = errors.New("kuttidb: server error")
	ErrClosed           = errors.New("kuttidb: client closed")
	ErrAuth             = errors.New("kuttidb: authentication failed")
	ErrResponseTooLarge = errors.New("kuttidb: response too large")
)

type Client struct {
	addr    string
	network string
	// Lock order: the state gate serializes one state exchange at a time and
// may be held while briefly taking lifeMu; Close takes lifeMu only and
// never waits for the state gate, so shutdown never waits for an in-flight
// state request. Neither the lifecycle mutex nor the state gate is ever
// held across dialing, AUTH, TLS handshakes, network reads/writes, or
// waiting for the state gate.
	lifeMu      sync.Mutex         // lifecycle: closure flag, active registry
	active      map[*conn]struct{} // leased connections and late dials
	done        chan struct{}      // closed once, when the client closes
	stateGate   chan struct{}      // one state exchange at a time (cap-1 token)
	stateConn   *conn
	pool        chan *conn // idle connection cache; get() dials overflow
	closed      bool
	dialTimeout time.Duration
	opTimeout   time.Duration
	authToken   []byte
	useTLS      bool
	tlsConfig   *tls.Config

	// dialOverride, when set (lifecycle tests), supplies the transport
	// instead of dialing the network. It receives the caller's context so
	// dial cancellation is observable.
	dialOverride func(ctx context.Context) (net.Conn, error)
}

// ManagedOptions configures the opt-in local lifecycle. Unix is the
// owner-only default; TCP is accepted only for a literal IPv4 loopback host.
// New/NewAuthenticated/NewTLS remain connect-only.
//
// The job-completion settings (additive, all optional) are forwarded to the
// `kuttidb ensure` allowlist; zero/false values keep the server defaults.
// JobCompletion requires the durable Queue WAL, which the managed launcher
// always provisions.
type ManagedOptions struct {
	DataDir                string
	Executable             string
	Transport              string // "unix" (default) or "tcp"
	Host                   string // TCP only; defaults to 127.0.0.1
	Port                   int    // TCP only; defaults to 7379
	IdleTimeout            time.Duration
	StartupTimeout         time.Duration
	Token                  []byte
	PoolSize               int
	JobCompletion          bool   // enable --job-completion
	JobStateMaxMemoryMB    int    // 0 = server default (64)
	JobReceiptsMaxMemoryMB int    // 0 = server default (64)
	JobReceiptsMaxCount    int    // 0 = server default (100000)
	JobReceiptRetentionMS  int64  // 0 = server default (86400000)
	JobCompletionMaxBytes  uint64 // 0 = server default (131072)
}

type conn struct {
	c net.Conn
}

// New creates a client with a connection pool of poolSize (0 = 4) connections.
func New(addr string, poolSize int) (*Client, error) {
	return newClient(addr, poolSize, nil, false, nil)
}

// NewAuthenticated creates a pooled client that authenticates every connection.
func NewAuthenticated(addr string, poolSize int, token []byte) (*Client, error) {
	if len(token) == 0 || len(token) > 1024 {
		return nil, ErrAuth
	}
	return newClient(addr, poolSize, append([]byte(nil), token...), false, nil)
}

// NewTLS creates a pooled TLS client. Config may be nil to use system roots
// and hostname verification. Token may be nil when the server has no AUTH.
func NewTLS(addr string, poolSize int, token []byte, config *tls.Config) (*Client, error) {
	if len(token) > 1024 {
		return nil, ErrAuth
	}
	if config == nil {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		config = config.Clone()
		if config.MinVersion == 0 {
			config.MinVersion = tls.VersionTLS12
		}
	}
	return newClient(addr, poolSize, append([]byte(nil), token...), true, config)
}

// NewManaged ensures and verifies one local managed instance, then eagerly
// fills its pool so it retains a native lifecycle lease.
func NewManaged(options ManagedOptions) (*Client, error) {
	if options.DataDir == "" {
		return nil, errors.New("kuttidb: managed DataDir is required")
	}
	dir, err := filepath.Abs(options.DataDir)
	if err != nil {
		return nil, err
	}
	if options.IdleTimeout <= 0 {
		options.IdleTimeout = time.Minute
	}
	if options.StartupTimeout <= 0 {
		options.StartupTimeout = 10 * time.Second
	}
	transport := options.Transport
	if transport == "" {
		transport = "unix"
	}
	if transport != "unix" && transport != "tcp" {
		return nil, errors.New("kuttidb: managed Transport must be unix or tcp")
	}
	endpoint := ""
	network := "unix"
	addr := filepath.Join(dir, "kuttidb.sock")
	if transport == "tcp" {
		host := options.Host
		if host == "" {
			host = "127.0.0.1"
		}
		ip := net.ParseIP(host)
		if ip == nil || ip.To4() == nil || ip.To4()[0] != 127 {
			return nil, errors.New("kuttidb: managed TCP requires a literal IPv4 loopback host")
		}
		port := options.Port
		if port == 0 {
			port = 7379
		}
		if port < 1 || port > 65535 {
			return nil, errors.New("kuttidb: managed TCP port is invalid")
		}
		network = "tcp"
		addr = net.JoinHostPort(host, fmt.Sprintf("%d", port))
		endpoint = "tcp:" + addr
	} else {
		endpoint = "unix:" + addr
	}
	expected, _ := os.ReadFile(filepath.Join(dir, "instance.id"))
	probe, probeErr := net.DialTimeout(network, addr, 250*time.Millisecond)
	if probeErr == nil {
		probe.Close()
	} else {
		if !errors.Is(probeErr, syscall.ECONNREFUSED) && (network != "unix" || !errors.Is(probeErr, os.ErrNotExist)) {
			return nil, fmt.Errorf("kuttidb: managed endpoint unavailable: %w", probeErr)
		}
		executable := options.Executable
		if executable == "" {
			executable = os.Getenv("KUTTIDB_SERVER")
		}
		if executable == "" {
			executable = "kuttidb"
		}
		ctx, cancel := context.WithTimeout(context.Background(), options.StartupTimeout+time.Second)
		defer cancel()
		args := []string{"ensure", "--data-dir", dir, "--listen", endpoint,
			"--idle-timeout-ms", fmt.Sprintf("%d", options.IdleTimeout.Milliseconds()),
			"--startup-timeout-ms", fmt.Sprintf("%d", options.StartupTimeout.Milliseconds()), "--json"}
		if options.JobCompletion {
			args = append(args, "--job-completion")
		}
		setting := func(flag string, v uint64) {
			if v != 0 {
				args = append(args, flag, strconv.FormatUint(v, 10))
			}
		}
		setting("--job-state-max-memory-mb", uint64(options.JobStateMaxMemoryMB))
		setting("--job-receipts-max-memory-mb", uint64(options.JobReceiptsMaxMemoryMB))
		setting("--job-receipts-max-count", uint64(options.JobReceiptsMaxCount))
		setting("--job-receipt-retention-ms", uint64(options.JobReceiptRetentionMS))
		setting("--job-completion-max-bytes", options.JobCompletionMaxBytes)
		cmd := exec.CommandContext(ctx, executable, args...)
		out, runErr := cmd.Output()
		if runErr != nil {
			return nil, fmt.Errorf("kuttidb: managed startup failed: %w", runErr)
		}
		var result struct {
			InstanceID string `json:"instance_id"`
		}
		if json.Unmarshal(out, &result) != nil || len(result.InstanceID) != 32 {
			return nil, errors.New("kuttidb: invalid managed launcher response")
		}
		expected = []byte(result.InstanceID)
	}
	if len(expected) == 0 {
		return nil, errors.New("kuttidb: managed endpoint is unverifiable")
	}
	c, err := newClientNetwork(network, addr, options.PoolSize, options.Token, false, nil)
	if err != nil {
		return nil, err
	}
	if err := c.verifyManaged(string(bytes.TrimSpace(expected))); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func newClient(addr string, poolSize int, token []byte, useTLS bool, tlsConfig *tls.Config) (*Client, error) {
	return newClientNetwork("tcp", addr, poolSize, token, useTLS, tlsConfig)
}

func newClientNetwork(network, addr string, poolSize int, token []byte, useTLS bool, tlsConfig *tls.Config) (*Client, error) {
	if poolSize <= 0 {
		poolSize = defaultPool
	}
	c := &Client{
		addr:        addr,
		network:     network,
		active:      make(map[*conn]struct{}),
		done:        make(chan struct{}),
		stateGate:   make(chan struct{}, 1),
		pool:        make(chan *conn, poolSize),
		dialTimeout: 5 * time.Second,
		opTimeout:   30 * time.Second,
		authToken:   token,
		useTLS:      useTLS,
		tlsConfig:   tlsConfig,
	}
	c.stateGate <- struct{}{}
	for i := 0; i < poolSize; i++ {
		cn, err := c.dial()
		if err != nil {
			c.Close()
			return nil, err
		}
		c.pool <- cn
	}
	return c, nil
}

func (c *Client) verifyManaged(expected string) error {
	ctx := context.Background()
	deadline, err := c.opDeadline(ctx)
	if err != nil {
		return err
	}
	cn, err := c.getCtx(ctx, deadline)
	if err != nil {
		return err
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
		return err
	}
	_ = cn.c.SetDeadline(deadline)
	err = c.watchIO(ctx, cn, func() error {
		if err := writeFull(cn, []byte{opServerInfo, 0, 0, 0, 0, 0, 0}); err != nil {
			return err
		}
		var head [5]byte
		if err := readFull(cn, head[:]); err != nil {
			return err
		}
		if head[0] != statusOK || binary.LittleEndian.Uint32(head[1:]) != 52 {
			return errors.New("kuttidb: managed server identity unavailable")
		}
		payload := make([]byte, 52)
		if err := readFull(cn, payload); err != nil {
			return err
		}
		if payload[0] != 1 || payload[1] != 32 || string(payload[2:34]) != expected {
			return errors.New("kuttidb: managed endpoint belongs to another instance")
		}
		return nil
	})
	if err != nil {
		return err
	}
	keep = true
	return nil
}

// get leases a connection with the default (no-context) path.
func (c *Client) get() (*conn, error) {
	deadline, err := c.opDeadline(context.Background())
	if err != nil {
		return nil, err
	}
	return c.getCtx(context.Background(), deadline)
}

// put returns a leased connection to the idle pool. The closed check and
// the channel send are synchronized with Close through lifeMu: a return
// that raced with Close either drains with the snapshot (send already
// ordered before Close's drain) or observes closure and discards. A
// returned connection is never reused after discard, and its obsolete
// deadline is cleared before it can be leased again.
func (c *Client) put(cn *conn) {
	c.lifeMu.Lock()
	if c.closed {
		c.lifeMu.Unlock()
		cn.c.Close()
		return
	}
	delete(c.active, cn)
	c.lifeMu.Unlock()
	_ = cn.c.SetDeadline(time.Time{})
	select {
	case c.pool <- cn:
	default:
		cn.c.Close()
	}
}

// discard closes a connection that left the pool and must not return to it
// (I/O failure, discarded response, or shutdown): it can never be reused.
func (c *Client) discard(cn *conn) {
	c.lifeMu.Lock()
	delete(c.active, cn)
	c.lifeMu.Unlock()
	cn.c.Close()
}

// isClosed reports whether the client has been closed (lifecycle mutex).
func (c *Client) isClosed() bool {
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	return c.closed
}

// readFull is defined in protocol.go.

// Put stores value under key.
func (c *Client) Put(key string, value []byte) error {
	return c.PutContext(context.Background(), key, value)
}

// PutContext stores value under key.
func (c *Client) PutContext(ctx context.Context, key string, value []byte) error {
	if len(key) > maxKey {
		return ErrKeyTooLarge
	}
	if len(value) > maxValue {
		return ErrValueTooLarge
	}
	req := make([]byte, 7, 7+len(key)+len(value))
	req[0] = opPut
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(req[3:7], uint32(len(value)))
	req = append(req, key...)
	req = append(req, value...)
	status, _, err := c.requestAt(ctx, mustDeadline(c, ctx), req)
	if err != nil {
		return err
	}
	if status != statusOK {
		return ErrServer
	}
	return nil
}

// PutWithTTL stores value under key with a time-to-live.
func (c *Client) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	return c.PutWithTTLContext(context.Background(), key, value, ttl)
}

// PutWithTTLContext stores value under key with a time-to-live.
func (c *Client) PutWithTTLContext(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if len(key) > maxKey {
		return ErrKeyTooLarge
	}
	if len(value) > maxValue {
		return ErrValueTooLarge
	}
	ttlMs := uint32(ttl.Milliseconds())
	if ttlMs == 0 {
		ttlMs = 1
	}
	req := make([]byte, 11, 11+len(key)+len(value))
	req[0] = opPutTTL
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(req[3:7], uint32(len(value)))
	binary.LittleEndian.PutUint32(req[7:11], ttlMs)
	req = append(req, key...)
	req = append(req, value...)
	status, _, err := c.requestAt(ctx, mustDeadline(c, ctx), req)
	if err != nil {
		return err
	}
	if status != statusOK {
		return ErrServer
	}
	return nil
}

// Get returns nil, nil on miss.
func (c *Client) Get(key string) ([]byte, error) {
	return c.GetContext(context.Background(), key)
}

// GetContext returns nil, nil on miss.
func (c *Client) GetContext(ctx context.Context, key string) ([]byte, error) {
	if len(key) > maxKey {
		return nil, ErrKeyTooLarge
	}
	status, value, err := c.requestCtx(ctx, opGet, key, nil)
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if status != statusOK {
		return nil, ErrServer
	}
	return value, nil
}

// Delete reports whether the key existed.
func (c *Client) Delete(key string) (bool, error) {
	return c.DeleteContext(context.Background(), key)
}

// DeleteContext reports whether the key existed.
func (c *Client) DeleteContext(ctx context.Context, key string) (bool, error) {
	if len(key) > maxKey {
		return false, ErrKeyTooLarge
	}
	status, _, err := c.requestCtx(ctx, opDelete, key, nil)
	if err != nil {
		return false, err
	}
	return status == statusOK, nil
}

// Stats returns the server STATS JSON payload.
func (c *Client) Stats() ([]byte, error) {
	return c.StatsContext(context.Background())
}

// StatsContext returns the server STATS JSON payload.
func (c *Client) StatsContext(ctx context.Context) ([]byte, error) {
	_, value, err := c.requestCtx(ctx, opStats, "", nil)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// PutMany writes pairs in batches of BatchSize (one round trip each).
func (c *Client) PutMany(pairs map[string][]byte) error {
	return c.PutManyContext(context.Background(), pairs)
}

// PutManyContext writes pairs in batches of BatchSize; every chunk shares
// the operation's single deadline budget.
func (c *Client) PutManyContext(ctx context.Context, pairs map[string][]byte) error {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	deadline, err := c.opDeadline(ctx)
	if err != nil {
		return err
	}
	for start := 0; start < len(keys); start += BatchSize {
		end := start + BatchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]
		req, err := putBatchFrame(chunk, pairs)
		if err != nil {
			return err
		}
		status, _, err := c.requestAt(ctx, deadline, req)
		if err != nil {
			return err
		}
		if status != statusOK {
			return ErrServer
		}
	}
	return nil
}

// putBatchFrame builds one opPutBatch request chunk.
func putBatchFrame(chunk []string, pairs map[string][]byte) ([]byte, error) {
	size := 7
	for _, k := range chunk {
		if len(k) > maxKey {
			return nil, ErrKeyTooLarge
		}
		if len(pairs[k]) > maxValue {
			return nil, ErrValueTooLarge
		}
		itemSize := 6 + len(k) + len(pairs[k])
		if itemSize > maxValue-size {
			return nil, ErrValueTooLarge
		}
		size += itemSize
	}
	req := make([]byte, 0, size)
	req = append(req, opPutBatch, 0, 0)
	req = appendU32(req, uint32(len(chunk)))
	for _, k := range chunk {
		req = appendU16(req, uint16(len(k)))
		req = appendU32(req, uint32(len(pairs[k])))
		req = append(req, k...)
		req = append(req, pairs[k]...)
	}
	return req, nil
}

// Item is a key/value pair with optional TTL for PutManyTTL.
type Item struct {
	Key   string
	Value []byte
	TTL   time.Duration // 0 = no expiry
}

// PutManyTTL writes items in batches of BatchSize; per-item TTL in
// milliseconds on the wire (0 = no expiry). One round trip per batch.
func (c *Client) PutManyTTL(items []Item) error {
	return c.PutManyTTLContext(context.Background(), items)
}

// PutManyTTLContext writes items in batches of BatchSize; every chunk
// shares the operation's single deadline budget.
func (c *Client) PutManyTTLContext(ctx context.Context, items []Item) error {
	deadline, err := c.opDeadline(ctx)
	if err != nil {
		return err
	}
	for start := 0; start < len(items); start += BatchSize {
		end := start + BatchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[start:end]
		req, err := putBatchTTLFrame(chunk)
		if err != nil {
			return err
		}
		status, _, err := c.requestAt(ctx, deadline, req)
		if err != nil {
			return err
		}
		if status != statusOK {
			return ErrServer
		}
	}
	return nil
}

// putBatchTTLFrame builds one opPutBatchTTL request chunk.
func putBatchTTLFrame(chunk []Item) ([]byte, error) {
	size := 7
	for _, it := range chunk {
		if len(it.Key) > maxKey {
			return nil, ErrKeyTooLarge
		}
		if len(it.Value) > maxValue {
			return nil, ErrValueTooLarge
		}
		itemSize := 10 + len(it.Key) + len(it.Value)
		if itemSize > maxValue-size {
			return nil, ErrValueTooLarge
		}
		size += itemSize
	}
	req := make([]byte, 0, size)
	req = append(req, opPutBatchTTL, 0, 0)
	req = appendU32(req, uint32(len(chunk)))
	for _, it := range chunk {
		req = appendU16(req, uint16(len(it.Key)))
		req = appendU32(req, uint32(len(it.Value)))
		ttlMs := uint32(it.TTL.Milliseconds())
		if it.TTL > 0 && ttlMs == 0 {
			ttlMs = 1
		}
		req = appendU32(req, ttlMs)
		req = append(req, it.Key...)
		req = append(req, it.Value...)
	}
	return req, nil
}

// GetMany fetches keys in batches of BatchSize; misses are nil entries.
func (c *Client) GetMany(keys []string) ([][]byte, error) {
	return c.GetManyContext(context.Background(), keys)
}

// GetManyContext fetches keys in batches of BatchSize; every chunk and
// response segment shares the operation's single deadline budget, and a
// canceled or partially read chunk discards its connection so trailing
// bytes can never become the next operation's response.
func (c *Client) GetManyContext(ctx context.Context, keys []string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	deadline, err := c.opDeadline(ctx)
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(keys); start += BatchSize {
		end := start + BatchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]
		req, err := getBatchFrame(chunk)
		if err != nil {
			return nil, err
		}
		cn, err := c.getCtx(ctx, deadline)
		if err != nil {
			return nil, err
		}
		err = func() error {
			keep := false
			defer func() {
				if keep {
					c.put(cn)
				} else {
					c.discard(cn)
				}
			}()
			if err := ctx.Err(); err != nil {
				return err
			}
			_ = cn.c.SetDeadline(deadline)
			xerr := c.watchIO(ctx, cn, func() error {
				if err := writeFull(cn, req); err != nil {
					return err
				}
				var rcount [4]byte
				if err := readFull(cn, rcount[:]); err != nil {
					return err
				}
				n := binary.LittleEndian.Uint32(rcount[:])
				for i := 0; i < int(n); i++ {
					var sh [5]byte
					if err := readFull(cn, sh[:]); err != nil {
						return err
					}
					vlen := binary.LittleEndian.Uint32(sh[1:5])
					if vlen > maxValue {
						return ErrResponseTooLarge
					}
					if sh[0] == statusOK && vlen > 0 {
						val := make([]byte, vlen)
						if err := readFull(cn, val); err != nil {
							return err
						}
						result[start+i] = val
					}
				}
				return nil
			})
			if xerr == nil {
				keep = true
			}
			return xerr
		}()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// getBatchFrame builds one opGetBatch request chunk.
func getBatchFrame(chunk []string) ([]byte, error) {
	size := 7
	for _, k := range chunk {
		if len(k) > maxKey {
			return nil, ErrKeyTooLarge
		}
		size += 2 + len(k)
	}
	req := make([]byte, 0, size)
	req = append(req, opGetBatch, 0, 0)
	req = appendU32(req, uint32(len(chunk)))
	for _, k := range chunk {
		req = appendU16(req, uint16(len(k)))
		req = append(req, k...)
	}
	return req, nil
}

// mustDeadline computes the operation deadline or surfaces context state.
func mustDeadline(c *Client, ctx context.Context) time.Time {
	d, err := c.opDeadline(ctx)
	if err != nil {
		// The context is already done; requestAt will surface it before
		// any I/O, so a zero time is safe here.
		return time.Time{}
	}
	return d
}

// Close terminates the client: it marks closure before any further lease,
// wakes shutdown watchers, and interrupts active network I/O by closing
// every live socket. It never waits for the 30-second read timeout and
// never acquires stateMu, so it does not wait for a state request to
// finish first. Close is idempotent and concurrency-safe; drained sockets
// are closed outside the lifecycle lock.
func (c *Client) Close() {
	c.lifeMu.Lock()
	if c.closed {
		c.lifeMu.Unlock()
		return
	}
	c.closed = true
	close(c.done)
	drained := make([]*conn, 0, len(c.pool))
drain:
	for {
		select {
		case cn := <-c.pool:
			drained = append(drained, cn)
		default:
			break drain
		}
	}
	leased := make([]*conn, 0, len(c.active))
	for cn := range c.active {
		leased = append(leased, cn)
	}
	c.active = make(map[*conn]struct{})
	c.stateConn = nil
	c.lifeMu.Unlock()
	for _, cn := range leased {
		cn.c.Close()
	}
	for _, cn := range drained {
		cn.c.Close()
	}
}
