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
	addr        string
	network     string
	mu          sync.Mutex
	stateMu     sync.Mutex
	stateConn   *conn
	pool        chan *conn
	closed      bool
	dialTimeout time.Duration
	opTimeout   time.Duration
	authToken   []byte
	useTLS      bool
	tlsConfig   *tls.Config
}

// ManagedOptions configures the opt-in local lifecycle. Unix is the
// owner-only default; TCP is accepted only for a literal IPv4 loopback host.
// New/NewAuthenticated/NewTLS remain connect-only.
type ManagedOptions struct {
	DataDir        string
	Executable     string
	Transport      string // "unix" (default) or "tcp"
	Host           string // TCP only; defaults to 127.0.0.1
	Port           int    // TCP only; defaults to 7379
	IdleTimeout    time.Duration
	StartupTimeout time.Duration
	Token          []byte
	PoolSize       int
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
		cmd := exec.CommandContext(ctx, executable, "ensure", "--data-dir", dir, "--listen", endpoint,
			"--idle-timeout-ms", fmt.Sprintf("%d", options.IdleTimeout.Milliseconds()),
			"--startup-timeout-ms", fmt.Sprintf("%d", options.StartupTimeout.Milliseconds()), "--json")
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
		pool:        make(chan *conn, poolSize),
		dialTimeout: 5 * time.Second,
		opTimeout:   30 * time.Second,
		authToken:   token,
		useTLS:      useTLS,
		tlsConfig:   tlsConfig,
	}
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

func (c *Client) dial() (*conn, error) {
	var nc net.Conn
	var err error
	if c.useTLS {
		dialer := &net.Dialer{Timeout: c.dialTimeout}
		nc, err = tls.DialWithDialer(dialer, "tcp", c.addr, c.tlsConfig)
	} else {
		nc, err = net.DialTimeout(c.network, c.addr, c.dialTimeout)
	}
	if err != nil {
		return nil, err
	}
	if t, ok := nc.(*net.TCPConn); ok {
		_ = t.SetNoDelay(true)
	}
	cn := &conn{c: nc}
	if len(c.authToken) > 0 {
		req := make([]byte, 7, 7+len(c.authToken))
		req[0] = opAuth
		binary.LittleEndian.PutUint16(req[1:3], uint16(len(c.authToken)))
		req = append(req, c.authToken...)
		_ = nc.SetWriteDeadline(time.Now().Add(c.opTimeout))
		if _, err := nc.Write(req); err != nil {
			nc.Close()
			return nil, err
		}
		var resp [5]byte
		if err := readFull(cn, resp[:]); err != nil {
			nc.Close()
			return nil, err
		}
		if resp[0] != statusOK {
			nc.Close()
			return nil, ErrAuth
		}
	}
	return cn, nil
}

func (c *Client) verifyManaged(expected string) error {
	cn, err := c.get()
	if err != nil {
		return err
	}
	defer c.put(cn)
	if _, err = cn.c.Write([]byte{opServerInfo, 0, 0, 0, 0, 0, 0}); err != nil {
		return err
	}
	var head [5]byte
	if err = readFull(cn, head[:]); err != nil {
		return err
	}
	if head[0] != statusOK || binary.LittleEndian.Uint32(head[1:]) != 52 {
		return errors.New("kuttidb: managed server identity unavailable")
	}
	payload := make([]byte, 52)
	if err = readFull(cn, payload); err != nil {
		return err
	}
	if payload[0] != 1 || payload[1] != 32 || string(payload[2:34]) != expected {
		return errors.New("kuttidb: managed endpoint belongs to another instance")
	}
	return nil
}

func (c *Client) get() (*conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	select {
	case cn := <-c.pool:
		return cn, nil
	default:
		return c.dial()
	}
}

func (c *Client) put(cn *conn) {
	select {
	case c.pool <- cn:
	default:
		cn.c.Close()
	}
}

func readFull(cn *conn, buf []byte) error {
	_ = cn.c.SetReadDeadline(time.Now().Add(30 * time.Second))
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

// Put stores value under key.
func (c *Client) Put(key string, value []byte) error {
	if len(key) > maxKey {
		return ErrKeyTooLarge
	}
	if len(value) > maxValue {
		return ErrValueTooLarge
	}
	cn, err := c.get()
	if err != nil {
		return err
	}
	req := make([]byte, 7, 7+len(key)+len(value))
	req[0] = opPut
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(req[3:7], uint32(len(value)))
	req = append(req, key...)
	req = append(req, value...)
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err := cn.c.Write(req); err != nil {
		cn.c.Close()
		return err
	}
	resp := make([]byte, 5)
	if err := readFull(cn, resp); err != nil {
		cn.c.Close()
		return err
	}
	c.put(cn)
	if resp[0] != statusOK {
		return ErrServer
	}
	return nil
}

// PutWithTTL stores value under key with a time-to-live.
func (c *Client) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	if len(key) > maxKey {
		return ErrKeyTooLarge
	}
	if len(value) > maxValue {
		return ErrValueTooLarge
	}
	cn, err := c.get()
	if err != nil {
		return err
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
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err := cn.c.Write(req); err != nil {
		cn.c.Close()
		return err
	}
	resp := make([]byte, 5)
	if err := readFull(cn, resp); err != nil {
		cn.c.Close()
		return err
	}
	c.put(cn)
	if resp[0] != statusOK {
		return ErrServer
	}
	return nil
}

// Get returns nil, nil on miss.
func (c *Client) Get(key string) ([]byte, error) {
	if len(key) > maxKey {
		return nil, ErrKeyTooLarge
	}
	cn, err := c.get()
	if err != nil {
		return nil, err
	}
	req := make([]byte, 7, 7+len(key))
	req[0] = opGet
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	req = append(req, key...)
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err := cn.c.Write(req); err != nil {
		cn.c.Close()
		return nil, err
	}
	head := make([]byte, 5)
	if err := readFull(cn, head); err != nil {
		cn.c.Close()
		return nil, err
	}
	vlen := binary.LittleEndian.Uint32(head[1:5])
	if vlen > maxValue {
		cn.c.Close()
		return nil, ErrResponseTooLarge
	}
	var val []byte
	if vlen > 0 {
		val = make([]byte, vlen)
		if err := readFull(cn, val); err != nil {
			cn.c.Close()
			return nil, err
		}
	}
	c.put(cn)
	if head[0] == statusMiss {
		return nil, nil
	}
	if head[0] != statusOK {
		return nil, ErrServer
	}
	return val, nil
}

// Delete reports whether the key existed.
func (c *Client) Delete(key string) (bool, error) {
	if len(key) > maxKey {
		return false, ErrKeyTooLarge
	}
	cn, err := c.get()
	if err != nil {
		return false, err
	}
	req := make([]byte, 7, 7+len(key))
	req[0] = opDelete
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	req = append(req, key...)
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err := cn.c.Write(req); err != nil {
		cn.c.Close()
		return false, err
	}
	head := make([]byte, 5)
	if err := readFull(cn, head); err != nil {
		cn.c.Close()
		return false, err
	}
	c.put(cn)
	return head[0] == statusOK, nil
}

// Stats returns the server STATS JSON payload.
func (c *Client) Stats() ([]byte, error) {
	cn, err := c.get()
	if err != nil {
		return nil, err
	}
	req := []byte{opStats, 0, 0, 0, 0, 0, 0}
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err := cn.c.Write(req); err != nil {
		cn.c.Close()
		return nil, err
	}
	head := make([]byte, 5)
	if err := readFull(cn, head); err != nil {
		cn.c.Close()
		return nil, err
	}
	vlen := binary.LittleEndian.Uint32(head[1:5])
	if vlen > maxValue {
		cn.c.Close()
		return nil, ErrResponseTooLarge
	}
	val := make([]byte, vlen)
	if err := readFull(cn, val); err != nil {
		cn.c.Close()
		return nil, err
	}
	c.put(cn)
	return val, nil
}

// PutMany writes pairs in batches of BatchSize (one round trip each).
func (c *Client) PutMany(pairs map[string][]byte) error {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	for start := 0; start < len(keys); start += BatchSize {
		end := start + BatchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]
		cn, err := c.get()
		if err != nil {
			return err
		}
		size := 7
		for _, k := range chunk {
			if len(k) > maxKey {
				return ErrKeyTooLarge
			}
			if len(pairs[k]) > maxValue {
				return ErrValueTooLarge
			}
			itemSize := 6 + len(k) + len(pairs[k])
			if itemSize > maxValue-size {
				return ErrValueTooLarge
			}
			size += itemSize
		}
		req := make([]byte, 0, size)
		req = append(req, opPutBatch, 0, 0)
		var cnt [4]byte
		binary.LittleEndian.PutUint32(cnt[:], uint32(len(chunk)))
		req = append(req, cnt[:]...)
		for _, k := range chunk {
			var h [6]byte
			binary.LittleEndian.PutUint16(h[0:2], uint16(len(k)))
			binary.LittleEndian.PutUint32(h[2:6], uint32(len(pairs[k])))
			req = append(req, h[:]...)
			req = append(req, k...)
			req = append(req, pairs[k]...)
		}
		_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
		if _, err := cn.c.Write(req); err != nil {
			cn.c.Close()
			return err
		}
		resp := make([]byte, 1)
		if err := readFull(cn, resp); err != nil {
			cn.c.Close()
			return err
		}
		c.put(cn)
		if resp[0] != statusOK {
			return ErrServer
		}
	}
	return nil
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
	for start := 0; start < len(items); start += BatchSize {
		end := start + BatchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[start:end]
		cn, err := c.get()
		if err != nil {
			return err
		}
		size := 7
		for _, it := range chunk {
			if len(it.Key) > maxKey {
				return ErrKeyTooLarge
			}
			if len(it.Value) > maxValue {
				return ErrValueTooLarge
			}
			itemSize := 10 + len(it.Key) + len(it.Value)
			if itemSize > maxValue-size {
				return ErrValueTooLarge
			}
			size += itemSize
		}
		req := make([]byte, 0, size)
		req = append(req, opPutBatchTTL, 0, 0)
		var cnt [4]byte
		binary.LittleEndian.PutUint32(cnt[:], uint32(len(chunk)))
		req = append(req, cnt[:]...)
		for _, it := range chunk {
			var h [10]byte
			binary.LittleEndian.PutUint16(h[0:2], uint16(len(it.Key)))
			binary.LittleEndian.PutUint32(h[2:6], uint32(len(it.Value)))
			ttlMs := uint32(it.TTL.Milliseconds())
			if it.TTL > 0 && ttlMs == 0 {
				ttlMs = 1
			}
			binary.LittleEndian.PutUint32(h[6:10], ttlMs)
			req = append(req, h[:]...)
			req = append(req, it.Key...)
			req = append(req, it.Value...)
		}
		_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
		if _, err := cn.c.Write(req); err != nil {
			cn.c.Close()
			return err
		}
		resp := make([]byte, 1)
		if err := readFull(cn, resp); err != nil {
			cn.c.Close()
			return err
		}
		c.put(cn)
		if resp[0] != statusOK {
			return ErrServer
		}
	}
	return nil
}

// GetMany fetches keys in batches of BatchSize; misses are nil entries.
func (c *Client) GetMany(keys []string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	for start := 0; start < len(keys); start += BatchSize {
		end := start + BatchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]
		cn, err := c.get()
		if err != nil {
			return nil, err
		}
		size := 7
		for _, k := range chunk {
			if len(k) > maxKey {
				return nil, ErrKeyTooLarge
			}
			size += 2 + len(k)
		}
		req := make([]byte, 0, size)
		req = append(req, opGetBatch, 0, 0)
		var cnt [4]byte
		binary.LittleEndian.PutUint32(cnt[:], uint32(len(chunk)))
		req = append(req, cnt[:]...)
		for _, k := range chunk {
			var h [2]byte
			binary.LittleEndian.PutUint16(h[0:2], uint16(len(k)))
			req = append(req, h[:]...)
			req = append(req, k...)
		}
		_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
		if _, err := cn.c.Write(req); err != nil {
			cn.c.Close()
			return nil, err
		}
		var rcount [4]byte
		if err := readFull(cn, rcount[:]); err != nil {
			cn.c.Close()
			return nil, err
		}
		n := binary.LittleEndian.Uint32(rcount[:])
		for i := 0; i < int(n); i++ {
			var sh [5]byte
			if err := readFull(cn, sh[:]); err != nil {
				cn.c.Close()
				return nil, err
			}
			vlen := binary.LittleEndian.Uint32(sh[1:5])
			if vlen > maxValue {
				cn.c.Close()
				return nil, ErrResponseTooLarge
			}
			if sh[0] == statusOK && vlen > 0 {
				val := make([]byte, vlen)
				if err := readFull(cn, val); err != nil {
					cn.c.Close()
					return nil, err
				}
				result[start+i] = val
			}
		}
		c.put(cn)
	}
	return result, nil
}

// Close terminates all pooled connections.
func (c *Client) Close() {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.stateConn != nil {
		c.stateConn.c.Close()
		c.stateConn = nil
	}
	close(c.pool)
	for cn := range c.pool {
		cn.c.Close()
	}
	c.mu.Unlock()
}
