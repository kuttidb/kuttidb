package kuttidb

import (
	"encoding/binary"
	"fmt"
	"io"
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

func (c *Client) request(op byte, key string, value []byte) (byte, []byte, error) {
	req, err := frame(op, key, value)
	if err != nil {
		return 0, nil, err
	}
	return c.requestFrame(req)
}

func (c *Client) requestFrame(req []byte) (byte, []byte, error) {
	cn, err := c.get()
	if err != nil {
		return 0, nil, err
	}
	keep := false
	defer func() {
		if keep {
			c.put(cn)
		} else {
			_ = cn.c.Close()
		}
	}()
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err = cn.c.Write(req); err != nil {
		return 0, nil, err
	}
	var head [5]byte
	if err = readFull(cn, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(head[1:])
	if n > maxValue {
		return 0, nil, ErrResponseTooLarge
	}
	payload := make([]byte, n)
	if _, err = io.ReadFull(cn.c, payload); err != nil {
		return 0, nil, err
	}
	keep = true
	return head[0], payload, nil
}

// stateRequest serializes operations whose server-side ownership is tied to
// one native connection (queue deliveries, single-flight leases, and stream
// group membership). The connection is replaced after an I/O failure.
func (c *Client) stateRequest(op byte, key string, value []byte) (byte, []byte, error) {
	req, err := frame(op, key, value)
	if err != nil {
		return 0, nil, err
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return 0, nil, ErrClosed
	}
	if c.stateConn == nil {
		c.stateConn, err = c.dial()
		if err != nil {
			return 0, nil, err
		}
	}
	cn := c.stateConn
	_ = cn.c.SetWriteDeadline(time.Now().Add(c.opTimeout))
	if _, err = cn.c.Write(req); err != nil {
		c.stateConn = nil
		_ = cn.c.Close()
		return 0, nil, err
	}
	var head [5]byte
	if err = readFull(cn, head[:]); err != nil {
		c.stateConn = nil
		_ = cn.c.Close()
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(head[1:])
	if n > maxValue {
		c.stateConn = nil
		_ = cn.c.Close()
		return 0, nil, ErrResponseTooLarge
	}
	payload := make([]byte, n)
	if _, err = io.ReadFull(cn.c, payload); err != nil {
		c.stateConn = nil
		_ = cn.c.Close()
		return 0, nil, err
	}
	return head[0], payload, nil
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
