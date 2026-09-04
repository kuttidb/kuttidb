package kuttidb

import (
	"encoding/binary"
	"fmt"
	"time"
)

type ExchangeType byte

const (
	ExchangeDirect ExchangeType = iota
	ExchangeFanout
	ExchangeTopic
)

type ExchangeOptions struct {
	Type              ExchangeType
	Durable           bool
	AlternateExchange string
}

func (c *Client) ExchangeDeclare(name string, options ExchangeOptions) error {
	if len(name) > 255 || options.Type > ExchangeTopic {
		return fmt.Errorf("kuttidb: invalid exchange")
	}
	v := []byte{0, byte(options.Type)}
	if options.Durable {
		v[0] = 1
	}
	if options.AlternateExchange != "" {
		if len(options.AlternateExchange) > 255 {
			return fmt.Errorf("kuttidb: invalid alternate exchange")
		}
		ext := appendU16(nil, uint16(len(options.AlternateExchange)))
		ext = append(ext, options.AlternateExchange...)
		v = appendU16(v, uint16(len(ext)))
		v = append(v, ext...)
	}
	s, _, e := c.request(opExchangeDeclare, name, v)
	if e != nil {
		return e
	}
	return requireOK(s, "exchange declare")
}
func exchangeBinding(queue, routingKey string) ([]byte, error) {
	if queue == "" || len(queue) > 255 || len(routingKey) > 65535 {
		return nil, fmt.Errorf("kuttidb: invalid binding")
	}
	p := appendU16(nil, uint16(len(queue)))
	p = append(p, queue...)
	p = appendU16(p, uint16(len(routingKey)))
	p = append(p, routingKey...)
	return p, nil
}
func (c *Client) ExchangeBind(exchange, queue, routingKey string) error {
	p, e := exchangeBinding(queue, routingKey)
	if e != nil {
		return e
	}
	s, _, e := c.request(opExchangeBind, exchange, p)
	if e != nil {
		return e
	}
	return requireOK(s, "exchange bind")
}
func (c *Client) ExchangeUnbind(exchange, queue, routingKey string) (bool, error) {
	p, e := exchangeBinding(queue, routingKey)
	if e != nil {
		return false, e
	}
	s, _, e := c.request(opExchangeUnbind, exchange, p)
	if e != nil {
		return false, e
	}
	if s == statusError {
		return false, ErrServer
	}
	return s == statusOK, nil
}
func (c *Client) ExchangePublish(exchange, routingKey string, value []byte, ttl time.Duration) (uint32, error) {
	ms, e := milliseconds(ttl, true)
	if e != nil {
		return 0, e
	}
	if len(routingKey) > 65535 {
		return 0, fmt.Errorf("kuttidb: routing key too large")
	}
	p := appendU16(nil, uint16(len(routingKey)))
	p = appendU64(p, ms)
	p = append(p, routingKey...)
	p = append(p, value...)
	s, v, e := c.request(opExchangePublish, exchange, p)
	if e != nil {
		return 0, e
	}
	if s == statusMiss {
		return 0, nil
	}
	if e = requireOK(s, "exchange publish"); e != nil {
		return 0, e
	}
	if len(v) != 4 {
		return 0, fmt.Errorf("kuttidb: malformed exchange response")
	}
	return binary.LittleEndian.Uint32(v), nil
}

type AtomicResult struct {
	TransactionID uint64
	Routed        uint32
	Unroutable    bool
}

func (c *Client) atomic(op byte, key string, p []byte) (AtomicResult, error) {
	s, v, e := c.request(op, key, p)
	if e != nil {
		return AtomicResult{}, e
	}
	if s == statusMiss {
		return AtomicResult{Unroutable: true}, nil
	}
	if e = requireOK(s, "atomic operation"); e != nil {
		return AtomicResult{}, e
	}
	if len(v) != 12 {
		return AtomicResult{}, fmt.Errorf("kuttidb: malformed atomic response")
	}
	return AtomicResult{binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint32(v[8:]), false}, nil
}
func atomicExchangePayload(exchange, routingKey string, ttl time.Duration, value []byte) ([]byte, error) {
	if len(exchange) > 65535 || len(routingKey) > 65535 {
		return nil, fmt.Errorf("kuttidb: atomic route too large")
	}
	ms, e := milliseconds(ttl, true)
	if e != nil || ms > 0xffffffff {
		if e != nil {
			return nil, e
		}
		return nil, fmt.Errorf("kuttidb: TTL too large")
	}
	p := appendU16(nil, uint16(len(exchange)))
	p = append(p, exchange...)
	p = appendU16(p, uint16(len(routingKey)))
	p = append(p, routingKey...)
	p = appendU32(p, uint32(ms))
	p = append(p, value...)
	return p, nil
}
func (c *Client) PutAndPublish(key string, value []byte, exchange, routingKey string, ttl time.Duration) (AtomicResult, error) {
	p, e := atomicExchangePayload(exchange, routingKey, ttl, value)
	if e != nil {
		return AtomicResult{}, e
	}
	return c.atomic(opAtomicPutPublish, key, p)
}
func (c *Client) UpdateAndEmit(key string, value []byte, exchange, routingKey string, ttl time.Duration) (AtomicResult, error) {
	p, e := atomicExchangePayload(exchange, routingKey, ttl, value)
	if e != nil {
		return AtomicResult{}, e
	}
	return c.atomic(opAtomicUpdateEmit, key, p)
}
func (c *Client) PutAndEnqueue(key string, value []byte, queue string, ttl time.Duration) (AtomicResult, error) {
	if len(queue) > 65535 {
		return AtomicResult{}, fmt.Errorf("kuttidb: queue name too large")
	}
	ms, e := milliseconds(ttl, true)
	if e != nil || ms > 0xffffffff {
		if e != nil {
			return AtomicResult{}, e
		}
		return AtomicResult{}, fmt.Errorf("kuttidb: TTL too large")
	}
	p := appendU16(nil, uint16(len(queue)))
	p = append(p, queue...)
	p = appendU32(p, uint32(ms))
	p = append(p, value...)
	return c.atomic(opAtomicPutEnqueue, key, p)
}
func (c *Client) DeleteAndPublish(key, exchange, routingKey string, message []byte) (AtomicResult, error) {
	if len(exchange) > 65535 || len(routingKey) > 65535 {
		return AtomicResult{}, fmt.Errorf("kuttidb: atomic route too large")
	}
	p := appendU16(nil, uint16(len(exchange)))
	p = append(p, exchange...)
	p = appendU16(p, uint16(len(routingKey)))
	p = append(p, routingKey...)
	p = appendU32(p, uint32(len(message)))
	p = append(p, message...)
	return c.atomic(opAtomicDeletePublish, key, p)
}

type SingleFlightState byte

const (
	StateValue SingleFlightState = iota
	StateClaimed
	StateWait
	StateNegative
	StateReleased
	StateTimeout
	StateLost
	StateStale
	StateRefresh
)

type SingleFlightResult struct {
	State  SingleFlightState
	Holder bool
	Value  []byte
}

func decodeSingleFlight(s byte, v []byte, holder bool) (SingleFlightResult, error) {
	if e := requireOK(s, "single-flight operation"); e != nil {
		return SingleFlightResult{}, e
	}
	minimum := 1
	if holder {
		minimum = 2
	}
	if len(v) < minimum {
		return SingleFlightResult{}, fmt.Errorf("kuttidb: malformed single-flight response")
	}
	r := SingleFlightResult{State: SingleFlightState(v[0])}
	at := 1
	if holder {
		r.Holder = v[1] != 0
		at = 2
	}
	if r.State == StateValue || r.State == StateStale || r.State == StateRefresh {
		r.Value = append([]byte(nil), v[at:]...)
	}
	return r, nil
}
func (c *Client) GetOrClaim(key string, lease time.Duration) (SingleFlightResult, error) {
	ms, e := milliseconds(lease, false)
	if e != nil || ms > 60000 {
		return SingleFlightResult{}, fmt.Errorf("kuttidb: lease must be 0..60 seconds")
	}
	s, v, e := c.stateRequest(opGetOrClaim, key, appendU32(nil, uint32(ms)))
	if e != nil {
		return SingleFlightResult{}, e
	}
	return decodeSingleFlight(s, v, false)
}
func (c *Client) WaitForKey(key string, timeout time.Duration) (SingleFlightResult, error) {
	ms, e := milliseconds(timeout, false)
	if e != nil || ms > 60000 {
		return SingleFlightResult{}, fmt.Errorf("kuttidb: timeout must be 0..60 seconds")
	}
	s, v, e := c.stateRequest(opWaitForKey, key, appendU32(nil, uint32(ms)))
	if e != nil {
		return SingleFlightResult{}, e
	}
	return decodeSingleFlight(s, v, false)
}
func (c *Client) PutAndRelease(key string, value []byte, ttl time.Duration, negative bool) error {
	ms, e := milliseconds(ttl, true)
	if e != nil || ms > 0xffffffff {
		return fmt.Errorf("kuttidb: invalid TTL")
	}
	p := appendU32(nil, uint32(ms))
	if negative {
		p = append(p, 1)
	} else {
		p = append(p, 0)
	}
	p = append(p, value...)
	s, _, e := c.stateRequest(opPutAndRelease, key, p)
	if e != nil {
		return e
	}
	return requireOK(s, "put and release")
}
func (c *Client) ReleaseClaim(key string) error {
	s, _, e := c.stateRequest(opReleaseClaim, key, nil)
	if e != nil {
		return e
	}
	return requireOK(s, "release claim")
}
func (c *Client) GetOrRefresh(key string, lease time.Duration) (SingleFlightResult, error) {
	if e := c.requireFeature(FeatureSWR, "stale-while-revalidate"); e != nil {
		return SingleFlightResult{}, e
	}
	ms, e := milliseconds(lease, false)
	if e != nil || ms > 60000 {
		return SingleFlightResult{}, fmt.Errorf("kuttidb: lease must be 0..60 seconds")
	}
	s, v, e := c.stateRequest(opGetOrRefresh, key, appendU32(nil, uint32(ms)))
	if e != nil {
		return SingleFlightResult{}, e
	}
	return decodeSingleFlight(s, v, true)
}

func (c *Client) PutSWR(key string, value []byte, ttl, staleFor, refreshAfter time.Duration) error {
	if e := c.requireFeature(FeatureSWR, "stale-while-revalidate"); e != nil {
		return e
	}
	tm, e := milliseconds(ttl, false)
	if e != nil {
		return e
	}
	sm, e := milliseconds(staleFor, false)
	if e != nil {
		return e
	}
	rm, e := milliseconds(refreshAfter, true)
	if e != nil {
		return e
	}
	const week = 7 * 24 * 60 * 60 * 1000
	if tm == 0 || sm == 0 || tm > 0xffffffff || sm > week || rm > week {
		return fmt.Errorf("kuttidb: invalid SWR window")
	}
	if len(key) > maxKey {
		return ErrKeyTooLarge
	}
	if len(value) > maxValue {
		return ErrValueTooLarge
	}
	req := make([]byte, 19, 19+len(key)+len(value))
	req[0] = opPutSWR
	binary.LittleEndian.PutUint16(req[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(req[3:7], uint32(len(value)))
	binary.LittleEndian.PutUint32(req[7:11], uint32(tm))
	binary.LittleEndian.PutUint32(req[11:15], uint32(sm))
	binary.LittleEndian.PutUint32(req[15:19], uint32(rm))
	req = append(req, key...)
	req = append(req, value...)
	s, _, e := c.requestFrame(req)
	if e != nil {
		return e
	}
	return requireOK(s, "put SWR")
}

type Loader func() ([]byte, error)

func (c *Client) GetOrLoad(key string, loader Loader, ttl, lease, wait time.Duration) ([]byte, error) {
	r, e := c.GetOrClaim(key, lease)
	if e != nil {
		return nil, e
	}
	if r.State == StateValue {
		return r.Value, nil
	}
	if r.State == StateNegative {
		return nil, nil
	}
	if r.State == StateWait {
		for range 3 {
			w, e := c.WaitForKey(key, wait)
			if e != nil {
				return nil, e
			}
			if w.State == StateValue {
				return w.Value, nil
			}
			if w.State == StateNegative || w.State == StateTimeout {
				return nil, nil
			}
			r, e = c.GetOrClaim(key, lease)
			if e != nil {
				return nil, e
			}
			if r.State == StateValue {
				return r.Value, nil
			}
			if r.State == StateClaimed {
				break
			}
		}
	}
	if r.State != StateClaimed {
		return nil, nil
	}
	loaded, e := loader()
	if e != nil {
		_ = c.ReleaseClaim(key)
		return nil, e
	}
	if loaded == nil {
		return nil, c.PutAndRelease(key, nil, ttl, true)
	}
	return loaded, c.PutAndRelease(key, loaded, ttl, false)
}
func (c *Client) GetOrLoadSWR(key string, loader Loader, ttl, staleFor, refreshAfter, lease, wait time.Duration) ([]byte, error) {
	r, e := c.GetOrRefresh(key, lease)
	if e != nil {
		return nil, e
	}
	if r.State == StateValue {
		return r.Value, nil
	}
	if r.State == StateNegative {
		return nil, nil
	}
	if (r.State == StateStale || r.State == StateRefresh) && !r.Holder {
		return r.Value, nil
	}
	if r.State == StateWait {
		for range 3 {
			w, e := c.WaitForKey(key, wait)
			if e != nil {
				return nil, e
			}
			if w.State == StateValue {
				return w.Value, nil
			}
			if w.State == StateNegative || w.State == StateTimeout {
				return nil, nil
			}
			r, e = c.GetOrRefresh(key, lease)
			if e != nil {
				return nil, e
			}
			if r.State == StateValue {
				return r.Value, nil
			}
			if (r.State == StateStale || r.State == StateRefresh) && !r.Holder {
				return r.Value, nil
			}
			if r.State == StateClaimed || r.Holder {
				break
			}
		}
	}
	if r.State != StateClaimed && !r.Holder {
		return nil, nil
	}
	loaded, e := loader()
	if e != nil {
		_ = c.ReleaseClaim(key)
		return nil, e
	}
	if loaded == nil {
		return nil, c.PutAndRelease(key, nil, ttl, true)
	}
	if e = c.PutSWR(key, loaded, ttl, staleFor, refreshAfter); e != nil {
		_ = c.ReleaseClaim(key)
		return nil, e
	}
	return loaded, c.ReleaseClaim(key)
}
