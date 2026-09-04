package kuttidb

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	opHealth                  = 0x09
	opPutSWR                  = 0x0b
	opQueueDeclare            = 0x20
	opQueuePublish            = 0x21
	opQueueConsume            = 0x22
	opQueueAck                = 0x23
	opQueueNack               = 0x24
	opQueuePublishTTL         = 0x25
	opQueueStats              = 0x26
	opQueuePrefetch           = 0x27
	opQueueCancel             = 0x28
	opQueueConsumerRegister   = 0x29
	opQueueConsumerUnregister = 0x2a
	opQueueConsumeAs          = 0x2b
	opQueueList               = 0x2c
	opQueuePublishBatch       = 0x2d
	opQueueConsumeBatch       = 0x2e
	opQueueAckBatch           = 0x2f
	opExchangeDeclare         = 0x30
	opExchangeBind            = 0x31
	opExchangeUnbind          = 0x32
	opExchangePublish         = 0x33
	opAtomicPutPublish        = 0x40
	opAtomicPutEnqueue        = 0x41
	opAtomicDeletePublish     = 0x42
	opAtomicUpdateEmit        = 0x43
	opGetOrClaim              = 0x50
	opWaitForKey              = 0x51
	opPutAndRelease           = 0x52
	opReleaseClaim            = 0x53
	opGetOrRefresh            = 0x54
	opStreamDeclare           = 0x60
	opStreamAppend            = 0x61
	opStreamFetch             = 0x62
	opStreamCommit            = 0x63
	opStreamGroupOffset       = 0x64
	opStreamGroupJoin         = 0x65
	opStreamGroupLag          = 0x66
	opStreamAppendBatch       = 0x67
	opStreamGroupLeave        = 0x68
	opStreamList              = 0x69
	opStreamGroupList         = 0x6a
	opStreamCommitBatch       = 0x6b
	opStreamFetchKeys         = 0x6c
)

const (
	FeatureCache uint64 = 1 << iota
	FeatureQueues
	FeatureExchanges
	FeatureAtomic
	FeatureSingleFlight
	FeatureStreams
	FeatureStreamBatch
	FeatureHealth
	FeatureStreamGenerations
	FeatureQueueConsumers
	FeatureAtomicUpdate
	FeatureSWR
	FeatureQueueBatch
	FeatureStreamCommitBatch
	FeatureStreamKeys
	FeatureServerInfo
)

type Capabilities struct {
	Major, Minor uint16
	Features     uint64
}

func (c *Client) Capabilities() (Capabilities, error) {
	p := appendU16(nil, protocolMajor)
	p = appendU16(p, protocolMinor)
	status, value, err := c.request(opCapabilities, "", p)
	if err != nil {
		return Capabilities{}, err
	}
	if status == statusMiss {
		return Capabilities{}, fmt.Errorf("kuttidb: incompatible protocol major")
	}
	if err = requireOK(status, "capabilities"); err != nil {
		return Capabilities{}, err
	}
	if len(value) != 12 {
		return Capabilities{}, fmt.Errorf("kuttidb: malformed capabilities response")
	}
	return Capabilities{binary.LittleEndian.Uint16(value), binary.LittleEndian.Uint16(value[2:]), binary.LittleEndian.Uint64(value[4:])}, nil
}

func (c *Client) requireFeature(feature uint64, name string) error {
	caps, err := c.Capabilities()
	if err != nil {
		return err
	}
	if caps.Features&feature == 0 {
		return fmt.Errorf("kuttidb: server does not support %s", name)
	}
	return nil
}

func (c *Client) Health() (bool, error) {
	status, _, err := c.request(opHealth, "", nil)
	if err != nil {
		return false, err
	}
	if status == statusError {
		return false, nil
	}
	return status == statusOK, nil
}

type QueueOptions struct {
	Durable         bool
	MaxDepth        uint64
	DeadLetterQueue string
	MaxDeliveries   uint32
}

type QueueInfo struct {
	Name            string
	Depth, Inflight uint64
}
type QueueStats struct{ Depth, Inflight uint64 }
type Delivery struct {
	DeliveryTag, MessageID uint64
	DeliveryCount          uint32
	Redelivered            bool
	Value                  []byte
}

func (c *Client) QueueDeclare(name string, options QueueOptions) error {
	if name == "" || len(name) > 255 {
		return fmt.Errorf("kuttidb: invalid queue name")
	}
	v := []byte{0}
	if options.Durable {
		v[0] = 1
	}
	v = appendU64(v, options.MaxDepth)
	if options.DeadLetterQueue != "" {
		if len(options.DeadLetterQueue) > 255 {
			return fmt.Errorf("kuttidb: invalid dead-letter queue")
		}
		ext := appendU16(nil, uint16(len(options.DeadLetterQueue)))
		ext = append(ext, options.DeadLetterQueue...)
		ext = appendU32(ext, options.MaxDeliveries)
		v = appendU16(v, uint16(len(ext)))
		v = append(v, ext...)
	}
	status, _, err := c.stateRequest(opQueueDeclare, name, v)
	if err != nil {
		return err
	}
	return requireOK(status, "queue declare")
}

func (c *Client) QueueList() ([]QueueInfo, error) {
	status, value, err := c.stateRequest(opQueueList, "", nil)
	if err != nil {
		return nil, err
	}
	if err = requireOK(status, "queue list"); err != nil {
		return nil, err
	}
	d := decoder{b: value}
	n, err := d.u16()
	if err != nil {
		return nil, err
	}
	out := make([]QueueInfo, 0, n)
	for range n {
		l, e := d.u16()
		if e != nil {
			return nil, e
		}
		b, e := d.bytes(int(l))
		if e != nil {
			return nil, e
		}
		depth, e := d.u64()
		if e != nil {
			return nil, e
		}
		inflight, e := d.u64()
		if e != nil {
			return nil, e
		}
		out = append(out, QueueInfo{string(b), depth, inflight})
	}
	return out, d.done()
}

func (c *Client) QueuePublish(name string, value []byte, ttl time.Duration) (uint64, error) {
	op, p := byte(opQueuePublish), value
	if ttl > 0 {
		ms, err := milliseconds(ttl, false)
		if err != nil {
			return 0, err
		}
		op = opQueuePublishTTL
		p = appendU64(nil, ms)
		p = append(p, value...)
	} else if ttl < 0 {
		return 0, fmt.Errorf("kuttidb: invalid TTL")
	}
	status, response, err := c.stateRequest(op, name, p)
	if err != nil {
		return 0, err
	}
	if err = requireOK(status, "queue publish"); err != nil {
		return 0, err
	}
	if len(response) != 8 {
		return 0, fmt.Errorf("kuttidb: malformed queue publish response")
	}
	return binary.LittleEndian.Uint64(response), nil
}

func decodeDelivery(value []byte) (Delivery, error) {
	d := decoder{b: value}
	tag, e := d.u64()
	if e != nil {
		return Delivery{}, e
	}
	id, e := d.u64()
	if e != nil {
		return Delivery{}, e
	}
	flag, e := d.bytes(1)
	if e != nil {
		return Delivery{}, e
	}
	count, e := d.u32()
	if e != nil {
		return Delivery{}, e
	}
	body, e := d.bytes(len(value) - d.i)
	if e != nil {
		return Delivery{}, e
	}
	return Delivery{tag, id, count, flag[0] != 0, append([]byte(nil), body...)}, nil
}

func (c *Client) QueueConsume(name string, visibility time.Duration) (*Delivery, error) {
	ms, err := milliseconds(visibility, true)
	if err != nil {
		return nil, err
	}
	p := appendU64(nil, ms)
	status, value, err := c.stateRequest(opQueueConsume, name, p)
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = requireOK(status, "queue consume"); err != nil {
		return nil, err
	}
	v, err := decodeDelivery(value)
	return &v, err
}

func (c *Client) QueueConsumeAs(name, consumer string, visibility time.Duration) (*Delivery, error) {
	if consumer == "" || len(consumer) > 255 {
		return nil, fmt.Errorf("kuttidb: invalid consumer")
	}
	ms, err := milliseconds(visibility, true)
	if err != nil {
		return nil, err
	}
	p := appendU16(nil, uint16(len(consumer)))
	p = append(p, consumer...)
	p = appendU64(p, ms)
	status, value, err := c.stateRequest(opQueueConsumeAs, name, p)
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = requireOK(status, "queue consume as"); err != nil {
		return nil, err
	}
	v, err := decodeDelivery(value)
	return &v, err
}

func (c *Client) QueueAck(name string, tag uint64) (bool, error) {
	status, _, err := c.stateRequest(opQueueAck, name, appendU64(nil, tag))
	if err != nil {
		return false, err
	}
	if status == statusError {
		return false, ErrServer
	}
	return status == statusOK, nil
}
func (c *Client) QueueNack(name string, tag uint64, requeue bool, delay time.Duration) (bool, error) {
	ms, err := milliseconds(delay, true)
	if err != nil {
		return false, err
	}
	p := appendU64(nil, tag)
	if requeue {
		p = append(p, 1)
	} else {
		p = append(p, 0)
	}
	if ms > 0 {
		p = appendU64(p, ms)
	}
	status, _, err := c.stateRequest(opQueueNack, name, p)
	if err != nil {
		return false, err
	}
	if status == statusError {
		return false, ErrServer
	}
	return status == statusOK, nil
}
func (c *Client) QueueStats(name string) (*QueueStats, error) {
	status, v, err := c.stateRequest(opQueueStats, name, nil)
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = requireOK(status, "queue stats"); err != nil {
		return nil, err
	}
	if len(v) != 16 {
		return nil, fmt.Errorf("kuttidb: malformed queue stats")
	}
	return &QueueStats{binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint64(v[8:])}, nil
}
func (c *Client) QueuePrefetch(count uint32) error {
	status, _, err := c.stateRequest(opQueuePrefetch, "_", appendU32(nil, count))
	if err != nil {
		return err
	}
	return requireOK(status, "queue prefetch")
}
func (c *Client) QueueCancel() error {
	status, _, err := c.stateRequest(opQueueCancel, "_", nil)
	if err != nil {
		return err
	}
	return requireOK(status, "queue cancel")
}
func (c *Client) QueueConsumerRegister(name string) (uint64, error) {
	if name == "" || len(name) > 255 {
		return 0, fmt.Errorf("kuttidb: invalid consumer")
	}
	s, v, e := c.stateRequest(opQueueConsumerRegister, name, nil)
	if e != nil {
		return 0, e
	}
	if e = requireOK(s, "consumer register"); e != nil {
		return 0, e
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("kuttidb: malformed consumer response")
	}
	return binary.LittleEndian.Uint64(v), nil
}
func (c *Client) QueueConsumerUnregister(name string) error {
	s, _, e := c.stateRequest(opQueueConsumerUnregister, name, nil)
	if e != nil {
		return e
	}
	return requireOK(s, "consumer unregister")
}

func (c *Client) QueuePublishBatch(name string, values [][]byte) ([]uint64, error) {
	if len(values) < 1 || len(values) > 256 {
		return nil, fmt.Errorf("kuttidb: batch size must be 1-256")
	}
	if e := c.requireFeature(FeatureQueueBatch, "queue batches"); e != nil {
		return nil, e
	}
	p := appendU32(nil, uint32(len(values)))
	for _, v := range values {
		if len(v) > maxValue {
			return nil, ErrValueTooLarge
		}
		p = appendU32(p, uint32(len(v)))
		p = append(p, v...)
	}
	s, v, e := c.stateRequest(opQueuePublishBatch, name, p)
	if e != nil {
		return nil, e
	}
	if e = requireOK(s, "queue publish batch"); e != nil {
		return nil, e
	}
	d := decoder{b: v}
	n, e := d.u32()
	if e != nil || int(n) != len(values) {
		return nil, fmt.Errorf("kuttidb: malformed batch response")
	}
	out := make([]uint64, n)
	for i := range out {
		out[i], e = d.u64()
		if e != nil {
			return nil, e
		}
	}
	return out, d.done()
}
func (c *Client) QueueConsumeBatch(name string, maxCount uint32) ([]Delivery, error) {
	if maxCount < 1 || maxCount > 256 {
		return nil, fmt.Errorf("kuttidb: batch size must be 1-256")
	}
	if e := c.requireFeature(FeatureQueueBatch, "queue batches"); e != nil {
		return nil, e
	}
	s, v, e := c.stateRequest(opQueueConsumeBatch, name, appendU32(nil, maxCount))
	if e != nil {
		return nil, e
	}
	if s == statusMiss {
		return []Delivery{}, nil
	}
	if e = requireOK(s, "queue consume batch"); e != nil {
		return nil, e
	}
	d := decoder{b: v}
	n, e := d.u32()
	if e != nil {
		return nil, e
	}
	out := make([]Delivery, 0, n)
	for range n {
		tag, e := d.u64()
		if e != nil {
			return nil, e
		}
		id, e := d.u64()
		if e != nil {
			return nil, e
		}
		count, e := d.u32()
		if e != nil {
			return nil, e
		}
		flag, e := d.bytes(1)
		if e != nil {
			return nil, e
		}
		l, e := d.u32()
		if e != nil {
			return nil, e
		}
		body, e := d.bytes(int(l))
		if e != nil {
			return nil, e
		}
		out = append(out, Delivery{tag, id, count, flag[0] != 0, append([]byte(nil), body...)})
	}
	return out, d.done()
}
func (c *Client) queueDispositionBatch(name string, tags []uint64, mode byte) (uint32, error) {
	if len(tags) < 1 || len(tags) > 256 {
		return 0, fmt.Errorf("kuttidb: batch size must be 1-256")
	}
	if e := c.requireFeature(FeatureQueueBatch, "queue batches"); e != nil {
		return 0, e
	}
	p := []byte{mode}
	p = appendU32(p, uint32(len(tags)))
	for _, tag := range tags {
		p = appendU64(p, tag)
	}
	s, v, e := c.stateRequest(opQueueAckBatch, name, p)
	if e != nil {
		return 0, e
	}
	if e = requireOK(s, "queue disposition batch"); e != nil {
		return 0, e
	}
	if len(v) != 4 {
		return 0, fmt.Errorf("kuttidb: malformed batch response")
	}
	return binary.LittleEndian.Uint32(v), nil
}
func (c *Client) QueueAckBatch(name string, tags []uint64) (uint32, error) {
	return c.queueDispositionBatch(name, tags, 0)
}
func (c *Client) QueueNackBatch(name string, tags []uint64, requeue bool) (uint32, error) {
	mode := byte(2)
	if requeue {
		mode = 1
	}
	return c.queueDispositionBatch(name, tags, mode)
}
