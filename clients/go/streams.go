package kuttidb

import (
	"encoding/binary"
	"fmt"
	"time"
)

type StreamOptions struct {
	Partitions uint32
	MaxBytes   uint64
	MaxAge     time.Duration
}
type StreamInfo struct {
	Topic          string
	Partitions     uint32
	Records, Bytes uint64
}
type StreamGroupInfo struct {
	Topic, Group string
	Generation   uint64
	Members      uint32
}
type StreamRecord struct {
	Offset     uint64
	Key, Value []byte
}
type StreamAppend struct{ Key, Value []byte }
type StreamPosition struct{ Partition, Offset uint64 }
type StreamCommit struct {
	Partition uint32
	Offset    uint64
}
type StreamAssignment struct {
	Partitions []uint32
	Generation uint64
}

func (c *Client) StreamDeclare(topic string, o StreamOptions) error {
	if topic == "" || len(topic) > 255 || o.Partitions < 1 || o.Partitions > 256 {
		return fmt.Errorf("kuttidb: invalid stream declaration")
	}
	age, e := milliseconds(o.MaxAge, true)
	if e != nil {
		return e
	}
	p := appendU32(nil, o.Partitions)
	p = appendU64(p, o.MaxBytes)
	p = appendU64(p, age)
	s, _, e := c.request(opStreamDeclare, topic, p)
	if e != nil {
		return e
	}
	return requireOK(s, "stream declare")
}
func (c *Client) StreamList() ([]StreamInfo, error) {
	s, v, e := c.request(opStreamList, "", nil)
	if e != nil {
		return nil, e
	}
	if e = requireOK(s, "stream list"); e != nil {
		return nil, e
	}
	d := decoder{b: v}
	n, e := d.u16()
	if e != nil {
		return nil, e
	}
	out := make([]StreamInfo, 0, n)
	for range n {
		l, e := d.u16()
		if e != nil {
			return nil, e
		}
		name, e := d.bytes(int(l))
		if e != nil {
			return nil, e
		}
		parts, e := d.u32()
		if e != nil {
			return nil, e
		}
		records, e := d.u64()
		if e != nil {
			return nil, e
		}
		bytes, e := d.u64()
		if e != nil {
			return nil, e
		}
		out = append(out, StreamInfo{string(name), parts, records, bytes})
	}
	return out, d.done()
}
func (c *Client) StreamGroupList() ([]StreamGroupInfo, error) {
	s, v, e := c.request(opStreamGroupList, "", nil)
	if e != nil {
		return nil, e
	}
	if e = requireOK(s, "stream group list"); e != nil {
		return nil, e
	}
	d := decoder{b: v}
	n, e := d.u16()
	if e != nil {
		return nil, e
	}
	out := make([]StreamGroupInfo, 0, n)
	for range n {
		tl, e := d.u16()
		if e != nil {
			return nil, e
		}
		topic, e := d.bytes(int(tl))
		if e != nil {
			return nil, e
		}
		gl, e := d.u16()
		if e != nil {
			return nil, e
		}
		group, e := d.bytes(int(gl))
		if e != nil {
			return nil, e
		}
		gen, e := d.u64()
		if e != nil {
			return nil, e
		}
		members, e := d.u32()
		if e != nil {
			return nil, e
		}
		out = append(out, StreamGroupInfo{string(topic), string(group), gen, members})
	}
	return out, d.done()
}

func (c *Client) StreamAppend(topic string, value, key []byte, partition *uint32) (StreamPosition, error) {
	hint := uint32(0xffffffff)
	if partition != nil {
		hint = *partition
	}
	if len(key) > 65535 {
		return StreamPosition{}, fmt.Errorf("kuttidb: stream key too large")
	}
	p := appendU32(nil, hint)
	p = appendU16(p, uint16(len(key)))
	p = append(p, key...)
	p = append(p, value...)
	s, v, e := c.request(opStreamAppend, topic, p)
	if e != nil {
		return StreamPosition{}, e
	}
	if e = requireOK(s, "stream append"); e != nil {
		return StreamPosition{}, e
	}
	if len(v) != 16 {
		return StreamPosition{}, fmt.Errorf("kuttidb: malformed stream append response")
	}
	return StreamPosition{binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint64(v[8:])}, nil
}
func (c *Client) StreamAppendBatch(topic string, items []StreamAppend, partition *uint32) ([]StreamPosition, error) {
	if len(items) < 1 || len(items) > 1024 {
		return nil, fmt.Errorf("kuttidb: stream batch size must be 1-1024")
	}
	if e := c.requireFeature(FeatureStreamBatch, "stream batch append"); e != nil {
		return nil, e
	}
	hint := uint32(0xffffffff)
	if partition != nil {
		hint = *partition
	}
	p := appendU32(nil, hint)
	p = appendU32(p, uint32(len(items)))
	for _, it := range items {
		if len(it.Key) > 65535 || len(it.Value) > maxValue {
			return nil, ErrValueTooLarge
		}
		p = appendU16(p, uint16(len(it.Key)))
		p = appendU32(p, uint32(len(it.Value)))
		p = append(p, it.Key...)
		p = append(p, it.Value...)
	}
	s, v, e := c.request(opStreamAppendBatch, topic, p)
	if e != nil {
		return nil, e
	}
	if e = requireOK(s, "stream batch append"); e != nil {
		return nil, e
	}
	d := decoder{b: v}
	n, e := d.u32()
	if e != nil || int(n) != len(items) {
		return nil, fmt.Errorf("kuttidb: malformed stream batch response")
	}
	out := make([]StreamPosition, n)
	for i := range out {
		out[i].Partition, e = d.u64()
		if e != nil {
			return nil, e
		}
		out[i].Offset, e = d.u64()
		if e != nil {
			return nil, e
		}
	}
	return out, d.done()
}

func (c *Client) StreamFetch(topic string, partition uint32, offset uint64, maxRecords uint32) ([]StreamRecord, error) {
	if maxRecords < 1 || maxRecords > 1024 {
		return nil, fmt.Errorf("kuttidb: invalid fetch count")
	}
	caps, e := c.Capabilities()
	if e != nil {
		return nil, e
	}
	keyed := caps.Features&FeatureStreamKeys != 0
	op := byte(opStreamFetch)
	if keyed {
		op = opStreamFetchKeys
	}
	p := appendU32(nil, partition)
	p = appendU64(p, offset)
	p = appendU32(p, maxRecords)
	s, v, e := c.request(op, topic, p)
	if e != nil {
		return nil, e
	}
	if s == statusMiss {
		return []StreamRecord{}, nil
	}
	if e = requireOK(s, "stream fetch"); e != nil {
		return nil, e
	}
	d := decoder{b: v}
	n, e := d.u32()
	if e != nil {
		return nil, e
	}
	out := make([]StreamRecord, 0, n)
	for range n {
		off, e := d.u64()
		if e != nil {
			return nil, e
		}
		kl := uint16(0)
		if keyed {
			kl, e = d.u16()
			if e != nil {
				return nil, e
			}
		}
		vl, e := d.u32()
		if e != nil {
			return nil, e
		}
		key, e := d.bytes(int(kl))
		if e != nil {
			return nil, e
		}
		value, e := d.bytes(int(vl))
		if e != nil {
			return nil, e
		}
		out = append(out, StreamRecord{off, append([]byte(nil), key...), append([]byte(nil), value...)})
	}
	return out, d.done()
}

func groupPartition(group string, partition uint32) ([]byte, error) {
	if group == "" || len(group) > 255 {
		return nil, fmt.Errorf("kuttidb: invalid stream group")
	}
	p := appendU16(nil, uint16(len(group)))
	p = append(p, group...)
	p = appendU32(p, partition)
	return p, nil
}
func (c *Client) StreamCommit(topic, group string, partition uint32, offset uint64) error {
	p, e := groupPartition(group, partition)
	if e != nil {
		return e
	}
	p = appendU64(p, offset)
	s, _, e := c.stateRequest(opStreamCommit, topic, p)
	if e != nil {
		return e
	}
	return requireOK(s, "stream commit")
}
func (c *Client) StreamCommitBatch(topic, group string, commits []StreamCommit) error {
	if len(commits) < 1 || len(commits) > 256 {
		return fmt.Errorf("kuttidb: commit batch size must be 1-256")
	}
	if e := c.requireFeature(FeatureStreamCommitBatch, "stream commit batch"); e != nil {
		return e
	}
	if group == "" || len(group) > 255 {
		return fmt.Errorf("kuttidb: invalid stream group")
	}
	p := appendU16(nil, uint16(len(group)))
	p = append(p, group...)
	p = appendU32(p, uint32(len(commits)))
	for _, it := range commits {
		p = appendU32(p, it.Partition)
		p = appendU64(p, it.Offset)
	}
	s, _, e := c.stateRequest(opStreamCommitBatch, topic, p)
	if e != nil {
		return e
	}
	return requireOK(s, "stream commit batch")
}
func (c *Client) streamGroupValue(op byte, topic, group string, partition uint32) (*uint64, error) {
	p, e := groupPartition(group, partition)
	if e != nil {
		return nil, e
	}
	s, v, e := c.stateRequest(op, topic, p)
	if e != nil {
		return nil, e
	}
	if s == statusMiss {
		return nil, nil
	}
	if e = requireOK(s, "stream group query"); e != nil {
		return nil, e
	}
	if len(v) != 8 {
		return nil, fmt.Errorf("kuttidb: malformed stream group response")
	}
	x := binary.LittleEndian.Uint64(v)
	return &x, nil
}
func (c *Client) StreamGroupOffset(topic, group string, partition uint32) (*uint64, error) {
	return c.streamGroupValue(opStreamGroupOffset, topic, group, partition)
}
func (c *Client) StreamGroupLag(topic, group string, partition uint32) (*uint64, error) {
	return c.streamGroupValue(opStreamGroupLag, topic, group, partition)
}
func (c *Client) StreamGroupJoin(topic, group string, lease time.Duration) (StreamAssignment, error) {
	if group == "" || len(group) > 255 {
		return StreamAssignment{}, fmt.Errorf("kuttidb: invalid stream group")
	}
	ms, e := milliseconds(lease, false)
	if e != nil || ms > 60000 {
		return StreamAssignment{}, fmt.Errorf("kuttidb: lease must be 0..60 seconds")
	}
	p := appendU16(nil, uint16(len(group)))
	p = append(p, group...)
	p = appendU32(p, uint32(ms))
	s, v, e := c.stateRequest(opStreamGroupJoin, topic, p)
	if e != nil {
		return StreamAssignment{}, e
	}
	if e = requireOK(s, "stream group join"); e != nil {
		return StreamAssignment{}, e
	}
	d := decoder{b: v}
	n, e := d.u32()
	if e != nil || n > 256 {
		return StreamAssignment{}, fmt.Errorf("kuttidb: malformed stream assignment")
	}
	out := StreamAssignment{Partitions: make([]uint32, n)}
	for i := range out.Partitions {
		out.Partitions[i], e = d.u32()
		if e != nil {
			return StreamAssignment{}, e
		}
	}
	if d.i < len(v) {
		out.Generation, e = d.u64()
		if e != nil {
			return StreamAssignment{}, e
		}
	}
	return out, d.done()
}
func (c *Client) StreamGroupLeave(topic, group string) error {
	if group == "" || len(group) > 255 {
		return fmt.Errorf("kuttidb: invalid stream group")
	}
	p := appendU16(nil, uint16(len(group)))
	p = append(p, group...)
	s, _, e := c.stateRequest(opStreamGroupLeave, topic, p)
	if e != nil {
		return e
	}
	return requireOK(s, "stream group leave")
}
