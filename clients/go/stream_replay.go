package kuttidb

// Native stream replay contract (protocol 1.9, opcode 0x6d, capability bit
// 17): one request returns records and replay metadata together — the
// persisted topic incarnation, both partition boundaries, the range
// decision, and the page — so no separate (racy) metadata fetch is needed.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
)

// streamIDLen is the identity length on the wire (16 raw bytes).
const streamIDLen = 16

var (
	// ErrStreamOffsetExpired reports a cursor below the retained base: the
	// history is gone and cannot be reconstructed.
	ErrStreamOffsetExpired = errors.New("kuttidb: stream offset expired")
	// ErrStreamOffsetAhead reports a cursor past the high-water mark.
	ErrStreamOffsetAhead = errors.New("kuttidb: stream offset ahead of the high-water mark")
	// ErrStreamRecreated reports that the topic incarnation does not match
	// the expected one (delete/recreate).
	ErrStreamRecreated = errors.New("kuttidb: stream topic was deleted and recreated")
	// ErrStreamMissingResource reports a missing topic or an invalid
	// partition — never an empty valid stream.
	ErrStreamMissingResource = errors.New("kuttidb: stream topic or partition missing")
	// ErrStreamReplayUnsupported reports a server without capability bit 17;
	// it never pretends a gap was detected.
	ErrStreamReplayUnsupported = errors.New("kuttidb: server does not support stream replay metadata")
)

// StreamGapError carries the gap kind together with the actual identity and
// boundaries, so an application can explicitly rebuild state and resume at
// the base or the tail. KuttiDB reports the gap; it cannot reconstruct
// expired events.
type StreamGapError struct {
	Kind   error             // one of ErrStreamOffsetExpired/Ahead/Recreated
	Result StreamFetchResult // StreamID and boundaries are always populated
}

func (e *StreamGapError) Error() string {
	return fmt.Sprintf("%s (stream %s partition %d base %d next %d)",
		e.Kind, e.Result.StreamID, e.Result.Partition,
		e.Result.BaseOffset, e.Result.NextOffset)
}

func (e *StreamGapError) Is(target error) bool { return target == e.Kind }

func (e *StreamGapError) Unwrap() error { return e.Kind }

// StreamFetchRange mirrors the wire range decision (0..3).
type StreamFetchRange byte

const (
	StreamRangeOK        StreamFetchRange = iota
	StreamRangeExpired
	StreamRangeAhead
	StreamRangeRecreated
)

// StreamReplayCursor is the persisted cursor for an SSE-style consumer: the
// opaque topic incarnation plus the next record to request.
type StreamReplayCursor struct {
	StreamID  string // hex-encoded opaque persisted topic incarnation
	Partition uint32
	Offset    uint64 // next record requested, not last record delivered
}

// StreamFetchOptions carries partition, requested next offset, maximum
// record count, and an optional expected StreamID (hex) for resumption. A
// first fetch may omit it.
type StreamFetchOptions struct {
	Partition        uint32
	Offset           uint64 // next record requested
	MaxRecords       uint32 // 1..1024
	ExpectedStreamID string // hex-encoded incarnation or "" for a first fetch
}


// StreamFetchResult is one self-consistent page even if state changes
// immediately afterward.
type StreamFetchResult struct {
	StreamID     string
	Partition    uint32
	BaseOffset   uint64 // earliest retained boundary, inclusive
	NextOffset   uint64 // append high-water mark, exclusive
	ResumeOffset uint64 // where to continue this page
	Records      []StreamRecord
}

func (c *Client) StreamFetchWithMetadata(topic string, options StreamFetchOptions) (StreamFetchResult, error) {
	return c.StreamFetchWithMetadataContext(context.Background(), topic, options)
}

// StreamFetchWithMetadataContext fetches records and replay metadata in one
// native round trip. Gap and identity mismatches return a *StreamGapError
// (recognizable with errors.Is against ErrStreamOffsetExpired,
// ErrStreamOffsetAhead, ErrStreamRecreated) whose Result still carries the
// actual StreamID and boundaries; Records are nil on a gap. A missing topic
// or partition yields ErrStreamMissingResource.
func (c *Client) StreamFetchWithMetadataContext(ctx context.Context, topic string, options StreamFetchOptions) (StreamFetchResult, error) {
	if options.MaxRecords < 1 || options.MaxRecords > 1024 {
		return StreamFetchResult{}, fmt.Errorf("kuttidb: invalid fetch count")
	}
	if topic == "" || len(topic) > 255 {
		return StreamFetchResult{}, fmt.Errorf("kuttidb: invalid stream topic")
	}
	if e := c.requireFeatureCtx(ctx, FeatureStreamReplay, "stream replay metadata"); e != nil {
		return StreamFetchResult{}, e
	}
	var expected []byte
	if options.ExpectedStreamID != "" {
		raw, e := hex.DecodeString(options.ExpectedStreamID)
		if e != nil || len(raw) != streamIDLen {
			return StreamFetchResult{}, fmt.Errorf("kuttidb: invalid expected StreamID")
		}
		expected = raw
	}
	p := appendU32(nil, options.Partition)
	p = appendU64(p, options.Offset)
	p = appendU32(p, options.MaxRecords)
	if expected != nil {
		p = append(p, 1)
		p = append(p, expected...)
	} else {
		p = append(p, 0)
	}
	status, value, err := c.requestCtx(ctx, opStreamFetchMeta, topic, p)
	if err != nil {
		return StreamFetchResult{}, err
	}
	if status == statusError {
		if len(value) == 1 {
			switch value[0] {
			case 1:
				return StreamFetchResult{}, ErrStreamMissingResource
			case 2:
				return StreamFetchResult{}, ErrServer
			case 3:
				return StreamFetchResult{}, fmt.Errorf("kuttidb: first record exceeds the fetch budget: %w", ErrServer)
			}
		}
		return StreamFetchResult{}, ErrServer
	}
	if status == statusMiss {
		// Not produced by this protocol version; treat conservatively.
		return StreamFetchResult{}, ErrStreamReplayUnsupported
	}
	return decodeStreamFetchMeta(value, options.Partition, options.Offset)
}

// decodeStreamFetchMeta validates the full body: enum range, ID length,
// boundary ordering, record count, ordering/ranges, and trailing data.
func decodeStreamFetchMeta(payload []byte, partition uint32, requested uint64) (StreamFetchResult, error) {
	d := decoder{b: payload}
	rangeByte, e := d.bytes(1)
	if e != nil {
		return StreamFetchResult{}, e
	}
	if rangeByte[0] > byte(StreamRangeRecreated) {
		return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
	}
	base, e := d.u64()
	if e != nil {
		return StreamFetchResult{}, e
	}
	next, e := d.u64()
	if e != nil {
		return StreamFetchResult{}, e
	}
	resume, e := d.u64()
	if e != nil {
		return StreamFetchResult{}, e
	}
	idRaw, e := d.bytes(streamIDLen)
	if e != nil {
		return StreamFetchResult{}, e
	}
	id := hex.EncodeToString(idRaw)
	count, e := d.u32()
	if e != nil {
		return StreamFetchResult{}, e
	}
	if count > 1024 {
		return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
	}
	records := make([]StreamRecord, 0, count)
	previous := uint64(0)
	for i := uint32(0); i < count; i++ {
		off, e := d.u64()
		if e != nil {
			return StreamFetchResult{}, e
		}
		kl, e := d.u16()
		if e != nil {
			return StreamFetchResult{}, e
		}
		vl, e := d.u32()
		if e != nil {
			return StreamFetchResult{}, e
		}
		key, e := d.bytes(int(kl))
		if e != nil {
			return StreamFetchResult{}, e
		}
		value, e := d.bytes(int(vl))
		if e != nil {
			return StreamFetchResult{}, e
		}
		if off < base || off >= next {
			return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
		}
		if i > 0 && off <= previous {
			return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
		}
		previous = off
		records = append(records, StreamRecord{off, append([]byte(nil), key...), append([]byte(nil), value...)})
	}
	if e = d.done(); e != nil {
		return StreamFetchResult{}, e
	}
	result := StreamFetchResult{
		StreamID:     id,
		Partition:    partition,
		BaseOffset:   base,
		NextOffset:   next,
		ResumeOffset: resume,
		Records:      records,
	}
	switch StreamFetchRange(rangeByte[0]) {
	case StreamRangeOK:
		if base > next {
			return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
		}
		// A successful page must be self-consistent: resume is the last
		// returned offset plus one, or the requested offset when empty.
		if len(records) > 0 {
			last := records[len(records)-1].Offset
			want := uint64(0)
			if last != ^uint64(0) {
				want = last + 1
			} else {
				want = last
			}
			if resume != want {
				return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
			}
		} else if resume != requested {
			return StreamFetchResult{}, fmt.Errorf("kuttidb: malformed stream fetch response")
		}
		return result, nil
	case StreamRangeExpired:
		return result, &StreamGapError{Kind: ErrStreamOffsetExpired, Result: result}
	case StreamRangeAhead:
		return result, &StreamGapError{Kind: ErrStreamOffsetAhead, Result: result}
	default: // StreamRangeRecreated
		return result, &StreamGapError{Kind: ErrStreamRecreated, Result: result}
	}
}

// ReplayCursorFrom converts a last-delivered record offset to the next
// offset with overflow validation, for persisting an SSE cursor. The
// StreamID comes from the first metadata fetch.
func ReplayCursorFrom(partition uint32, lastDelivered uint64) (StreamReplayCursor, error) {
	if lastDelivered == ^uint64(0) {
		return StreamReplayCursor{}, fmt.Errorf("kuttidb: last-delivered offset %d cannot be advanced", lastDelivered)
	}
	return StreamReplayCursor{Partition: partition, Offset: lastDelivered + 1}, nil
}

// stream id length on the wire
const StreamIDLen = 16