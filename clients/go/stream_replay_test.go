package kuttidb

// Unit coverage for the stream replay decoder: valid pages, every gap row,
// and malformed/truncated frames must fail closed without fabricating data.
// Integration coverage drives the public API against a real server.

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"
)

func metaPayload(rangeByte byte, base, next, resume uint64, id [16]byte, records ...[]byte) []byte {
	out := []byte{rangeByte}
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], base)
	out = append(out, b8[:]...)
	binary.LittleEndian.PutUint64(b8[:], next)
	out = append(out, b8[:]...)
	binary.LittleEndian.PutUint64(b8[:], resume)
	out = append(out, b8[:]...)
	out = append(out, id[:]...)
	out = appendU32(out, uint32(len(records)))
	for _, r := range records {
		out = append(out, r...)
	}
	return out
}

func testID(v byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = v
	}
	return id
}

func tooLargeCount() []byte {
	p := []byte{0}
	var b [24]byte
	binary.LittleEndian.PutUint64(b[:], 5)
	binary.LittleEndian.PutUint64(b[8:], 10)
	binary.LittleEndian.PutUint64(b[16:], 6)
	p = append(p, b[:]...)
	id := testID(2)
	p = append(p, id[:]...)
	p = append(p, 200, 0, 0, 0)
	return p
}

func recordBytes(off uint64, key, value string) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, off)
	var klen [2]byte
	binary.LittleEndian.PutUint16(klen[:], uint16(len(key)))
	out = append(out, klen[:]...)
	var vlen [4]byte
	binary.LittleEndian.PutUint32(vlen[:], uint32(len(value)))
	out = append(out, vlen[:]...)
	out = append(out, key...)
	out = append(out, value...)
	return out
}

func TestStreamFetchMetaDecodeSuccessPages(t *testing.T) {
	id := testID(7)
	res, err := decodeStreamFetchMeta(metaPayload(0, 0, 0, 0, id), 0, 0)
	if err != nil {
		t.Fatalf("empty stream: %v", err)
	}
	if res.BaseOffset != 0 || res.NextOffset != 0 || res.ResumeOffset != 0 ||
		len(res.Records) != 0 || res.StreamID != hex.EncodeToString(id[:]) {
		t.Fatalf("empty stream result: %+v", res)
	}
	res, err = decodeStreamFetchMeta(metaPayload(0, 5, 10, 7, id,
		recordBytes(5, "", "a"), recordBytes(6, "k", "b")), 0, 5)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if res.ResumeOffset != 7 || len(res.Records) != 2 || res.Records[1].Offset != 6 {
		t.Fatalf("page result: %+v", res)
	}
	res, err = decodeStreamFetchMeta(metaPayload(0, 5, 10, 10, id), 0, 10)
	if err != nil || res.ResumeOffset != 10 || len(res.Records) != 0 {
		t.Fatalf("tail result: %+v %v", res, err)
	}
	res, err = decodeStreamFetchMeta(metaPayload(0, 10, 10, 10, id), 0, 10)
	if err != nil || res.ResumeOffset != 10 {
		t.Fatalf("expired-tail result: %+v %v", res, err)
	}
	// Near-overflow edge: the last representable record has resume = next.
	res, err = decodeStreamFetchMeta(metaPayload(0, 0, ^uint64(0), ^uint64(0), id,
		recordBytes(^uint64(0)-1, "", "x")), 0, ^uint64(0))
	if err != nil || res.ResumeOffset != ^uint64(0) {
		t.Fatalf("overflow resume: %+v %v", res, err)
	}
}

func TestStreamFetchMetaDecodeGapRows(t *testing.T) {
	id := testID(9)
	cases := []struct {
		name string
		rng  byte
		base uint64
		next uint64
		req  uint64
		want error
	}{
		{"expired below base", 1, 5, 10, 3, ErrStreamOffsetExpired},
		{"ahead of high water", 2, 5, 10, 11, ErrStreamOffsetAhead},
		{"recreated identity", 3, 5, 10, 5, ErrStreamRecreated},
		{"expired on fully expired", 1, 10, 10, 3, ErrStreamOffsetExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeStreamFetchMeta(metaPayload(tc.rng, tc.base, tc.next, tc.req, id), 0, tc.req)
			var gap *StreamGapError
			if !errors.As(err, &gap) || !errors.Is(err, tc.want) {
				t.Fatalf("want gap %v, got %v", tc.want, err)
			}
			if gap.Result.StreamID != hex.EncodeToString(id[:]) ||
				gap.Result.BaseOffset != tc.base || gap.Result.NextOffset != tc.next {
				t.Fatalf("gap result must carry identity and boundaries: %+v", gap.Result)
			}
			if len(gap.Result.Records) != 0 {
				t.Fatalf("gap must return no records: %+v", gap.Result.Records)
			}
		})
	}
}

func TestStreamFetchMetaDecoderRejectsMalformed(t *testing.T) {
	id := testID(1)
	good := recordBytes(5, "k", "v")
	cases := []struct {
		name    string
		payload []byte
	}{
		{"range out of enum", metaPayload(9, 0, 10, 6, id)},
		{"base above next", metaPayload(0, 11, 10, 10, id)},
		{"record below base", metaPayload(0, 5, 10, 6, id, recordBytes(4, "", "x"))},
		{"record at next", metaPayload(0, 5, 10, 6, id, recordBytes(10, "", "x"))},
		{"unordered records", metaPayload(0, 5, 10, 7, id, recordBytes(6, "", "x"), recordBytes(6, "", "y"))},
		{"count too large", tooLargeCount()},
		{"truncated record", append(metaPayload(0, 5, 10, 6, id), good[:len(good)-1]...)},
		{"trailing bytes", append(metaPayload(0, 5, 10, 7, id, good), 0)},
		{"resume mismatch on page", metaPayload(0, 5, 10, 8, id, good)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeStreamFetchMeta(tc.payload, 0, 6); err == nil {
				t.Fatal("malformed payload accepted")
			}
		})
	}
}

func TestReplayCursorFromRejectsOverflow(t *testing.T) {
	if _, err := ReplayCursorFrom(0, ^uint64(0)); err == nil {
		t.Fatal("overflow accepted")
	}
	c, err := ReplayCursorFrom(2, 9)
	if err != nil || c.Offset != 10 || c.Partition != 2 {
		t.Fatalf("cursor conversion: %+v %v", c, err)
	}
}