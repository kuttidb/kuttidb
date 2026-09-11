package kuttidb

// Integration coverage for the native replay contract over the public Go
// API against a real server: gap rows, identity stability across restart,
// retained boundaries, and legacy fetch compatibility.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func startReplayServer(t *testing.T, wal string, bin string, port int) *jobServerProc {
	t.Helper()
	return startJobServer(t, bin, port, wal, false)
}

func TestStreamFetchWithMetadataIntegration(t *testing.T) {
	bin := buildServer(t)
	dir := t.TempDir()
	wal := dir + "/k.wal"
	port := freeJobPort(t)
	proc := startJobServer(t, bin, port, wal, false)
	c := connectJobClient(t, proc.port)
	ctx := context.Background()

	// Never-written partition: base=next=0; request 0 → success, empty,
	// resume=0, valid stream ID.
	// Size retention is configured up front so the retained window below
	// is deterministic without timing dependence.
	if err := c.StreamDeclare("replay-topic", StreamOptions{Partitions: 2, MaxBytes: 25}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	first, err := c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Partition: 1, Offset: 0, MaxRecords: 10})
	if err != nil || len(first.Records) != 0 || first.BaseOffset != 0 ||
		first.NextOffset != 0 || first.ResumeOffset != 0 {
		t.Fatalf("never-written partition: %+v %v", first, err)
	}
	if len(first.StreamID) != 32 {
		t.Fatalf("stream id must be present even when empty: %q", first.StreamID)
	}

	// Retained 5..9 via size retention: ten 5-byte records with a 25-byte
	// ceiling leave exactly the last five, so base=5 and next=10 without
	// any timing dependence.
	items := make([]StreamAppend, 0, 10)
	for i := 0; i < 10; i++ {
		items = append(items, StreamAppend{Value: []byte(fmt.Sprintf("rec-%02d", i))[:5]})
	}
	if _, err := c.StreamAppendBatch("replay-topic", items, nil); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	// A metadata fetch applies applicable retention first, then snapshots.
	page, err := c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Offset: 5, MaxRecords: 5})
	if err != nil {
		t.Fatalf("page 5: %v", err)
	}
	if page.BaseOffset != 5 || page.NextOffset != 10 || page.ResumeOffset != 10 || len(page.Records) != 5 {
		t.Fatalf("retained page: %+v", page)
	}
	for i, r := range page.Records {
		if r.Offset != uint64(5+i) || string(r.Value) != fmt.Sprintf("rec-%02d", 5+i)[:5] {
			t.Fatalf("record %d: %+v", i, r)
		}
	}
	// Request 9 → success starting at the requested offset.
	page, err = c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Offset: 9, MaxRecords: 5})
	if err != nil || len(page.Records) != 1 || page.Records[0].Offset != 9 || page.ResumeOffset != 10 {
		t.Fatalf("page at 9: %+v %v", page, err)
	}
	// Request 3 → offset_expired with boundaries 5/10 and no records.
	_, err = c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Offset: 3, MaxRecords: 5})
	var expired *StreamGapError
	if !errors.As(err, &expired) || !errors.Is(err, ErrStreamOffsetExpired) {
		t.Fatalf("request below base: %v", err)
	}
	if expired.Result.BaseOffset != 5 || expired.Result.NextOffset != 10 || len(expired.Result.Records) != 0 {
		t.Fatalf("expired result must keep boundaries: %+v", expired.Result)
	}
	// Request 10 (tail) → success, empty, resume=10.
	page, err = c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Offset: 10, MaxRecords: 5})
	if err != nil || len(page.Records) != 0 || page.ResumeOffset != 10 {
		t.Fatalf("tail request: %+v %v", page, err)
	}
	// Request 11 → offset_ahead with boundaries.
	_, err = c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Offset: 11, MaxRecords: 5})
	var ahead *StreamGapError
	if !errors.As(err, &ahead) || !errors.Is(err, ErrStreamOffsetAhead) {
		t.Fatalf("request past high-water mark: %v", err)
	}
	// Expected ID matching keeps the page; a mismatched expected ID would
	// be recreated (delete/recreate is covered by the engine and native
	// protocol suites, which own the delete surface).
	same, err := c.StreamFetchWithMetadataContext(ctx, "replay-topic",
		StreamFetchOptions{Offset: 5, MaxRecords: 5, ExpectedStreamID: page.StreamID})
	if err != nil || len(same.Records) != 5 {
		t.Fatalf("expected-ID page: %+v %v", same, err)
	}
	// Missing topic → explicit typed error, not an empty valid stream.
	if _, err := c.StreamFetchWithMetadata("ghost-topic", StreamFetchOptions{Offset: 0, MaxRecords: 1}); !errors.Is(err, ErrStreamMissingResource) {
		t.Fatalf("missing topic: %v", err)
	}

	// Legacy fetch behavior is unchanged on the same server.
	legacy, err := c.StreamFetch("replay-topic", 0, 5, 5)
	if err != nil || len(legacy) != 5 || legacy[0].Offset != 5 {
		t.Fatalf("legacy fetch: %+v %v", legacy, err)
	}

	// Restart (SIGKILL) preserves the identity and boundaries; the next
	// append must not reuse historical offsets.
	origID := page.StreamID
	proc.kill()
	_ = startReplayServer(t, wal, bin, port)
	c2 := connectJobClient(t, port)
	defer c2.Close()
	after, err := c2.StreamFetchWithMetadata("replay-topic", StreamFetchOptions{Offset: 10, MaxRecords: 5})
	if err != nil || after.BaseOffset != 5 || after.NextOffset != 10 {
		t.Fatalf("restart boundaries: %+v %v", after, err)
	}
	if after.StreamID != origID {
		t.Fatalf("restart changed the topic incarnation: %s != %s", after.StreamID, origID)
	}
	pos, err := c2.StreamAppend("replay-topic", []byte("post-restart"), nil, nil)
	if err != nil || pos.Offset != 10 {
		t.Fatalf("append after restart must reuse the high-water mark, not history: %+v %v", pos, err)
	}
}

func TestStreamFullExpiryGapSurvivesRestart(t *testing.T) {
	// Gaps remain detectable after complete expiry AND restart: base=next
	// with zero retained records still reports offset_expired below the
	// boundary, and the next append does not rewind the high-water mark.
	bin := buildServer(t)
	dir := t.TempDir()
	wal := dir + "/k.wal"
	port := freeJobPort(t)
	proc := startJobServer(t, bin, port, wal, false)
	c := connectJobClient(t, proc.port)
	if err := c.StreamDeclare("expiry-topic", StreamOptions{Partitions: 1, MaxAge: 150 * time.Millisecond}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := c.StreamAppend("expiry-topic", []byte{byte('a' + i)}, nil, nil); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	time.Sleep(400 * time.Millisecond)
	// The fetch applies retention: everything expires, base=next=4.
	_, err := c.StreamFetchWithMetadata("expiry-topic", StreamFetchOptions{Offset: 0, MaxRecords: 10})
	var expired *StreamGapError
	if !errors.As(err, &expired) || !errors.Is(err, ErrStreamOffsetExpired) {
		t.Fatalf("fully expired stream must report a gap: %v", err)
	}
	if expired.Result.BaseOffset != 4 || expired.Result.NextOffset != 4 {
		t.Fatalf("fully expired boundaries: %+v", expired.Result)
	}
	// Requesting the boundary itself succeeds with an empty page.
	page, err := c.StreamFetchWithMetadata("expiry-topic", StreamFetchOptions{Offset: 4, MaxRecords: 10})
	if err != nil || len(page.Records) != 0 || page.ResumeOffset != 4 {
		t.Fatalf("expired tail request: %+v %v", page, err)
	}
	proc.kill()
	proc2 := startReplayServer(t, wal, bin, port)
	defer proc2.kill()
	c2 := connectJobClient(t, port)
	defer c2.Close()
	// The gap survives the restart; the next append does not reuse history.
	_, err = c2.StreamFetchWithMetadata("expiry-topic", StreamFetchOptions{Offset: 0, MaxRecords: 10})
	if !errors.As(err, &expired) || !errors.Is(err, ErrStreamOffsetExpired) {
		t.Fatalf("gap after restart: %v", err)
	}
	pos, err := c2.StreamAppend("expiry-topic", []byte("fresh"), nil, nil)
	if err != nil || pos.Offset != 4 {
		t.Fatalf("append after expiry: %+v %v", pos, err)
	}
}