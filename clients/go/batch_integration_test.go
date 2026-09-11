package kuttidb

// Integration coverage for the KV batch families (PutMany, PutManyTTL,
// GetMany) against a real server: these use raw-status/segment responses
// rather than framed bodies, so they need their own end-to-end coverage.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCacheBatchContextIntegration(t *testing.T) {
	bin := buildServer(t)
	proc := startJobServer(t, bin, freeJobPort(t), t.TempDir()+"/k.wal", false)
	c := connectJobClient(t, proc.port)
	defer c.Close()
	ctx := context.Background()

	// Multi-chunk batch: > 2*BatchSize items share one deadline budget.
	pairs := map[string][]byte{}
	for i := 0; i < 2*BatchSize+40; i++ {
		pairs[fmt.Sprintf("kb%d", i)] = []byte(fmt.Sprintf("v%d", i))
	}
	if err := c.PutManyContext(ctx, pairs); err != nil {
		t.Fatalf("PutManyContext: %v", err)
	}
	// Reads across chunk boundaries, misses included.
	keys := make([]string, 0, len(pairs)+3)
	for k := range pairs {
		keys = append(keys, k)
	}
	keys = append(keys, "missing-1", "kb0", "missing-2")
	values, err := c.GetManyContext(ctx, keys)
	if err != nil {
		t.Fatalf("GetManyContext: %v", err)
	}
	if len(values) != len(keys) {
		t.Fatalf("GetManyContext length: %d", len(values))
	}
	for i, k := range keys {
		if k == "missing-1" || k == "missing-2" {
			if values[i] != nil {
				t.Fatalf("miss returned a value: %s", k)
			}
			continue
		}
		want := fmt.Sprintf("v%s", k[2:])
		if string(values[i]) != want {
			t.Fatalf("mismatch %s: %q != %q", k, values[i], want)
		}
	}
	// Per-item TTL: even items expire, odd items persist.
	items := make([]Item, 0, 8)
	for i := 0; i < 8; i++ {
		it := Item{Key: fmt.Sprintf("ttl%d", i), Value: []byte("y")}
		if i%2 == 0 {
			it.TTL = time.Second
		}
		items = append(items, it)
	}
	if err := c.PutManyTTLContext(ctx, items); err != nil {
		t.Fatalf("PutManyTTLContext: %v", err)
	}
	if v, _ := c.Get("ttl1"); v == nil {
		t.Fatal("odd TTL item should exist")
	}
	time.Sleep(1200 * time.Millisecond)
	if v, _ := c.Get("ttl0"); v != nil {
		t.Fatal("even item should have expired")
	}
	if v, _ := c.Get("ttl1"); v == nil {
		t.Fatal("odd item should persist")
	}
	// A canceled context fails the whole batch without sending chunks.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.PutManyContext(canceled, pairs); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PutManyContext: %v", err)
	}
	if _, err := c.GetManyContext(canceled, keys); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled GetManyContext: %v", err)
	}
	// Legacy wrappers still work against a real server.
	if err := c.PutMany(pairs); err != nil {
		t.Fatalf("PutMany legacy: %v", err)
	}
	if _, err := c.GetMany(keys); err != nil {
		t.Fatalf("GetMany legacy: %v", err)
	}
}