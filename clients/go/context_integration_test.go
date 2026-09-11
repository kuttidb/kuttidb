package kuttidb

// Integration coverage for the context-aware APIs against a real server:
// connection affinity must survive the context plumbing (named-consumer
// deliveries and stream-group membership ride the dedicated state path),
// and job-family contexts must work beyond the pre-canceled case.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestQueueContextAffinity(t *testing.T) {
	bin := buildServer(t)
	proc := startJobServer(t, bin, freeJobPort(t), t.TempDir()+"/queues.wal", false)
	c := connectJobClient(t, proc.port)
	defer c.Close()

	ctx := context.Background()
	for i := 0; i < 32; i++ {
		if _, err := c.QueueConsumerRegisterContext(ctx, fmt.Sprintf("ctx-pad-%d", i)); err != nil {
			t.Fatalf("pad register %d: %v", i, err)
		}
	}
	if err := c.QueueDeclareContext(ctx, "ctxq", QueueOptions{Durable: true}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if _, err := c.QueueConsumerRegisterContext(ctx, "ctx-worker"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := c.QueuePublishContext(ctx, "ctxq", []byte("payload"), 0); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got, err := c.QueueConsumeAsContext(ctx, "ctxq", "ctx-worker", 30*time.Second)
	if err != nil || got == nil {
		t.Fatalf("consume-as: %v %v", got, err)
	}
	ok, err := c.QueueNackContext(ctx, "ctxq", got.DeliveryTag, true, time.Second)
	if err != nil || !ok {
		t.Fatalf("delayed nack via context: ok=%v err=%v", ok, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	again := (*Delivery)(nil)
	for time.Now().Before(deadline) {
		again, err = c.QueueConsumeAsContext(ctx, "ctxq", "ctx-worker", 30*time.Second)
		if err != nil {
			t.Fatalf("retry consume: %v", err)
		}
		if again != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if again == nil || again.MessageID != got.MessageID || !again.Redelivered {
		t.Fatalf("delayed retry via context API: %+v", again)
	}
	if ok, err := c.QueueAckContext(ctx, "ctxq", again.DeliveryTag); err != nil || !ok {
		t.Fatalf("ack via context API: ok=%v err=%v", ok, err)
	}
	if _, err := c.QueueListContext(ctx); err != nil {
		t.Fatalf("list: %v", err)
	}
	if err := c.QueueConsumerUnregisterContext(ctx, "ctx-worker"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if err := c.QueuePrefetchContext(ctx, 2); err != nil {
		t.Fatalf("prefetch: %v", err)
	}
	if err := c.QueueCancelContext(ctx); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestStreamContextAffinity(t *testing.T) {
	bin := buildServer(t)
	proc := startJobServer(t, bin, freeJobPort(t), t.TempDir()+"/k.wal", false)
	c := connectJobClient(t, proc.port)
	defer c.Close()

	ctx := context.Background()
	if err := c.StreamDeclareContext(ctx, "events", StreamOptions{Partitions: 1}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := c.StreamAppendContext(ctx, "events", []byte(fmt.Sprintf("v%d", i)), nil, nil); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := c.StreamGroupJoinContext(ctx, "events", "workers", 30*time.Second); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := c.StreamCommitContext(ctx, "events", "workers", 0, 3); err != nil {
		t.Fatalf("commit: %v", err)
	}
	off, err := c.StreamGroupOffsetContext(ctx, "events", "workers", 0)
	if err != nil || off == nil || *off != 3 {
		t.Fatalf("group offset: %v %v", off, err)
	}
	records, err := c.StreamFetchContext(ctx, "events", 0, 0, 10)
	if err != nil || len(records) != 5 {
		t.Fatalf("fetch: %d %v", len(records), err)
	}
	if err := c.StreamGroupLeaveContext(ctx, "events", "workers"); err != nil {
		t.Fatalf("leave: %v", err)
	}
}

func TestJobContextBeyondPreCanceled(t *testing.T) {
	// The job family already had a pre-canceled-only context test; this
	// exercises real exchanges with deadlines through the shared
	// context-aware transport, including the serialized state path.
	bin := buildServer(t)
	proc := startJobServer(t, bin, freeJobPort(t), t.TempDir()+"/k.wal", true)
	c := connectJobClient(t, proc.port)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.QueueDeclare("jobq", QueueOptions{Durable: true}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if _, err := c.StatePut(ctx, "job:counter", []byte("1"), StateOptions{}); err != nil {
		t.Fatalf("state put: %v", err)
	}
	v, err := c.StateGet(ctx, "job:counter")
	if err != nil || string(v.Value) != "1" {
		t.Fatalf("state get: %v %v", v, err)
	}
	if _, err := c.QueueManifest(ctx); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	// Two concurrent state consumers share the serialized state path; a
	// canceled one must not disturb the other.
	inner, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := c.StateGet(inner, "job:counter"); err != nil {
				done <- err
				return
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled state loop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled state loop did not stop")
	}
	if _, err := c.StateGet(ctx, "job:counter"); err != nil {
		t.Fatalf("state get after concurrent cancel: %v", err)
	}
}