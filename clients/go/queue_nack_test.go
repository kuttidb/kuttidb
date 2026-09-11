package kuttidb

// Regression coverage for delayed NACK ownership: the server's 17-byte
// delayed QUEUE_NACK branch previously matched only the connection's own id
// and silently missed deliveries owned by a connection's named consumer.
// This suite drives the public methods in features.go against a real server
// (built by the shared job_test.go harness).

import (
	"fmt"
	"testing"
	"time"
)

func waitForNamedRetry(t *testing.T, c *Client, queue, consumer string) *Delivery {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		d, err := c.QueueConsumeAs(queue, consumer, 30*time.Second)
		if err != nil {
			t.Fatalf("consume-as during retry poll: %v", err)
		}
		if d != nil {
			return d
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("delayed named-consumer retry never became available")
	return nil
}

func TestQueueNamedConsumerDelayedNack(t *testing.T) {
	bin := buildServer(t)
	proc := startJobServer(t, bin, freeJobPort(t), t.TempDir()+"/queues.wal", false)
	c := connectJobClient(t, proc.port)
	defer c.Close()

	// Pad the consumer-owner space above any connection id: owner tokens
	// and connection ids are independent counters, and without the pad a
	// numeric coincidence between them can hide the dispatch defect.
	for i := 0; i < 32; i++ {
		if _, err := c.QueueConsumerRegister(fmt.Sprintf("delay-pad-%d", i)); err != nil {
			t.Fatalf("consumer pad %d: %v", i, err)
		}
	}
	if err := c.QueueDeclare("delaynamed", QueueOptions{Durable: true}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	owner, err := c.QueueConsumerRegister("delay-worker")
	if err != nil || owner == 0 {
		t.Fatalf("consumer register: %v %v", owner, err)
	}
	if _, err := c.QueuePublish("delaynamed", []byte("delayed-payload"), 0); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got, err := c.QueueConsumeAs("delaynamed", "delay-worker", 30*time.Second)
	if err != nil || got == nil {
		t.Fatalf("consume-as: %v %v", got, err)
	}
	if got.Value == nil || string(got.Value) != "delayed-payload" {
		t.Fatalf("unexpected payload %q", got.Value)
	}
	if got.Redelivered || got.DeliveryCount != 1 {
		t.Fatalf("unexpected first delivery %+v", got)
	}
	if stats, _ := c.QueueStats("delaynamed"); stats == nil || stats.Depth != 1 || stats.Inflight != 1 {
		t.Fatalf("stats before nack: %+v", stats)
	}

	ok, err := c.QueueNack("delaynamed", got.DeliveryTag, true, time.Second)
	if err != nil || !ok {
		t.Fatalf("delayed nack of a named-consumer delivery: ok=%v err=%v", ok, err)
	}
	if stats, _ := c.QueueStats("delaynamed"); stats == nil || stats.Depth != 1 || stats.Inflight != 0 {
		t.Fatalf("stats after delayed nack: %+v (depth keeps the live message)", stats)
	}
	// Not consumable before the delay; the poll runs well before the 30s
	// visibility could expire.
	if d, _ := c.QueueConsume("delaynamed", time.Second); d != nil {
		t.Fatalf("delayed message was consumable before its delay: %+v", d)
	}
	again := waitForNamedRetry(t, c, "delaynamed", "delay-worker")
	if again.MessageID != got.MessageID {
		t.Fatalf("retry changed the message id: %d != %d", again.MessageID, got.MessageID)
	}
	if string(again.Value) != "delayed-payload" {
		t.Fatalf("retry payload: %q", again.Value)
	}
	if !again.Redelivered || again.DeliveryCount != 2 {
		t.Fatalf("retry delivery state: %+v", again)
	}
	if again.DeliveryTag == got.DeliveryTag {
		t.Fatalf("retry must carry a fresh delivery tag: %d", again.DeliveryTag)
	}
	if ok, err := c.QueueAck("delaynamed", again.DeliveryTag); err != nil || !ok {
		t.Fatalf("ack of the retry: ok=%v err=%v", ok, err)
	}
	if stats, _ := c.QueueStats("delaynamed"); stats == nil || stats.Depth != 0 || stats.Inflight != 0 {
		t.Fatalf("stats after ack: %+v", stats)
	}
}

func TestQueueNamedConsumerNackAckOwnership(t *testing.T) {
	bin := buildServer(t)
	proc := startJobServer(t, bin, freeJobPort(t), t.TempDir()+"/queues.wal", false)
	c := connectJobClient(t, proc.port)
	defer c.Close()

	for i := 0; i < 32; i++ {
		if _, err := c.QueueConsumerRegister(fmt.Sprintf("own-pad-%d", i)); err != nil {
			t.Fatalf("consumer pad %d: %v", i, err)
		}
	}
	if err := c.QueueDeclare("ownq", QueueOptions{Durable: true}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if _, err := c.QueueConsumerRegister("own-worker"); err != nil {
		t.Fatalf("consumer register: %v", err)
	}
	if _, err := c.QueuePublish("ownq", []byte("owned"), 0); err != nil {
		t.Fatalf("publish: %v", err)
	}
	held, err := c.QueueConsumeAs("ownq", "own-worker", 30*time.Second)
	if err != nil || held == nil {
		t.Fatalf("consume-as: %v %v", held, err)
	}

	// An unrelated connection (different owner token) cannot disposition
	// the delivery: NACK and ACK both miss, state is unchanged.
	outsider, err := New(fmt.Sprintf("127.0.0.1:%d", proc.port), 1)
	if err != nil {
		t.Fatalf("second client: %v", err)
	}
	defer outsider.Close()
	if ok, err := outsider.QueueNack("ownq", held.DeliveryTag, true, 0); err != nil || ok {
		t.Fatalf("foreign delayed nack accepted: ok=%v err=%v", ok, err)
	}
	if ok, err := outsider.QueueNack("ownq", held.DeliveryTag, true, 0); err != nil || ok {
		t.Fatalf("foreign immediate nack accepted: ok=%v err=%v", ok, err)
	}
	if ok, err := outsider.QueueAck("ownq", held.DeliveryTag); err != nil || ok {
		t.Fatalf("foreign ack accepted: ok=%v err=%v", ok, err)
	}
	if ok, err := outsider.QueueNack("ownq", 999999, true, time.Second); err != nil || ok {
		t.Fatalf("stale tag nack accepted: ok=%v err=%v", ok, err)
	}
	if stats, _ := c.QueueStats("ownq"); stats == nil || stats.Depth != 1 || stats.Inflight != 1 {
		t.Fatalf("stats after foreign attempts: %+v", stats)
	}

	// A second connection legitimately attached to the same named consumer
	// shares the owner token and may disposition the delivery.
	if _, err := outsider.QueueConsumerRegister("own-worker"); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if d, _ := outsider.QueueConsumeAs("ownq", "own-worker", 0); d != nil {
		t.Fatalf("unexpected delivery while attaching: %+v", d)
	}
	if ok, err := outsider.QueueNack("ownq", held.DeliveryTag, true, 0); err != nil || !ok {
		t.Fatalf("same-owner nack from a second connection: ok=%v err=%v", ok, err)
	}
	if ok, err := outsider.QueueNack("ownq", held.DeliveryTag, true, 0); err != nil || ok {
		t.Fatalf("duplicate nack of a requeued delivery accepted: ok=%v err=%v", ok, err)
	}
	back, err := c.QueueConsumeAs("ownq", "own-worker", 30*time.Second)
	if err != nil || back == nil {
		t.Fatalf("requeue consume: %v %v", back, err)
	}
	if back.MessageID != held.MessageID || !back.Redelivered {
		t.Fatalf("requeued delivery state: %+v", back)
	}
	if ok, err := c.QueueAck("ownq", back.DeliveryTag); err != nil || !ok {
		t.Fatalf("ack: ok=%v err=%v", ok, err)
	}
}