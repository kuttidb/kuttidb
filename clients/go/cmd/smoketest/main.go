package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/kuttidb/kuttidb/clients/go"
)

func main() {
	var c *kuttidb.Client
	var err error
	if token := os.Getenv("KUTTIDB_AUTH_TOKEN"); token != "" {
		c, err = kuttidb.NewAuthenticated("127.0.0.1:7394", 4, []byte(token))
	} else {
		c, err = kuttidb.New("127.0.0.1:7394", 4)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	caps, err := c.Capabilities()
	if err != nil || caps.Major != 1 {
		log.Fatalf("capabilities: %+v %v", caps, err)
	}
	if healthy, err := c.Health(); err != nil || !healthy {
		log.Fatalf("health: %v %v", healthy, err)
	}

	// single ops
	if err := c.Put("hello", []byte("world")); err != nil {
		log.Fatal(err)
	}
	v, err := c.Get("hello")
	if err != nil || string(v) != "world" {
		log.Fatalf("get: %v %q", err, v)
	}
	if missing, _ := c.Get("nope"); missing != nil {
		log.Fatal("expected miss")
	}
	ok, _ := c.Delete("hello")
	if !ok {
		log.Fatal("delete should report hit")
	}

	// ttl
	if err := c.PutWithTTL("ttl-key", []byte("brief"), 30*time.Second); err != nil {
		log.Fatal(err)
	}
	if v, _ := c.Get("ttl-key"); v == nil {
		log.Fatal("ttl key should be alive")
	}

	// batched + concurrency
	pairs := map[string][]byte{}
	for i := 0; i < 5000; i++ {
		pairs[fmt.Sprintf("g%d", i)] = []byte(fmt.Sprintf("v%d", i))
	}
	if err := c.PutMany(pairs); err != nil {
		log.Fatal(err)
	}
	// batched with per-item TTL
	items := make([]kuttidb.Item, 0, 100)
	for i := 0; i < 100; i++ {
		ttl := time.Duration(30) * time.Second
		if i%2 == 0 {
			ttl = 0
		}
		items = append(items, kuttidb.Item{
			Key: fmt.Sprintf("bt%d", i), Value: []byte("y10"), TTL: ttl,
		})
	}
	if err := c.PutManyTTL(items); err != nil {
		log.Fatal(err)
	}
	if v, _ := c.Get("bt0"); v == nil {
		log.Fatal("bt0 should exist")
	}
	if v, _ := c.Get("bt1"); v == nil {
		log.Fatal("bt1 should exist")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			keys := make([]string, 0, 625)
			for i := id * 625; i < (id+1)*625; i++ {
				keys = append(keys, fmt.Sprintf("g%d", i))
			}
			got, err := c.GetMany(keys)
			if err != nil {
				errCh <- err
				return
			}
			for i, k := range keys {
				want := fmt.Sprintf("v%s", k[1:])
				if string(got[i]) != want {
					errCh <- fmt.Errorf("mismatch %s: %q != %q", k, got[i], want)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		log.Fatal(e)
	}

	// queues, durable consumers, inspection, and batches
	if err := c.QueueDeclare("go-jobs", kuttidb.QueueOptions{Durable: true, MaxDepth: 100}); err != nil {
		log.Fatal(err)
	}
	if err := c.QueuePrefetch(10); err != nil {
		log.Fatal(err)
	}
	if _, err := c.QueuePublish("go-jobs", []byte("one"), 0); err != nil {
		log.Fatal(err)
	}
	delivery, err := c.QueueConsume("go-jobs", 5*time.Second)
	if err != nil || delivery == nil || string(delivery.Value) != "one" {
		log.Fatalf("queue consume: %+v %v", delivery, err)
	}
	if ok, err := c.QueueAck("go-jobs", delivery.DeliveryTag); err != nil || !ok {
		log.Fatalf("queue ack: %v %v", ok, err)
	}
	ids, err := c.QueuePublishBatch("go-jobs", [][]byte{[]byte("two"), []byte("three")})
	if err != nil || len(ids) != 2 {
		log.Fatalf("queue publish batch: %v %v", ids, err)
	}
	deliveries, err := c.QueueConsumeBatch("go-jobs", 2)
	if err != nil || len(deliveries) != 2 {
		log.Fatalf("queue consume batch: %+v %v", deliveries, err)
	}
	tags := []uint64{deliveries[0].DeliveryTag, deliveries[1].DeliveryTag}
	if n, err := c.QueueAckBatch("go-jobs", tags); err != nil || n != 2 {
		log.Fatalf("queue ack batch: %d %v", n, err)
	}
	if _, err := c.QueuePublishBatch("go-jobs", [][]byte{[]byte("drop-1"), []byte("drop-2")}); err != nil {
		log.Fatal(err)
	}
	dropped, err := c.QueueConsumeBatch("go-jobs", 2)
	if err != nil || len(dropped) != 2 {
		log.Fatalf("queue consume for NACK: %+v %v", dropped, err)
	}
	dropTags := []uint64{dropped[0].DeliveryTag, dropped[1].DeliveryTag}
	if n, err := c.QueueNackBatch("go-jobs", dropTags, false); err != nil || n != 2 {
		log.Fatalf("queue nack batch: %d %v", n, err)
	}
	if _, err := c.QueueConsumerRegister("go-worker"); err != nil {
		log.Fatal(err)
	}
	if _, err := c.QueuePublish("go-jobs", []byte("named"), 0); err != nil {
		log.Fatal(err)
	}
	named, err := c.QueueConsumeAs("go-jobs", "go-worker", 5*time.Second)
	if err != nil || named == nil {
		log.Fatalf("named consume: %+v %v", named, err)
	}
	if ok, err := c.QueueNack("go-jobs", named.DeliveryTag, false, 0); err != nil || !ok {
		log.Fatalf("queue nack: %v %v", ok, err)
	}
	if err := c.QueueConsumerUnregister("go-worker"); err != nil {
		log.Fatal(err)
	}
	queues, err := c.QueueList()
	if err != nil || len(queues) == 0 {
		log.Fatalf("queue list: %+v %v", queues, err)
	}
	if stats, err := c.QueueStats("go-jobs"); err != nil || stats == nil {
		log.Fatalf("queue stats: %+v %v", stats, err)
	}
	if err := c.QueueCancel(); err != nil {
		log.Fatal(err)
	}

	// exchanges and every atomic operation
	if err := c.ExchangeDeclare("go-events", kuttidb.ExchangeOptions{Type: kuttidb.ExchangeTopic, Durable: true}); err != nil {
		log.Fatal(err)
	}
	if err := c.ExchangeBind("go-events", "go-jobs", "order.*"); err != nil {
		log.Fatal(err)
	}
	if n, err := c.ExchangePublish("go-events", "order.new", []byte("event"), 0); err != nil || n != 1 {
		log.Fatalf("exchange publish: %d %v", n, err)
	}
	if err := c.QueueDeclare("go-fallback-q", kuttidb.QueueOptions{Durable: true}); err != nil {
		log.Fatal(err)
	}
	if err := c.ExchangeDeclare("go-fallback", kuttidb.ExchangeOptions{Type: kuttidb.ExchangeFanout, Durable: true}); err != nil {
		log.Fatal(err)
	}
	if err := c.ExchangeBind("go-fallback", "go-fallback-q", ""); err != nil {
		log.Fatal(err)
	}
	if err := c.ExchangeDeclare("go-primary", kuttidb.ExchangeOptions{Type: kuttidb.ExchangeDirect, Durable: true, AlternateExchange: "go-fallback"}); err != nil {
		log.Fatal(err)
	}
	if n, err := c.ExchangePublish("go-primary", "missing", []byte("fallback"), 0); err != nil || n != 1 {
		log.Fatalf("alternate exchange: %d %v", n, err)
	}
	if ok, err := c.ExchangeUnbind("go-fallback", "go-fallback-q", ""); err != nil || !ok {
		log.Fatalf("exchange unbind: %v %v", ok, err)
	}
	if r, err := c.PutAndEnqueue("go-atomic-1", []byte("value"), "go-jobs", 0); err != nil || r.TransactionID == 0 {
		log.Fatalf("put enqueue: %+v %v", r, err)
	}
	if r, err := c.PutAndPublish("go-atomic-2", []byte("value"), "go-events", "order.x", 0); err != nil || r.Routed != 1 {
		log.Fatalf("put publish: %+v %v", r, err)
	}
	if r, err := c.UpdateAndEmit("go-atomic-2", []byte("updated"), "go-events", "order.x", 0); err != nil || r.Routed != 1 {
		log.Fatalf("update emit: %+v %v", r, err)
	}
	if r, err := c.DeleteAndPublish("go-atomic-2", "go-events", "order.x", []byte("deleted")); err != nil || r.Routed != 1 {
		log.Fatalf("delete publish: %+v %v", r, err)
	}

	// single-flight and stale-while-revalidate
	loaded, err := c.GetOrLoad("go-load", func() ([]byte, error) { return []byte("loaded"), nil }, 30*time.Second, 5*time.Second, 10*time.Second)
	if err != nil || string(loaded) != "loaded" {
		log.Fatalf("get or load: %q %v", loaded, err)
	}
	if err := c.PutSWR("go-swr", []byte("fresh"), 30*time.Second, time.Minute, 0); err != nil {
		log.Fatal(err)
	}
	swr, err := c.GetOrRefresh("go-swr", 5*time.Second)
	if err != nil || swr.State != kuttidb.StateValue || string(swr.Value) != "fresh" {
		log.Fatalf("SWR: %+v %v", swr, err)
	}
	claim, err := c.GetOrClaim("go-claim", 5*time.Second)
	if err != nil || claim.State != kuttidb.StateClaimed {
		log.Fatalf("claim: %+v %v", claim, err)
	}
	if err := c.ReleaseClaim("go-claim"); err != nil {
		log.Fatal(err)
	}
	swrLoaded, err := c.GetOrLoadSWR("go-swr-load", func() ([]byte, error) { return []byte("swr-loaded"), nil }, 30*time.Second, time.Minute, 0, 5*time.Second, 10*time.Second)
	if err != nil || string(swrLoaded) != "swr-loaded" {
		log.Fatalf("get or load SWR: %q %v", swrLoaded, err)
	}

	// streams, keys, batches, offsets, groups, and inspection
	if err := c.StreamDeclare("go-stream", kuttidb.StreamOptions{Partitions: 2}); err != nil {
		log.Fatal(err)
	}
	pos, err := c.StreamAppend("go-stream", []byte("record"), []byte("key-1"), nil)
	if err != nil {
		log.Fatal(err)
	}
	part := uint32(pos.Partition)
	if _, err := c.StreamAppendBatch("go-stream", []kuttidb.StreamAppend{{Key: []byte("key-2"), Value: []byte("record-2")}}, &part); err != nil {
		log.Fatal(err)
	}
	records, err := c.StreamFetch("go-stream", part, 0, 10)
	if err != nil || len(records) < 2 || string(records[0].Key) != "key-1" {
		log.Fatalf("stream fetch: %+v %v", records, err)
	}
	assignment, err := c.StreamGroupJoin("go-stream", "go-group", 30*time.Second)
	if err != nil || len(assignment.Partitions) == 0 {
		log.Fatalf("group join: %+v %v", assignment, err)
	}
	if err := c.StreamCommit("go-stream", "go-group", part, 1); err != nil {
		log.Fatal(err)
	}
	if err := c.StreamCommitBatch("go-stream", "go-group", []kuttidb.StreamCommit{{Partition: part, Offset: 2}}); err != nil {
		log.Fatal(err)
	}
	offset, err := c.StreamGroupOffset("go-stream", "go-group", part)
	if err != nil || offset == nil || *offset != 2 {
		log.Fatalf("group offset: %v %v", offset, err)
	}
	if _, err := c.StreamGroupLag("go-stream", "go-group", part); err != nil {
		log.Fatal(err)
	}
	streams, err := c.StreamList()
	if err != nil || len(streams) == 0 {
		log.Fatalf("stream list: %+v %v", streams, err)
	}
	groups, err := c.StreamGroupList()
	if err != nil || len(groups) == 0 {
		log.Fatalf("group list: %+v %v", groups, err)
	}
	if err := c.StreamGroupLeave("go-stream", "go-group"); err != nil {
		log.Fatal(err)
	}

	stats, _ := c.Stats()
	fmt.Printf("GO CLIENT OK — stats: %s\n", stats)
}
