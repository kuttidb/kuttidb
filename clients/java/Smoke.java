import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Full v1.8 smoke test for the Java client. The Makefile runs it against a
 * fresh server on 127.0.0.1:7394 with durable queue and stream WALs enabled.
 */
public class Smoke {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 7394;

    static KuttiDBClient connect() throws Exception {
        String token = System.getenv("KUTTIDB_AUTH_TOKEN");
        byte[] auth = token == null || token.isEmpty() ? null : token.getBytes(StandardCharsets.UTF_8);
        return new KuttiDBClient(HOST, PORT, auth);
    }

    public static void main(String[] args) throws Exception {
        try (KuttiDBClient c = connect()) {
            capabilitiesAndHealth(c);
            singleOps(c);
            ttlAndBatches(c);
            concurrentGetMany(c);
            queues(c);
            exchangesAndAtomic(c);
            singleFlightAndSwr(c);
            streams(c);
            System.out.println("JAVA CLIENT OK — stats: " + c.stats());
        }
    }

    static void capabilitiesAndHealth(KuttiDBClient c) throws Exception {
        KuttiDBClient.Capabilities caps = c.capabilities();
        if (caps.major != 1) throw new AssertionError("unexpected protocol major " + caps.major);
        if (!caps.hasFeature(KuttiDBClient.FEATURE_QUEUES)
                || !caps.hasFeature(KuttiDBClient.FEATURE_STREAMS)
                || !caps.hasFeature(KuttiDBClient.FEATURE_SWR)) {
            throw new AssertionError("expected a full-featured server, got " + caps);
        }
        if (!c.health()) throw new AssertionError("server should be healthy");
    }

    static void singleOps(KuttiDBClient c) throws Exception {
        c.put("hello", "world".getBytes(StandardCharsets.UTF_8));
        assertEqual("world", new String(c.get("hello"), StandardCharsets.UTF_8));
        if (c.get("nope") != null) throw new AssertionError("expected miss");
        if (!c.delete("hello")) throw new AssertionError("delete should hit");
        if (c.delete("hello")) throw new AssertionError("second delete should miss");
    }

    static void ttlAndBatches(KuttiDBClient c) throws Exception {
        c.put("ttl-key", "brief".getBytes(StandardCharsets.UTF_8), 30000);
        assertEqual("brief", new String(c.get("ttl-key"), StandardCharsets.UTF_8));

        Map<String, byte[]> pairs = new HashMap<>();
        for (int i = 0; i < 5000; i++) pairs.put("j" + i, ("v" + i).getBytes(StandardCharsets.UTF_8));
        c.putMany(pairs);

        List<KuttiDBClient.Item> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            long ttl = (i % 2 == 0) ? 0 : 30_000;
            items.add(new KuttiDBClient.Item("jt" + i, "y10".getBytes(StandardCharsets.UTF_8), ttl));
        }
        c.putManyTTL(items);
        assertEqual("y10", new String(c.get("jt0"), StandardCharsets.UTF_8));
        assertEqual("y10", new String(c.get("jt1"), StandardCharsets.UTF_8));
    }

    /** Exercises the shared pool: one client, eight concurrent workers. */
    static void concurrentGetMany(KuttiDBClient c) throws Exception {
        ExecutorService ex = Executors.newFixedThreadPool(8);
        List<String> allKeys = new ArrayList<>();
        for (int i = 0; i < 5000; i++) allKeys.add("j" + i);
        for (int w = 0; w < 8; w++) {
            final int id = w;
            ex.submit(() -> {
                try {
                    List<String> keys = allKeys.subList(id * 625, (id + 1) * 625);
                    byte[][] got = c.getMany(keys);
                    for (int i = 0; i < keys.size(); i++) {
                        String want = "v" + keys.get(i).substring(1);
                        assertEqual(want, new String(got[i], StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        ex.shutdown();
        if (!ex.awaitTermination(60, TimeUnit.SECONDS)) throw new AssertionError("workers timed out");
    }

    static void queues(KuttiDBClient c) throws Exception {
        c.queueDeclare("java-jobs", new KuttiDBClient.QueueOptions().durable(true).maxDepth(100));
        c.queuePrefetch(10);

        long id = c.queuePublish("java-jobs", "one".getBytes(StandardCharsets.UTF_8));
        if (id == 0) throw new AssertionError("publish should return a message id");
        KuttiDBClient.QueueDelivery d = c.queueConsume("java-jobs", 5000);
        if (d == null || !"one".equals(new String(d.value, StandardCharsets.UTF_8))) {
            throw new AssertionError("queue consume failed");
        }
        if (!c.queueAck("java-jobs", d.deliveryTag)) throw new AssertionError("queue ack failed");

        long[] ids = c.queuePublishBatch("java-jobs", List.of(
                "two".getBytes(StandardCharsets.UTF_8), "three".getBytes(StandardCharsets.UTF_8)));
        if (ids.length != 2) throw new AssertionError("publish batch ids");
        List<KuttiDBClient.QueueDelivery> batch = c.queueConsumeBatch("java-jobs", 2);
        if (batch.size() != 2) throw new AssertionError("consume batch size");
        if (c.queueAckBatch("java-jobs", new long[]{batch.get(0).deliveryTag, batch.get(1).deliveryTag}) != 2) {
            throw new AssertionError("ack batch count");
        }

        c.queuePublishBatch("java-jobs", List.of(
                "drop-1".getBytes(StandardCharsets.UTF_8), "drop-2".getBytes(StandardCharsets.UTF_8)));
        List<KuttiDBClient.QueueDelivery> dropped = c.queueConsumeBatch("java-jobs", 2);
        if (dropped.size() != 2) throw new AssertionError("consume for nack");
        if (c.queueNackBatch("java-jobs", new long[]{dropped.get(0).deliveryTag, dropped.get(1).deliveryTag},
                false) != 2) {
            throw new AssertionError("nack batch count");
        }

        c.queueConsumerRegister("java-worker");
        c.queuePublish("java-jobs", "named".getBytes(StandardCharsets.UTF_8));
        KuttiDBClient.QueueDelivery named = c.queueConsumeAs("java-jobs", "java-worker", 5000);
        if (named == null) throw new AssertionError("named consume failed");
        if (!c.queueNack("java-jobs", named.deliveryTag, false, 0)) {
            throw new AssertionError("named nack failed");
        }
        c.queueConsumerUnregister("java-worker");

        if (c.queueList().isEmpty()) throw new AssertionError("queue list empty");
        KuttiDBClient.QueueStats stats = c.queueStats("java-jobs");
        if (stats == null) throw new AssertionError("queue stats missing");
        if (c.queueStats("missing-queue") != null) throw new AssertionError("stats for missing queue");
        c.queueCancel();
    }

    static void exchangesAndAtomic(KuttiDBClient c) throws Exception {
        c.exchangeDeclare("java-events", new KuttiDBClient.ExchangeOptions()
                .type(KuttiDBClient.ExchangeType.TOPIC).durable(true));
        c.exchangeBind("java-events", "java-jobs", "order.*");
        if (c.exchangePublish("java-events", "order.new", "event".getBytes(StandardCharsets.UTF_8)) != 1) {
            throw new AssertionError("exchange publish should route one copy");
        }

        c.queueDeclare("java-fallback-q", new KuttiDBClient.QueueOptions().durable(true));
        c.exchangeDeclare("java-fallback", new KuttiDBClient.ExchangeOptions()
                .type(KuttiDBClient.ExchangeType.FANOUT).durable(true));
        c.exchangeBind("java-fallback", "java-fallback-q", "");
        c.exchangeDeclare("java-primary", new KuttiDBClient.ExchangeOptions()
                .type(KuttiDBClient.ExchangeType.DIRECT).durable(true)
                .alternateExchange("java-fallback"));
        if (c.exchangePublish("java-primary", "missing", "fallback".getBytes(StandardCharsets.UTF_8)) != 1) {
            throw new AssertionError("alternate exchange should catch the publish");
        }
        if (!c.exchangeUnbind("java-fallback", "java-fallback-q", "")) {
            throw new AssertionError("unbind should report the binding existed");
        }

        KuttiDBClient.AtomicResult r = c.putAndEnqueue("java-atomic-1", "value".getBytes(StandardCharsets.UTF_8),
                "java-jobs", 0);
        if (r.unroutable || r.transactionId == 0) throw new AssertionError("put and enqueue");
        r = c.putAndPublish("java-atomic-2", "value".getBytes(StandardCharsets.UTF_8),
                "java-events", "order.x", 0);
        if (r.unroutable || r.routed != 1) throw new AssertionError("put and publish");
        r = c.updateAndEmit("java-atomic-2", "updated".getBytes(StandardCharsets.UTF_8),
                "java-events", "order.x", 0);
        if (r.unroutable || r.routed != 1) throw new AssertionError("update and emit");
        r = c.deleteAndPublish("java-atomic-2", "java-events", "order.x",
                "deleted".getBytes(StandardCharsets.UTF_8));
        if (r.unroutable || r.routed != 1) throw new AssertionError("delete and publish");
    }

    static void singleFlightAndSwr(KuttiDBClient c) throws Exception {
        byte[] loaded = c.getOrLoad("java-load", () -> "loaded".getBytes(StandardCharsets.UTF_8),
                30000, 5000, 10000);
        assertEqual("loaded", new String(loaded, StandardCharsets.UTF_8));

        c.putSWR("java-swr", "fresh".getBytes(StandardCharsets.UTF_8), 30000, 60000, 0);
        KuttiDBClient.SingleFlightResult swr = c.getOrRefresh("java-swr", 5000);
        if (swr.state != KuttiDBClient.SingleFlightState.VALUE
                || !"fresh".equals(new String(swr.value, StandardCharsets.UTF_8))) {
            throw new AssertionError("SWR fresh read: " + swr.state);
        }

        KuttiDBClient.SingleFlightResult claim = c.getOrClaim("java-claim", 5000);
        if (claim.state != KuttiDBClient.SingleFlightState.CLAIMED) {
            throw new AssertionError("expected claim, got " + claim.state);
        }
        c.releaseClaim("java-claim");

        byte[] swrLoaded = c.getOrLoadSWR("java-swr-load",
                () -> "swr-loaded".getBytes(StandardCharsets.UTF_8), 30000, 60000, 0, 5000, 10000);
        assertEqual("swr-loaded", new String(swrLoaded, StandardCharsets.UTF_8));

        // Contended wait: a holder claims, a second client waits for the wake,
        // and repeated waits on one client keep working.
        try (KuttiDBClient other = connect()) {
            c.getOrClaim("java-wait", 30000);
            Thread wake = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    c.putAndRelease("java-wait", "woke".getBytes(StandardCharsets.UTF_8), 30000, false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            wake.start();
            KuttiDBClient.SingleFlightResult waited = other.waitForKey("java-wait", 30000);
            wake.join();
            if (waited.state != KuttiDBClient.SingleFlightState.VALUE
                    || !"woke".equals(new String(waited.value, StandardCharsets.UTF_8))) {
                throw new AssertionError("contended wait state " + waited.state);
            }

            c.getOrClaim("java-wait-2", 30000);
            Thread release = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    c.releaseClaim("java-wait-2");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            release.start();
            KuttiDBClient.SingleFlightResult released = other.waitForKey("java-wait-2", 30000);
            release.join();
            if (released.state != KuttiDBClient.SingleFlightState.RELEASED) {
                throw new AssertionError("second wait state " + released.state);
            }
        }
    }

    static void streams(KuttiDBClient c) throws Exception {
        c.streamDeclare("java-stream", new KuttiDBClient.StreamOptions().partitions(2));
        KuttiDBClient.StreamPosition pos = c.streamAppend("java-stream",
                "record".getBytes(StandardCharsets.UTF_8), "key-1".getBytes(StandardCharsets.UTF_8), null);
        int part = (int) pos.partition;
        c.streamAppendBatch("java-stream", List.of(KuttiDBClient.StreamAppend.of(
                "key-2".getBytes(StandardCharsets.UTF_8), "record-2".getBytes(StandardCharsets.UTF_8))), part);
        List<KuttiDBClient.StreamRecord> records = c.streamFetch("java-stream", part, 0, 10);
        if (records.size() < 2 || !"key-1".equals(new String(records.get(0).key, StandardCharsets.UTF_8))) {
            throw new AssertionError("keyed stream fetch failed");
        }

        KuttiDBClient.StreamAssignment assignment = c.streamGroupJoin("java-stream", "java-group", 30000);
        if (assignment.partitions.length == 0) throw new AssertionError("empty partition assignment");
        c.streamCommit("java-stream", "java-group", part, 1);
        c.streamCommitBatch("java-stream", "java-group",
                List.of(new KuttiDBClient.StreamCommit(part, 2)));
        Long offset = c.streamGroupOffset("java-stream", "java-group", part);
        if (offset == null || offset != 2) throw new AssertionError("group offset " + offset);
        if (c.streamGroupLag("java-stream", "java-group", part) == null) {
            throw new AssertionError("group lag missing");
        }

        if (c.streamList().isEmpty()) throw new AssertionError("stream list empty");
        if (c.streamGroupList().isEmpty()) throw new AssertionError("group list empty");
        c.streamGroupLeave("java-stream", "java-group");
    }

    static void assertEqual(String a, String b) {
        if (!a.equals(b)) throw new AssertionError(a + " != " + b);
    }
}
