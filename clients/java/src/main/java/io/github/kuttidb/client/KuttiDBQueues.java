package io.github.kuttidb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Native durable queue operations: declare/publish/consume/ACK/NACK,
 * TTL publishes, prefetch and durable named consumers, inventory, and the
 * batch operations (capability bit 12). Wire layouts are specified in
 * docs/messaging/QUEUES.md.
 *
 * Not part of the public API; see {@link KuttiDBClient} for entry points.
 */
final class KuttiDBQueues {

    private KuttiDBQueues() {}

    private static final int MAX_NAME = 255;
    private static final int MAX_BATCH = 256;

    static void declare(KuttiDBClient client, String name, KuttiDBClient.QueueOptions options)
            throws IOException {
        if (options == null) options = new KuttiDBClient.QueueOptions();
        byte[] kb = nameBytes(name);
        byte[] v = KuttiDBProtocol.concat(new byte[]{(byte) (options.durable ? 1 : 0)},
                KuttiDBProtocol.u64(options.maxDepth));
        if (!options.deadLetterQueue.isEmpty()) {
            byte[] dlq = utf8(options.deadLetterQueue);
            if (dlq.length > MAX_NAME) throw new KuttiDBException("dead-letter queue name too large");
            byte[] ext = KuttiDBProtocol.concat(KuttiDBProtocol.u16(dlq.length), dlq,
                    KuttiDBProtocol.u32(options.maxDeliveries));
            v = KuttiDBProtocol.concat(v, KuttiDBProtocol.u16(ext.length), ext);
        }
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_DECLARE, kb, v)), "queue declare");
    }

    static List<KuttiDBClient.QueueInfo> list(KuttiDBClient client) throws IOException {
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_LIST, new byte[0], new byte[0]));
        KuttiDBProtocol.requireOK(r, "queue list");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        int n = d.u16();
        List<KuttiDBClient.QueueInfo> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String name = d.utf8(d.u16());
            long depth = d.u64();
            long inflight = d.u64();
            out.add(new KuttiDBClient.QueueInfo(name, depth, inflight));
        }
        d.done();
        return out;
    }

    static long publish(KuttiDBClient client, String name, byte[] value, Long ttlMillis)
            throws IOException {
        byte[] kb = nameBytes(name);
        KuttiDBProtocol.checkValueLength(value);
        int op;
        byte[] payload;
        if (ttlMillis == null) {
            op = KuttiDBProtocol.OP_QUEUE_PUBLISH;
            payload = value;
        } else {
            op = KuttiDBProtocol.OP_QUEUE_PUBLISH_TTL;
            payload = KuttiDBProtocol.concat(KuttiDBProtocol.u64(
                    KuttiDBProtocol.ttlMs(ttlMillis, "ttl")), value);
        }
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(op, kb, payload));
        KuttiDBProtocol.requireOK(r, "queue publish");
        if (r.value.length != 8) throw new KuttiDBException("malformed queue publish response");
        return new KuttiDBProtocol.Decoder(r.value).u64();
    }

    static KuttiDBClient.QueueDelivery consume(KuttiDBClient client, String name, String consumer,
                                               long visibilityMillis) throws IOException {
        byte[] kb = nameBytes(name);
        if (visibilityMillis < 0) throw new KuttiDBException("visibility must be non-negative");
        byte[] payload;
        int op = KuttiDBProtocol.OP_QUEUE_CONSUME;
        if (consumer == null) {
            payload = KuttiDBProtocol.u64(visibilityMillis);
        } else {
            if (consumer.isEmpty() || utf8(consumer).length > MAX_NAME) {
                throw new KuttiDBException("invalid consumer");
            }
            op = KuttiDBProtocol.OP_QUEUE_CONSUME_AS;
            payload = KuttiDBProtocol.concat(KuttiDBProtocol.l16String(consumer, "consumer"),
                    KuttiDBProtocol.u64(visibilityMillis));
        }
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(op, kb, payload));
        if (r.miss()) return null;
        KuttiDBProtocol.requireOK(r, "queue consume");
        return decodeDelivery(r.value);
    }

    static boolean ack(KuttiDBClient client, String name, long deliveryTag) throws IOException {
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_ACK, nameBytes(name), KuttiDBProtocol.u64(deliveryTag)));
        if (r.status == KuttiDBProtocol.ST_ERR) throw new KuttiDBException("queue ack failed");
        return r.ok();
    }

    static boolean nack(KuttiDBClient client, String name, long deliveryTag, boolean requeue,
                        long delayMillis) throws IOException {
        if (delayMillis < 0) throw new KuttiDBException("delay must be non-negative");
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u64(deliveryTag),
                new byte[]{(byte) (requeue ? 1 : 0)});
        if (delayMillis > 0) {
            payload = KuttiDBProtocol.concat(payload, KuttiDBProtocol.u64(delayMillis));
        }
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_NACK, nameBytes(name), payload));
        if (r.status == KuttiDBProtocol.ST_ERR) throw new KuttiDBException("queue nack failed");
        return r.ok();
    }

    static KuttiDBClient.QueueStats stats(KuttiDBClient client, String name) throws IOException {
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_STATS, nameBytes(name), new byte[0]));
        if (r.miss()) return null;
        KuttiDBProtocol.requireOK(r, "queue stats");
        if (r.value.length != 16) throw new KuttiDBException("malformed queue stats response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        return new KuttiDBClient.QueueStats(d.u64(), d.u64());
    }

    static void prefetch(KuttiDBClient client, int count) throws IOException {
        if (count < 0 || count > 0xFFFFFFFFL) throw new KuttiDBException("invalid prefetch count");
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_PREFETCH, new byte[]{'_'},
                KuttiDBProtocol.u32(count))), "queue prefetch");
    }

    static void cancel(KuttiDBClient client) throws IOException {
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_CANCEL, new byte[]{'_'}, new byte[0])), "queue cancel");
    }

    static long consumerRegister(KuttiDBClient client, String name) throws IOException {
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_CONSUMER_REGISTER, nameBytes(name), new byte[0]));
        KuttiDBProtocol.requireOK(r, "consumer register");
        if (r.value.length != 8) throw new KuttiDBException("malformed consumer response");
        return new KuttiDBProtocol.Decoder(r.value).u64();
    }

    static void consumerUnregister(KuttiDBClient client, String name) throws IOException {
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_CONSUMER_UNREGISTER, nameBytes(name), new byte[0])),
                "consumer unregister");
    }

    static long[] publishBatch(KuttiDBClient client, String name, List<byte[]> values)
            throws IOException {
        if (values == null || values.size() < 1 || values.size() > MAX_BATCH) {
            throw new KuttiDBException("queue batch size must be 1-" + MAX_BATCH);
        }
        client.requireFeature(KuttiDBProtocol.FEAT_QUEUE_BATCH, "queue batches");
        List<byte[]> parts = new ArrayList<>();
        parts.add(KuttiDBProtocol.u32(values.size()));
        for (byte[] value : values) {
            if (value == null) throw new KuttiDBException("queue batch value must not be null");
            KuttiDBProtocol.checkValueLength(value);
            parts.add(KuttiDBProtocol.concat(KuttiDBProtocol.u32(value.length), value));
        }
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_PUBLISH_BATCH, nameBytes(name),
                KuttiDBProtocol.concat(parts.toArray(new byte[0][]))));
        KuttiDBProtocol.requireOK(r, "queue publish batch");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        long n = d.u32();
        if (n != values.size()) throw new KuttiDBException("malformed batch response");
        long[] ids = new long[values.size()];
        for (int i = 0; i < ids.length; i++) ids[i] = d.u64();
        d.done();
        return ids;
    }

    static List<KuttiDBClient.QueueDelivery> consumeBatch(KuttiDBClient client, String name,
                                                          int maxCount) throws IOException {
        if (maxCount < 1 || maxCount > MAX_BATCH) {
            throw new KuttiDBException("queue batch size must be 1-" + MAX_BATCH);
        }
        client.requireFeature(KuttiDBProtocol.FEAT_QUEUE_BATCH, "queue batches");
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_CONSUME_BATCH, nameBytes(name),
                KuttiDBProtocol.u32(maxCount)));
        if (r.miss()) return new ArrayList<>();
        KuttiDBProtocol.requireOK(r, "queue consume batch");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        long n = d.u32();
        List<KuttiDBClient.QueueDelivery> out = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            long tag = d.u64();
            long messageId = d.u64();
            long count = d.u32();
            int flag = d.u8();
            long vlen = d.u32();
            byte[] body = d.bytes((int) vlen);
            out.add(new KuttiDBClient.QueueDelivery(tag, messageId, (int) count, flag != 0, body));
        }
        d.done();
        return out;
    }

    /** One round trip for ACK (mode 0), NACK+requeue (1), or NACK without requeue (2). */
    static int dispositionBatch(KuttiDBClient client, String name, long[] deliveryTags, byte mode)
            throws IOException {
        if (deliveryTags == null || deliveryTags.length < 1 || deliveryTags.length > MAX_BATCH) {
            throw new KuttiDBException("queue batch size must be 1-" + MAX_BATCH);
        }
        client.requireFeature(KuttiDBProtocol.FEAT_QUEUE_BATCH, "queue batches");
        List<byte[]> parts = new ArrayList<>();
        parts.add(new byte[]{mode});
        parts.add(KuttiDBProtocol.u32(deliveryTags.length));
        for (long tag : deliveryTags) parts.add(KuttiDBProtocol.u64(tag));
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_ACK_BATCH, nameBytes(name),
                KuttiDBProtocol.concat(parts.toArray(new byte[0][]))));
        KuttiDBProtocol.requireOK(r, "queue disposition batch");
        if (r.value.length != 4) throw new KuttiDBException("malformed batch response");
        return (int) KuttiDBProtocol.u32At(r.value, 0);
    }

    /** Delivery envelope: [tag:8][message_id:8][redelivered:1][delivery_count:4][value]. */
    private static KuttiDBClient.QueueDelivery decodeDelivery(byte[] value) throws KuttiDBException {
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(value);
        long tag = d.u64();
        long messageId = d.u64();
        int flag = d.u8();
        long count = d.u32();
        byte[] body = d.bytes(d.remaining());
        return new KuttiDBClient.QueueDelivery(tag, messageId, (int) count, flag != 0, body);
    }

    private static byte[] nameBytes(String name) throws KuttiDBException {
        if (name == null || name.isEmpty() || utf8(name).length > MAX_NAME) {
            throw new KuttiDBException("invalid queue name");
        }
        return KuttiDBProtocol.utf8(name);
    }

    private static byte[] utf8(String s) {
        return KuttiDBProtocol.utf8(s);
    }
}
