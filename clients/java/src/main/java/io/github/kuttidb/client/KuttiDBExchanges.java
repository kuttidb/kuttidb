import java.io.IOException;

/**
 * Exchange routing and atomic cache-plus-messaging operations: declare, bind,
 * unbind, publish (with alternate-exchange support), plus the four atomic
 * opcodes that commit a cache mutation and a queue delivery under one durable
 * commit id. Wire layouts are specified in docs/messaging/EXCHANGES.md and
 * docs/design/DURABILITY.md.
 *
 * Not part of the public API; see {@link KuttiDBClient} for entry points.
 */
final class KuttiDBExchanges {

    private KuttiDBExchanges() {}

    private static final int MAX_NAME = 255;

    static void declare(KuttiDBClient client, String name, KuttiDBClient.ExchangeOptions options)
            throws IOException {
        if (options == null) options = new KuttiDBClient.ExchangeOptions();
        if (options.type == null) throw new KuttiDBException("invalid exchange");
        byte[] kb = KuttiDBProtocol.keyBytes(name);
        if (kb.length > MAX_NAME) throw new KuttiDBException("exchange name too large");
        byte[] v = KuttiDBProtocol.concat(new byte[]{(byte) (options.durable ? 1 : 0), options.type.wire});
        if (!options.alternateExchange.isEmpty()) {
            byte[] alt = KuttiDBProtocol.utf8(options.alternateExchange);
            if (alt.length > MAX_NAME) throw new KuttiDBException("alternate exchange name too large");
            byte[] ext = KuttiDBProtocol.concat(KuttiDBProtocol.u16(alt.length), alt);
            v = KuttiDBProtocol.concat(v, KuttiDBProtocol.u16(ext.length), ext);
        }
        KuttiDBProtocol.requireOK(client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_EXCHANGE_DECLARE, kb, v)), "exchange declare");
    }

    static void bind(KuttiDBClient client, String exchange, String queue, String routingKey)
            throws IOException {
        byte[] payload = bindingPayload(queue, routingKey);
        KuttiDBProtocol.requireOK(client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_EXCHANGE_BIND, KuttiDBProtocol.keyBytes(exchange), payload)),
                "exchange bind");
    }

    static boolean unbind(KuttiDBClient client, String exchange, String queue, String routingKey)
            throws IOException {
        byte[] payload = bindingPayload(queue, routingKey);
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_EXCHANGE_UNBIND, KuttiDBProtocol.keyBytes(exchange), payload));
        if (r.status == KuttiDBProtocol.ST_ERR) throw new KuttiDBException("exchange unbind failed");
        return r.ok();
    }

    static int publish(KuttiDBClient client, String exchange, String routingKey, byte[] value,
                       long ttlMillis) throws IOException {
        byte[] rkb = KuttiDBProtocol.utf8(routingKey == null ? "" : routingKey);
        if (rkb.length > KuttiDBProtocol.MAX_KEY) throw new KuttiDBException("routing key too large");
        byte[] v = value == null ? new byte[0] : value;
        KuttiDBProtocol.checkValueLength(v);
        long ttl = KuttiDBProtocol.ttlMs(ttlMillis, "ttl");
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(rkb.length),
                KuttiDBProtocol.u64(ttl), rkb, v);
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_EXCHANGE_PUBLISH, KuttiDBProtocol.keyBytes(exchange), payload));
        if (r.miss()) return 0; // unroutable: nothing was committed
        KuttiDBProtocol.requireOK(r, "exchange publish");
        if (r.value.length != 4) throw new KuttiDBException("malformed exchange response");
        return (int) KuttiDBProtocol.u32At(r.value, 0);
    }

    /** PUT_AND_PUBLISH / UPDATE_AND_EMIT share one payload layout. */
    static KuttiDBClient.AtomicResult atomicExchange(int op, KuttiDBClient client, String key,
                                                     byte[] value, String exchange, String routingKey,
                                                     long ttlMillis) throws IOException {
        byte[] xb = routeBytes(exchange);
        byte[] rkb = routeBytes(routingKey);
        byte[] v = value == null ? new byte[0] : value;
        KuttiDBProtocol.checkValueLength(v);
        long ttl = KuttiDBProtocol.ttlMs(ttlMillis, "ttl");
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(xb.length), xb,
                KuttiDBProtocol.u16(rkb.length), rkb, KuttiDBProtocol.u32(ttl), v);
        return atomic(client, op, key, payload);
    }

    static KuttiDBClient.AtomicResult atomicEnqueue(KuttiDBClient client, String key, byte[] value,
                                                    String queue, long ttlMillis) throws IOException {
        byte[] qb = routeBytes(queue);
        byte[] v = value == null ? new byte[0] : value;
        KuttiDBProtocol.checkValueLength(v);
        long ttl = KuttiDBProtocol.ttlMs(ttlMillis, "ttl");
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(qb.length), qb,
                KuttiDBProtocol.u32(ttl), v);
        return atomic(client, KuttiDBProtocol.OP_ATOMIC_PUT_ENQUEUE, key, payload);
    }

    static KuttiDBClient.AtomicResult atomicDeletePublish(KuttiDBClient client, String key,
                                                          String exchange, String routingKey,
                                                          byte[] message) throws IOException {
        byte[] xb = routeBytes(exchange);
        byte[] rkb = routeBytes(routingKey);
        if (message == null) message = new byte[0];
        KuttiDBProtocol.checkValueLength(message);
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(xb.length), xb,
                KuttiDBProtocol.u16(rkb.length), rkb, KuttiDBProtocol.u32(message.length), message);
        return atomic(client, KuttiDBProtocol.OP_ATOMIC_DELETE_PUBLISH, key, payload);
    }

    /** Response is [tx_id:8][routed:4]; MISS means unroutable and nothing committed. */
    private static KuttiDBClient.AtomicResult atomic(KuttiDBClient client, int op, String key,
                                                     byte[] payload) throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(op,
                KuttiDBProtocol.keyBytes(key), payload));
        if (r.miss()) return new KuttiDBClient.AtomicResult(0, 0, true);
        KuttiDBProtocol.requireOK(r, "atomic operation");
        if (r.value.length != 12) throw new KuttiDBException("malformed atomic response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        return new KuttiDBClient.AtomicResult(d.u64(), (int) d.u32(), false);
    }

    /** [qlen:2][queue][rklen:2][routing-key] binding payload. */
    private static byte[] bindingPayload(String queue, String routingKey) throws KuttiDBException {
        if (queue == null || queue.isEmpty()) throw new KuttiDBException("invalid binding");
        byte[] qb = KuttiDBProtocol.utf8(queue);
        if (qb.length > MAX_NAME) throw new KuttiDBException("invalid binding");
        byte[] rkb = KuttiDBProtocol.utf8(routingKey == null ? "" : routingKey);
        if (rkb.length > KuttiDBProtocol.MAX_KEY) throw new KuttiDBException("routing key too large");
        return KuttiDBProtocol.concat(KuttiDBProtocol.u16(qb.length), qb,
                KuttiDBProtocol.u16(rkb.length), rkb);
    }

    private static byte[] routeBytes(String route) throws KuttiDBException {
        byte[] b = KuttiDBProtocol.utf8(route == null ? "" : route);
        if (b.length > KuttiDBProtocol.MAX_KEY) throw new KuttiDBException("atomic route too large");
        return b;
    }
}
