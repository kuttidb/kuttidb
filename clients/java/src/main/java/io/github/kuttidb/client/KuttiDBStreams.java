package io.github.kuttidb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Partitioned stream operations (declare, append, keyed fetch, batches,
 * consumer groups, inventory) plus the anti-cache-stampede single-flight and
 * stale-while-revalidate read paths. Wire layouts are specified in
 * docs/messaging/STREAMS.md and docs/design/PROTOCOL.md.
 *
 * Not part of the public API; see {@link KuttiDBClient} for entry points.
 */
final class KuttiDBStreams {

    private KuttiDBStreams() {}

    private static final int MAX_NAME = 255;
    private static final int MAX_STREAM_BATCH = 1024;
    private static final int MAX_COMMIT_BATCH = 256;
    private static final int MAX_FETCH = 1024;
    private static final long MAX_WINDOW_MS = 7L * 24 * 60 * 60 * 1000; // 7 days

    // ---- streams ------------------------------------------------------------

    static void declare(KuttiDBClient client, String topic, KuttiDBClient.StreamOptions options)
            throws IOException {
        if (options == null) options = new KuttiDBClient.StreamOptions();
        byte[] tb = topicBytes(topic);
        if (options.partitions < 1 || options.partitions > 256) {
            throw new KuttiDBException("stream partitions must be 1..256");
        }
        if (options.maxBytes < 0 || options.maxAgeMillis < 0) {
            throw new KuttiDBException("stream retention must be non-negative");
        }
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u32(options.partitions),
                KuttiDBProtocol.u64(options.maxBytes), KuttiDBProtocol.u64(options.maxAgeMillis));
        KuttiDBProtocol.requireOK(client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_DECLARE, tb, payload)), "stream declare");
    }

    static List<KuttiDBClient.StreamInfo> list(KuttiDBClient client) throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_LIST, new byte[0], new byte[0]));
        KuttiDBProtocol.requireOK(r, "stream list");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        int n = d.u16();
        List<KuttiDBClient.StreamInfo> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String topic = d.utf8(d.u16());
            int partitions = (int) d.u32();
            long records = d.u64();
            long bytes = d.u64();
            out.add(new KuttiDBClient.StreamInfo(topic, partitions, records, bytes));
        }
        d.done();
        return out;
    }

    static List<KuttiDBClient.StreamGroupInfo> groupList(KuttiDBClient client) throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_GROUP_LIST, new byte[0], new byte[0]));
        KuttiDBProtocol.requireOK(r, "stream group list");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        int n = d.u16();
        List<KuttiDBClient.StreamGroupInfo> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String topic = d.utf8(d.u16());
            String group = d.utf8(d.u16());
            long generation = d.u64();
            int members = (int) d.u32();
            out.add(new KuttiDBClient.StreamGroupInfo(topic, group, generation, members));
        }
        d.done();
        return out;
    }

    static KuttiDBClient.StreamPosition append(KuttiDBClient client, String topic, byte[] value,
                                               byte[] key, Integer partition) throws IOException {
        byte[] vb = value == null ? new byte[0] : value;
        KuttiDBProtocol.checkValueLength(vb);
        byte[] kb = streamKey(key);
        long hint = partitionHint(partition);
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u32(hint),
                KuttiDBProtocol.u16(kb.length), kb, vb);
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_APPEND, KuttiDBProtocol.utf8(topic), payload));
        KuttiDBProtocol.requireOK(r, "stream append");
        if (r.value.length != 16) throw new KuttiDBException("malformed stream append response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        return new KuttiDBClient.StreamPosition(d.u64(), d.u64());
    }

    static List<KuttiDBClient.StreamPosition> appendBatch(KuttiDBClient client, String topic,
                                                          List<KuttiDBClient.StreamAppend> items,
                                                          Integer partition) throws IOException {
        if (items == null || items.size() < 1 || items.size() > MAX_STREAM_BATCH) {
            throw new KuttiDBException("stream batch size must be 1-" + MAX_STREAM_BATCH);
        }
        client.requireFeature(KuttiDBProtocol.FEAT_STREAM_BATCH, "stream batch append");
        long hint = partitionHint(partition);
        List<byte[]> parts = new ArrayList<>();
        parts.add(KuttiDBProtocol.u32(hint));
        parts.add(KuttiDBProtocol.u32(items.size()));
        for (KuttiDBClient.StreamAppend item : items) {
            if (item == null) throw new KuttiDBException("stream batch item must not be null");
            byte[] kb = streamKey(item.key);
            byte[] vb = item.value == null ? new byte[0] : item.value;
            KuttiDBProtocol.checkValueLength(vb);
            parts.add(KuttiDBProtocol.concat(KuttiDBProtocol.u16(kb.length),
                    KuttiDBProtocol.u32(vb.length), kb, vb));
        }
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_APPEND_BATCH, KuttiDBProtocol.utf8(topic),
                KuttiDBProtocol.concat(parts.toArray(new byte[0][]))));
        KuttiDBProtocol.requireOK(r, "stream batch append");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        long n = d.u32();
        if (n != items.size()) throw new KuttiDBException("malformed stream batch response");
        List<KuttiDBClient.StreamPosition> out = new ArrayList<>(items.size());
        for (int i = 0; i < n; i++) {
            long p = d.u64();
            long off = d.u64();
            out.add(new KuttiDBClient.StreamPosition(p, off));
        }
        d.done();
        return out;
    }

    /**
     * Replay fetch. Uses the key-preserving fetch (0x6c) when the server
     * advertises capability bit 14; records then carry their keys.
     */
    static List<KuttiDBClient.StreamRecord> fetch(KuttiDBClient client, String topic, int partition,
                                                  long offset, int maxRecords) throws IOException {
        if (partition < 0) throw new KuttiDBException("invalid partition");
        if (offset < 0) throw new KuttiDBException("invalid offset");
        if (maxRecords < 1 || maxRecords > MAX_FETCH) {
            throw new KuttiDBException("invalid fetch count");
        }
        boolean keyed = client.capabilities().hasFeature(KuttiDBProtocol.FEAT_STREAM_KEYS);
        int op = keyed ? KuttiDBProtocol.OP_STREAM_FETCH_KEYS : KuttiDBProtocol.OP_STREAM_FETCH;
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u32(partition),
                KuttiDBProtocol.u64(offset), KuttiDBProtocol.u32(maxRecords));
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(op,
                KuttiDBProtocol.utf8(topic), payload));
        if (r.miss()) return new ArrayList<>(); // unknown topic, or offset beyond the retained range
        KuttiDBProtocol.requireOK(r, "stream fetch");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        long n = d.u32();
        List<KuttiDBClient.StreamRecord> out = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            long off = d.u64();
            int klen = keyed ? d.u16() : 0;
            long vlen = d.u32();
            byte[] key = d.bytes(klen);
            byte[] value = d.bytes((int) vlen);
            out.add(new KuttiDBClient.StreamRecord(off, key, value));
        }
        d.done();
        return out;
    }

    static void commit(KuttiDBClient client, String topic, String group, int partition,
                       long offset) throws IOException {
        byte[] payload = groupPartition(group, partition);
        payload = KuttiDBProtocol.concat(payload, KuttiDBProtocol.u64(offset));
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_COMMIT, KuttiDBProtocol.utf8(topic), payload)),
                "stream commit");
    }

    static void commitBatch(KuttiDBClient client, String topic, String group,
                            List<KuttiDBClient.StreamCommit> commits) throws IOException {
        if (commits == null || commits.size() < 1 || commits.size() > MAX_COMMIT_BATCH) {
            throw new KuttiDBException("commit batch size must be 1-" + MAX_COMMIT_BATCH);
        }
        client.requireFeature(KuttiDBProtocol.FEAT_STREAM_COMMIT_BATCH, "stream commit batch");
        byte[] gb = groupBytes(group);
        List<byte[]> parts = new ArrayList<>();
        parts.add(KuttiDBProtocol.concat(KuttiDBProtocol.u16(gb.length), gb));
        parts.add(KuttiDBProtocol.u32(commits.size()));
        for (KuttiDBClient.StreamCommit c : commits) {
            if (c == null) throw new KuttiDBException("stream commit must not be null");
            if (c.partition < 0 || c.offset < 0) throw new KuttiDBException("invalid stream commit");
            parts.add(KuttiDBProtocol.concat(KuttiDBProtocol.u32(c.partition),
                    KuttiDBProtocol.u64(c.offset)));
        }
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_COMMIT_BATCH, KuttiDBProtocol.utf8(topic),
                KuttiDBProtocol.concat(parts.toArray(new byte[0][])))), "stream commit batch");
    }

    static Long groupValue(KuttiDBClient client, int op, String topic, String group,
                           int partition) throws IOException {
        byte[] payload = groupPartition(group, partition);
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(op,
                KuttiDBProtocol.utf8(topic), payload));
        if (r.miss()) return null;
        KuttiDBProtocol.requireOK(r, "stream group query");
        if (r.value.length != 8) throw new KuttiDBException("malformed stream group response");
        return new KuttiDBProtocol.Decoder(r.value).u64();
    }

    static KuttiDBClient.StreamAssignment groupJoin(KuttiDBClient client, String topic, String group,
                                                    long leaseMillis) throws IOException {
        byte[] gb = groupBytes(group);
        checkLease(leaseMillis);
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(gb.length), gb,
                KuttiDBProtocol.u32(leaseMillis));
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_GROUP_JOIN, KuttiDBProtocol.utf8(topic), payload));
        KuttiDBProtocol.requireOK(r, "stream group join");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        long n = d.u32();
        if (n > 256) throw new KuttiDBException("malformed stream assignment");
        int[] partitions = new int[(int) n];
        for (int i = 0; i < n; i++) partitions[i] = (int) d.u32();
        long generation = 0;
        if (d.remaining() > 0) generation = d.u64();
        d.done();
        return new KuttiDBClient.StreamAssignment(partitions, generation);
    }

    static void groupLeave(KuttiDBClient client, String topic, String group) throws IOException {
        byte[] gb = groupBytes(group);
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(gb.length), gb);
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STREAM_GROUP_LEAVE, KuttiDBProtocol.utf8(topic), payload)),
                "stream group leave");
    }

    // ---- single-flight -------------------------------------------------------

    static KuttiDBClient.SingleFlightResult getOrClaim(KuttiDBClient client, String key,
                                                       long leaseMillis) throws IOException {
        checkLease(leaseMillis);
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_GET_OR_CLAIM, KuttiDBProtocol.keyBytes(key),
                KuttiDBProtocol.u32(leaseMillis)));
        return decodeSingleFlight(r, false);
    }

    static KuttiDBClient.SingleFlightResult waitForKey(KuttiDBClient client, String key,
                                                       long timeoutMillis) throws IOException {
        checkLease(timeoutMillis);
        // The server defers this response until a wake or the deadline; give
        // the socket read timeout headroom beyond the server-side deadline.
        int readTimeout = (int) Math.min(timeoutMillis + 5_000, Integer.MAX_VALUE);
        // A wait consumes the connection's one deferred-wait slot for its
        // lifetime, so it runs on a throwaway connection (see
        // KuttiDBClient.ephemeralRequest) and repeated waits keep working.
        KuttiDBProtocol.Reply r = client.ephemeralRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_WAIT_FOR_KEY, KuttiDBProtocol.keyBytes(key),
                KuttiDBProtocol.u32(timeoutMillis)), readTimeout);
        return decodeSingleFlight(r, false);
    }

    static void putAndRelease(KuttiDBClient client, String key, byte[] value, long ttlMillis,
                              boolean negative) throws IOException {
        long ttl = KuttiDBProtocol.ttlMs(ttlMillis, "ttl");
        byte[] v = value == null ? new byte[0] : value;
        KuttiDBProtocol.checkValueLength(v);
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u32(ttl),
                new byte[]{(byte) (negative ? 1 : 0)}, v);
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_PUT_AND_RELEASE, KuttiDBProtocol.keyBytes(key), payload)),
                "put and release");
    }

    static void releaseClaim(KuttiDBClient client, String key) throws IOException {
        KuttiDBProtocol.requireOK(client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_RELEASE_CLAIM, KuttiDBProtocol.keyBytes(key), new byte[0])),
                "release claim");
    }

    static KuttiDBClient.SingleFlightResult getOrRefresh(KuttiDBClient client, String key,
                                                         long leaseMillis) throws IOException {
        client.requireFeature(KuttiDBProtocol.FEAT_SWR, "stale-while-revalidate");
        checkLease(leaseMillis);
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_GET_OR_REFRESH, KuttiDBProtocol.keyBytes(key),
                KuttiDBProtocol.u32(leaseMillis)));
        return decodeSingleFlight(r, true);
    }

    // ---- stale-while-revalidate ----------------------------------------------

    static void putSWR(KuttiDBClient client, String key, byte[] value, long ttlMillis,
                       long staleMillis, long refreshMillis) throws IOException {
        client.requireFeature(KuttiDBProtocol.FEAT_SWR, "stale-while-revalidate");
        if (ttlMillis <= 0 || staleMillis <= 0 || ttlMillis > MAX_WINDOW_MS
                || staleMillis > MAX_WINDOW_MS || refreshMillis < 0 || refreshMillis > MAX_WINDOW_MS) {
            throw new KuttiDBException("invalid SWR window");
        }
        byte[] kb = KuttiDBProtocol.keyBytes(key);
        byte[] v = value == null ? new byte[0] : value;
        KuttiDBProtocol.checkValueLength(v);
        byte[] frame = KuttiDBProtocol.putSwrFrame(kb, v, ttlMillis, staleMillis, refreshMillis);
        KuttiDBProtocol.requireOK(client.pooledRequest(frame), "put SWR");
    }

    // ---- single-flight loading -----------------------------------------------

    static byte[] getOrLoad(KuttiDBClient client, String key, KuttiDBClient.Loader loader,
                            long ttlMillis, long leaseMillis, long waitMillis) throws IOException {
        KuttiDBClient.SingleFlightResult r = getOrClaim(client, key, leaseMillis);
        if (r.state == KuttiDBClient.SingleFlightState.VALUE) return r.value;
        if (r.state == KuttiDBClient.SingleFlightState.NEGATIVE) return null;
        if (r.state == KuttiDBClient.SingleFlightState.WAIT) {
            for (int i = 0; i < 3; i++) {
                KuttiDBClient.SingleFlightResult w = waitForKey(client, key, waitMillis);
                if (w.state == KuttiDBClient.SingleFlightState.VALUE) return w.value;
                if (w.state == KuttiDBClient.SingleFlightState.NEGATIVE
                        || w.state == KuttiDBClient.SingleFlightState.TIMEOUT) return null;
                r = getOrClaim(client, key, leaseMillis);
                if (r.state == KuttiDBClient.SingleFlightState.VALUE) return r.value;
                if (r.state == KuttiDBClient.SingleFlightState.CLAIMED) break;
            }
        }
        if (r.state != KuttiDBClient.SingleFlightState.CLAIMED) return null;
        byte[] loaded;
        try {
            loaded = loader.load();
        } catch (IOException e) {
            releaseClaim(client, key);
            throw e;
        }
        if (loaded == null) {
            putAndRelease(client, key, null, ttlMillis, true);
            return null;
        }
        putAndRelease(client, key, loaded, ttlMillis, false);
        return loaded;
    }

    static byte[] getOrLoadSWR(KuttiDBClient client, String key, KuttiDBClient.Loader loader,
                               long ttlMillis, long staleMillis, long refreshMillis,
                               long leaseMillis, long waitMillis) throws IOException {
        KuttiDBClient.SingleFlightResult r = getOrRefresh(client, key, leaseMillis);
        if (r.state == KuttiDBClient.SingleFlightState.VALUE) return r.value;
        if (r.state == KuttiDBClient.SingleFlightState.NEGATIVE) return null;
        if ((r.state == KuttiDBClient.SingleFlightState.STALE
                || r.state == KuttiDBClient.SingleFlightState.REFRESH) && !r.holder) {
            return r.value;
        }
        if (r.state == KuttiDBClient.SingleFlightState.WAIT) {
            for (int i = 0; i < 3; i++) {
                KuttiDBClient.SingleFlightResult w = waitForKey(client, key, waitMillis);
                if (w.state == KuttiDBClient.SingleFlightState.VALUE) return w.value;
                if (w.state == KuttiDBClient.SingleFlightState.NEGATIVE
                        || w.state == KuttiDBClient.SingleFlightState.TIMEOUT) return null;
                r = getOrRefresh(client, key, leaseMillis);
                if (r.state == KuttiDBClient.SingleFlightState.VALUE) return r.value;
                if ((r.state == KuttiDBClient.SingleFlightState.STALE
                        || r.state == KuttiDBClient.SingleFlightState.REFRESH) && !r.holder) {
                    return r.value;
                }
                if (r.state == KuttiDBClient.SingleFlightState.CLAIMED || r.holder) break;
            }
        }
        if (r.state != KuttiDBClient.SingleFlightState.CLAIMED && !r.holder) return null;
        byte[] loaded;
        try {
            loaded = loader.load();
        } catch (IOException e) {
            releaseClaim(client, key);
            throw e;
        }
        if (loaded == null) {
            putAndRelease(client, key, null, ttlMillis, true);
            return null;
        }
        try {
            putSWR(client, key, loaded, ttlMillis, staleMillis, refreshMillis);
        } catch (IOException e) {
            releaseClaim(client, key);
            throw e;
        }
        releaseClaim(client, key);
        return loaded;
    }

    // ---- helpers --------------------------------------------------------------

    /**
     * Envelope: [state:1][value] with an extra holder byte on GET_OR_REFRESH;
     * the value is attached for value / stale / refresh states.
     */
    private static KuttiDBClient.SingleFlightResult decodeSingleFlight(KuttiDBProtocol.Reply r,
                                                                       boolean holder)
            throws KuttiDBException {
        KuttiDBProtocol.requireOK(r, "single-flight operation");
        if (r.value.length < (holder ? 2 : 1)) {
            throw new KuttiDBException("malformed single-flight response");
        }
        KuttiDBClient.SingleFlightState state = KuttiDBClient.SingleFlightState.of(r.value[0] & 0xFF);
        boolean isHolder = holder && r.value[1] != 0;
        int at = holder ? 2 : 1;
        byte[] value = null;
        if (state == KuttiDBClient.SingleFlightState.VALUE
                || state == KuttiDBClient.SingleFlightState.STALE
                || state == KuttiDBClient.SingleFlightState.REFRESH) {
            value = new byte[r.value.length - at];
            System.arraycopy(r.value, at, value, 0, value.length);
        }
        return new KuttiDBClient.SingleFlightResult(state, isHolder, value);
    }

    private static void checkLease(long millis) throws KuttiDBException {
        if (millis < 1 || millis > KuttiDBProtocol.MAX_LEASE_MS) {
            throw new KuttiDBException("lease must be 1.." + KuttiDBProtocol.MAX_LEASE_MS + " ms");
        }
    }

    private static long partitionHint(Integer partition) throws KuttiDBException {
        if (partition == null) return 0xFFFFFFFFL;
        if (partition < 0) throw new KuttiDBException("invalid partition");
        return partition & 0xFFFFFFFFL;
    }

    private static byte[] streamKey(byte[] key) throws KuttiDBException {
        byte[] kb = key == null ? new byte[0] : key;
        if (kb.length > 0xFFFF) throw new KuttiDBException("stream key too large");
        return kb;
    }

    /** [glen:2][group][partition:4]. */
    private static byte[] groupPartition(String group, int partition) throws KuttiDBException {
        byte[] gb = groupBytes(group);
        if (partition < 0) throw new KuttiDBException("invalid partition");
        return KuttiDBProtocol.concat(KuttiDBProtocol.u16(gb.length), gb,
                KuttiDBProtocol.u32(partition));
    }

    private static byte[] groupBytes(String group) throws KuttiDBException {
        if (group == null || group.isEmpty() || KuttiDBProtocol.utf8(group).length > MAX_NAME) {
            throw new KuttiDBException("invalid stream group");
        }
        return KuttiDBProtocol.utf8(group);
    }

    private static byte[] topicBytes(String topic) throws KuttiDBException {
        if (topic == null || topic.isEmpty() || KuttiDBProtocol.utf8(topic).length > MAX_NAME) {
            throw new KuttiDBException("invalid stream topic");
        }
        return KuttiDBProtocol.utf8(topic);
    }
}
