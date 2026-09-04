import java.nio.charset.StandardCharsets;

/**
 * Wire constants and little-endian helpers for the KuttiDB binary protocol
 * (v1.8). Layouts are specified in docs/design/PROTOCOL.md plus
 * docs/messaging/QUEUES.md, EXCHANGES.md, and STREAMS.md.
 *
 * Not part of the public API.
 */
final class KuttiDBProtocol {

    private KuttiDBProtocol() {}

    // ---- statuses ----------------------------------------------------------
    static final int ST_OK = 0x00;   // OK / HIT
    static final int ST_MISS = 0x01; // MISS (or protocol major mismatch)
    static final int ST_ERR = 0x02;  // ERROR: the durable effect did not happen

    // ---- limits ------------------------------------------------------------
    static final int MAX_KEY = (1 << 16) - 1;
    static final long MAX_VALUE = 64L << 20;
    static final long MAX_TTL_MS = 0xFFFFFFFFL;
    static final long MAX_LEASE_MS = 60_000;

    static final int PROTOCOL_MAJOR = 1;
    static final int PROTOCOL_MINOR = 8;

    // ---- cache / single ops ------------------------------------------------
    static final int OP_PUT = 0x01;
    static final int OP_GET = 0x02;
    static final int OP_DELETE = 0x03;
    static final int OP_STATS = 0x04;
    static final int OP_PUT_TTL = 0x05;
    static final int OP_AUTH = 0x06;
    static final int OP_HEALTH = 0x09;
    static final int OP_CAPABILITIES = 0x0A;
    static final int OP_PUT_SWR = 0x0B;
    static final int OP_SERVER_INFO = 0x0C;
    static final int OP_PUT_BATCH = 0x11;
    static final int OP_GET_BATCH = 0x12;
    static final int OP_PUT_BATCH_TTL = 0x13;

    // ---- queues ------------------------------------------------------------
    static final int OP_QUEUE_DECLARE = 0x20;
    static final int OP_QUEUE_PUBLISH = 0x21;
    static final int OP_QUEUE_CONSUME = 0x22;
    static final int OP_QUEUE_ACK = 0x23;
    static final int OP_QUEUE_NACK = 0x24;
    static final int OP_QUEUE_PUBLISH_TTL = 0x25;
    static final int OP_QUEUE_STATS = 0x26;
    static final int OP_QUEUE_PREFETCH = 0x27;
    static final int OP_QUEUE_CANCEL = 0x28;
    static final int OP_QUEUE_CONSUMER_REGISTER = 0x29;
    static final int OP_QUEUE_CONSUMER_UNREGISTER = 0x2A;
    static final int OP_QUEUE_CONSUME_AS = 0x2B;
    static final int OP_QUEUE_LIST = 0x2C;
    static final int OP_QUEUE_PUBLISH_BATCH = 0x2D;
    static final int OP_QUEUE_CONSUME_BATCH = 0x2E;
    static final int OP_QUEUE_ACK_BATCH = 0x2F;

    // ---- exchanges ---------------------------------------------------------
    static final int OP_EXCHANGE_DECLARE = 0x30;
    static final int OP_EXCHANGE_BIND = 0x31;
    static final int OP_EXCHANGE_UNBIND = 0x32;
    static final int OP_EXCHANGE_PUBLISH = 0x33;

    // ---- atomic cache plus messaging ---------------------------------------
    static final int OP_ATOMIC_PUT_PUBLISH = 0x40;
    static final int OP_ATOMIC_PUT_ENQUEUE = 0x41;
    static final int OP_ATOMIC_DELETE_PUBLISH = 0x42;
    static final int OP_ATOMIC_UPDATE_EMIT = 0x43;

    // ---- single-flight / stale-while-revalidate -----------------------------
    static final int OP_GET_OR_CLAIM = 0x50;
    static final int OP_WAIT_FOR_KEY = 0x51;
    static final int OP_PUT_AND_RELEASE = 0x52;
    static final int OP_RELEASE_CLAIM = 0x53;
    static final int OP_GET_OR_REFRESH = 0x54;

    // ---- streams ------------------------------------------------------------
    static final int OP_STREAM_DECLARE = 0x60;
    static final int OP_STREAM_APPEND = 0x61;
    static final int OP_STREAM_FETCH = 0x62;
    static final int OP_STREAM_COMMIT = 0x63;
    static final int OP_STREAM_GROUP_OFFSET = 0x64;
    static final int OP_STREAM_GROUP_JOIN = 0x65;
    static final int OP_STREAM_GROUP_LAG = 0x66;
    static final int OP_STREAM_APPEND_BATCH = 0x67;
    static final int OP_STREAM_GROUP_LEAVE = 0x68;
    static final int OP_STREAM_LIST = 0x69;
    static final int OP_STREAM_GROUP_LIST = 0x6A;
    static final int OP_STREAM_COMMIT_BATCH = 0x6B;
    static final int OP_STREAM_FETCH_KEYS = 0x6C;

    /** Single-flight / SWR envelope states. */
    static final int SF_VALUE = 0, SF_CLAIMED = 1, SF_WAIT = 2, SF_NEGATIVE = 3,
            SF_RELEASED = 4, SF_TIMEOUT = 5, SF_LOST = 6, SF_STALE = 7, SF_REFRESH = 8;

    // ---- capability feature bits (CAPABILITIES response bitset) -------------
    static final long FEAT_CACHE = 1L << 0;
    static final long FEAT_QUEUES = 1L << 1;
    static final long FEAT_EXCHANGES = 1L << 2;
    static final long FEAT_ATOMIC = 1L << 3;
    static final long FEAT_SINGLEFLIGHT = 1L << 4;
    static final long FEAT_STREAMS = 1L << 5;
    static final long FEAT_STREAM_BATCH = 1L << 6;
    static final long FEAT_HEALTH = 1L << 7;
    static final long FEAT_STREAM_GENERATIONS = 1L << 8;
    static final long FEAT_QUEUE_CONSUMERS = 1L << 9;
    static final long FEAT_ATOMIC_UPDATE = 1L << 10;
    static final long FEAT_SWR = 1L << 11;
    static final long FEAT_QUEUE_BATCH = 1L << 12;
    static final long FEAT_STREAM_COMMIT_BATCH = 1L << 13;
    static final long FEAT_STREAM_KEYS = 1L << 14;
    static final long FEAT_SERVER_INFO = 1L << 15;

    // ---- little-endian encoders --------------------------------------------

    static byte[] u16(int v) {
        return new byte[]{(byte) v, (byte) (v >>> 8)};
    }

    static byte[] u32(long v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }

    static byte[] u64(long v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24),
                (byte) (v >>> 32), (byte) (v >>> 40), (byte) (v >>> 48), (byte) (v >>> 56)};
    }

    static byte[] utf8(String s) {
        return (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
    }

    /** Validate a UTF-8 key length, returning its bytes. */
    static byte[] keyBytes(String key) throws KuttiDBException {
        byte[] kb = utf8(key);
        if (kb.length > MAX_KEY) throw new KuttiDBException("key too large (" + kb.length + " bytes)");
        return kb;
    }

    static void checkValueLength(byte[] value) throws KuttiDBException {
        if (value.length > MAX_VALUE) throw new KuttiDBException("value too large (" + value.length + " bytes)");
    }

    /** Milliseconds clamped into a u32 wire field; 0..2^32-1 after validation. */
    static long ttlMs(long ttlMillis, String what) throws KuttiDBException {
        if (ttlMillis < 0 || ttlMillis > MAX_TTL_MS) {
            throw new KuttiDBException(what + " must be 0.." + MAX_TTL_MS + " ms");
        }
        return ttlMillis;
    }

    static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }

    /**
     * [op][klen:2][vlen:4] request header plus key and value.
     */
    static byte[] frame(int op, byte[] key, byte[] value) {
        return concat(new byte[]{(byte) op}, u16(key.length), u32(value.length), key, value);
    }

    /**
     * PUT_SWR (0x0b) frame: metadata precedes the key and vlen counts the
     * value only: [0x0b][klen:2][vlen:4][ttl_ms:4][stale_ms:4][refresh_ms:4][key][value].
     */
    static byte[] putSwrFrame(byte[] key, byte[] value, long ttlMs, long staleMs, long refreshMs) {
        return concat(new byte[]{(byte) OP_PUT_SWR}, u16(key.length), u32(value.length),
                u32(ttlMs), u32(staleMs), u32(refreshMs), key, value);
    }

    // ---- response decoding --------------------------------------------------

    /** One parsed [status:1][vlen:4][value] response envelope. */
    static final class Reply {
        final int status;
        final byte[] value;

        Reply(int status, byte[] value) {
            this.status = status;
            this.value = value;
        }

        boolean ok() {
            return status == ST_OK;
        }

        boolean miss() {
            return status == ST_MISS;
        }
    }

    /** Fail unless the reply is OK; MISS is refused where the contract is OK-only. */
    static Reply requireOK(Reply r, String what) throws KuttiDBException {
        if (r.status == ST_ERR) throw new KuttiDBException(what + " failed");
        if (!r.ok()) throw new KuttiDBException(what + " failed: unexpected status 0x" + Integer.toHexString(r.status));
        return r;
    }

    /** Length-prefixed UTF-8 string: [len:2][bytes]. */
    static byte[] l16String(String s, String what) throws KuttiDBException {
        byte[] b = utf8(s);
        if (b.length > MAX_KEY) throw new KuttiDBException(what + " too large (" + b.length + " bytes)");
        return concat(u16(b.length), b);
    }


    /** Sequential little-endian reader over one response payload. */
    static final class Decoder {
        private final byte[] b;
        private int i;

        Decoder(byte[] b) {
            this.b = b;
        }

        int remaining() {
            return b.length - i;
        }

        int u8() throws KuttiDBException {
            if (i + 1 > b.length) throw new KuttiDBException("malformed response: truncated");
            return b[i++] & 0xFF;
        }

        int u16() throws KuttiDBException {
            if (i + 2 > b.length) throw new KuttiDBException("malformed response: truncated");
            int v = (b[i] & 0xFF) | ((b[i + 1] & 0xFF) << 8);
            i += 2;
            return v;
        }

        long u32() throws KuttiDBException {
            if (i + 4 > b.length) throw new KuttiDBException("malformed response: truncated");
            long v = (b[i] & 0xFFL) | ((b[i + 1] & 0xFFL) << 8) | ((b[i + 2] & 0xFFL) << 16)
                    | ((b[i + 3] & 0xFFL) << 24);
            i += 4;
            return v;
        }

        long u64() throws KuttiDBException {
            if (i + 8 > b.length) throw new KuttiDBException("malformed response: truncated");
            long v = 0;
            for (int j = 7; j >= 0; j--) v = (v << 8) | (b[i + j] & 0xFFL);
            i += 8;
            return v;
        }

        byte[] bytes(int n) throws KuttiDBException {
            if (n < 0 || i + n > b.length) throw new KuttiDBException("malformed response: truncated");
            byte[] v = new byte[n];
            System.arraycopy(b, i, v, 0, n);
            i += n;
            return v;
        }

        String utf8(int n) throws KuttiDBException {
            return new String(bytes(n), StandardCharsets.UTF_8);
        }

        /** The whole payload must be consumed exactly. */
        void done() throws KuttiDBException {
            if (i != b.length) throw new KuttiDBException("malformed response: trailing bytes");
        }
    }

    /** Read a little-endian u32 that starts at payload offset `at`. */
    static long u32At(byte[] b, int at) {
        return (b[at] & 0xFFL) | ((b[at + 1] & 0xFFL) << 8)
                | ((b[at + 2] & 0xFFL) << 16) | ((b[at + 3] & 0xFFL) << 24);
    }
}
