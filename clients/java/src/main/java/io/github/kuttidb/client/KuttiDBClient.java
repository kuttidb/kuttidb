import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * Java client for the KuttiDB binary protocol (v1.8).
 *
 * Thread-safe: a fixed pool of native connections is shared across callers
 * (extra connections are dialed under load and closed when released).
 * Connection-bound operations — queue deliveries, single-flight claims and
 * waits, and stream group membership — run over one dedicated connection that
 * this class serializes internally, so a crashed loader can never strand
 * another caller's lease.
 *
 * Misses are not exceptions: {@link #get} returns {@code null},
 * {@link #delete} returns {@code false}, {@link #queueConsume} returns
 * {@code null}, unroutable publishes report zero routes, and so on, matching
 * the other native clients. Failures surface as {@link KuttiDBException}
 * (an {@link IOException}).
 *
 * Batched methods ({@link #putMany}, {@link #getMany}, {@link #putManyTTL})
 * send up to {@link #BATCH_SIZE} operations per round trip.
 */
public class KuttiDBClient implements AutoCloseable {

    /** Maximum operations per KV batch round trip. */
    public static final int BATCH_SIZE = 256;

    static final int DEFAULT_POOL_SIZE = 4;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    static final int OP_TIMEOUT_MS = 30_000;

    // ---- cache feature bits exposed for capability checks -------------------
    /** Cache, queues, exchanges, atomic, single-flight, streams, ... — see docs/design/PROTOCOL.md. */
    public static final long FEATURE_CACHE = KuttiDBProtocol.FEAT_CACHE;
    public static final long FEATURE_QUEUES = KuttiDBProtocol.FEAT_QUEUES;
    public static final long FEATURE_EXCHANGES = KuttiDBProtocol.FEAT_EXCHANGES;
    public static final long FEATURE_ATOMIC = KuttiDBProtocol.FEAT_ATOMIC;
    public static final long FEATURE_SINGLE_FLIGHT = KuttiDBProtocol.FEAT_SINGLEFLIGHT;
    public static final long FEATURE_STREAMS = KuttiDBProtocol.FEAT_STREAMS;
    public static final long FEATURE_STREAM_BATCH_APPEND = KuttiDBProtocol.FEAT_STREAM_BATCH;
    public static final long FEATURE_HEALTH = KuttiDBProtocol.FEAT_HEALTH;
    public static final long FEATURE_STREAM_GENERATIONS = KuttiDBProtocol.FEAT_STREAM_GENERATIONS;
    public static final long FEATURE_QUEUE_CONSUMERS = KuttiDBProtocol.FEAT_QUEUE_CONSUMERS;
    public static final long FEATURE_ATOMIC_UPDATE = KuttiDBProtocol.FEAT_ATOMIC_UPDATE;
    public static final long FEATURE_SWR = KuttiDBProtocol.FEAT_SWR;
    public static final long FEATURE_QUEUE_BATCH = KuttiDBProtocol.FEAT_QUEUE_BATCH;
    public static final long FEATURE_STREAM_COMMIT_BATCH = KuttiDBProtocol.FEAT_STREAM_COMMIT_BATCH;
    public static final long FEATURE_STREAM_KEYS = KuttiDBProtocol.FEAT_STREAM_KEYS;
    public static final long FEATURE_SERVER_INFO = KuttiDBProtocol.FEAT_SERVER_INFO;

    // ======================================================================
    // Public value types
    // ======================================================================

    /** Key/value pair with optional TTL for {@link #putManyTTL(List)}. */
    public static final class Item {
        public final String key;
        public final byte[] value;
        public final long ttlMillis; // 0 = no expiry

        public Item(String key, byte[] value, long ttlMillis) {
            this.key = key;
            this.value = value;
            this.ttlMillis = ttlMillis;
        }
    }

    /** Capability discovery result: protocol version plus feature bitset. */
    public static final class Capabilities {
        public final int major;
        public final int minor;
        public final long features;

        public Capabilities(int major, int minor, long features) {
            this.major = major;
            this.minor = minor;
            this.features = features;
        }

        public boolean hasFeature(long bit) {
            return (features & bit) != 0;
        }

        @Override
        public String toString() {
            return "Capabilities{major=" + major + ", minor=" + minor
                    + ", features=0x" + Long.toHexString(features) + "}";
        }
    }

    /** Queue declaration options: {@code new QueueOptions().durable(true).maxDepth(100)}. */
    public static final class QueueOptions {
        public boolean durable;
        public long maxDepth;
        public String deadLetterQueue = "";
        public long maxDeliveries;

        public QueueOptions durable(boolean v) { this.durable = v; return this; }
        public QueueOptions maxDepth(long v) { this.maxDepth = v; return this; }
        public QueueOptions deadLetterQueue(String v) { this.deadLetterQueue = v == null ? "" : v; return this; }
        public QueueOptions maxDeliveries(long v) { this.maxDeliveries = v; return this; }
    }

    /** One entry of {@link #queueList()}. */
    public static final class QueueInfo {
        public final String name;
        public final long depth;
        public final long inflight;

        public QueueInfo(String name, long depth, long inflight) {
            this.name = name;
            this.depth = depth;
            this.inflight = inflight;
        }
    }

    /** Depth/inflight snapshot for one queue, or {@code null} when it does not exist. */
    public static final class QueueStats {
        public final long depth;
        public final long inflight;

        public QueueStats(long depth, long inflight) {
            this.depth = depth;
            this.inflight = inflight;
        }
    }

    /** One message handed to a consumer. ACK/NACK by {@link #deliveryTag}. */
    public static final class QueueDelivery {
        public final long deliveryTag;
        public final long messageId;
        public final int deliveryCount;
        public final boolean redelivered;
        public final byte[] value;

        public QueueDelivery(long deliveryTag, long messageId, int deliveryCount, boolean redelivered, byte[] value) {
            this.deliveryTag = deliveryTag;
            this.messageId = messageId;
            this.deliveryCount = deliveryCount;
            this.redelivered = redelivered;
            this.value = value;
        }
    }

    /** Routing topology: direct, fanout, or topic. */
    public enum ExchangeType {
        DIRECT((byte) 0), FANOUT((byte) 1), TOPIC((byte) 2);

        final byte wire;

        ExchangeType(byte wire) { this.wire = wire; }
    }

    /** Exchange declaration options: {@code new ExchangeOptions().type(ExchangeType.TOPIC).durable(true)}. */
    public static final class ExchangeOptions {
        public ExchangeType type = ExchangeType.DIRECT;
        public boolean durable;
        public String alternateExchange = "";

        public ExchangeOptions type(ExchangeType v) { this.type = v == null ? ExchangeType.DIRECT : v; return this; }
        public ExchangeOptions durable(boolean v) { this.durable = v; return this; }
        public ExchangeOptions alternateExchange(String v) { this.alternateExchange = v == null ? "" : v; return this; }
    }

    /** Result of an atomic cache-plus-message operation. */
    public static final class AtomicResult {
        public final long transactionId;
        public final int routed;
        /** True when nothing matched the route; nothing was committed. */
        public final boolean unroutable;

        public AtomicResult(long transactionId, int routed, boolean unroutable) {
            this.transactionId = transactionId;
            this.routed = routed;
            this.unroutable = unroutable;
        }
    }

    /** Single-flight / stale-while-revalidate read states. */
    public enum SingleFlightState {
        VALUE, CLAIMED, WAIT, NEGATIVE, RELEASED, TIMEOUT, LOST, STALE, REFRESH;

        static SingleFlightState of(int wire) throws KuttiDBException {
            SingleFlightState[] all = values();
            if (wire < 0 || wire >= all.length) throw new KuttiDBException("malformed single-flight response");
            return all[wire];
        }
    }

    /** Result of a single-flight or SWR read; {@link #value} is set for VALUE/STALE/REFRESH. */
    public static final class SingleFlightResult {
        public final SingleFlightState state;
        /** GET_OR_REFRESH only: this caller owns the revalidation lease. */
        public final boolean holder;
        public final byte[] value;

        public SingleFlightResult(SingleFlightState state, boolean holder, byte[] value) {
            this.state = state;
            this.holder = holder;
            this.value = value;
        }
    }

    /** Loads a missing value for {@link #getOrLoad} / {@link #getOrLoadSWR}. */
    @FunctionalInterface
    public interface Loader {
        byte[] load() throws IOException;
    }

    /** Stream declaration options. */
    public static final class StreamOptions {
        public int partitions = 1;
        public long maxBytes;
        public long maxAgeMillis;

        public StreamOptions partitions(int v) { this.partitions = v; return this; }
        public StreamOptions maxBytes(long v) { this.maxBytes = v; return this; }
        public StreamOptions maxAgeMillis(long v) { this.maxAgeMillis = v; return this; }
    }

    /** One entry of {@link #streamList()}. */
    public static final class StreamInfo {
        public final String topic;
        public final int partitions;
        public final long records;
        public final long bytes;

        public StreamInfo(String topic, int partitions, long records, long bytes) {
            this.topic = topic;
            this.partitions = partitions;
            this.records = records;
            this.bytes = bytes;
        }
    }

    /** One entry of {@link #streamGroupList()}. */
    public static final class StreamGroupInfo {
        public final String topic;
        public final String group;
        public final long generation;
        public final int members;

        public StreamGroupInfo(String topic, String group, long generation, int members) {
            this.topic = topic;
            this.group = group;
            this.generation = generation;
            this.members = members;
        }
    }

    /** One fetched stream record; {@link #key} is empty for the unkeyed fetch. */
    public static final class StreamRecord {
        public final long offset;
        public final byte[] key;
        public final byte[] value;

        public StreamRecord(long offset, byte[] key, byte[] value) {
            this.offset = offset;
            this.key = key;
            this.value = value;
        }
    }

    /** Append position returned by stream appends. */
    public static final class StreamPosition {
        public final long partition;
        public final long offset;

        public StreamPosition(long partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    /** One record of a stream batch append. */
    public static final class StreamAppend {
        public final byte[] key;
        public final byte[] value;

        public StreamAppend(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public static StreamAppend of(byte[] key, byte[] value) {
            return new StreamAppend(key, value);
        }
    }

    /** One partition offset of a batch commit. */
    public static final class StreamCommit {
        public final int partition;
        public final long offset;

        public StreamCommit(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    /** Partition assignment from {@link #streamGroupJoin}. */
    public static final class StreamAssignment {
        public final int[] partitions;
        public final long generation;

        public StreamAssignment(int[] partitions, long generation) {
            this.partitions = partitions;
            this.generation = generation;
        }
    }

    // ======================================================================
    // Connection plumbing
    // ======================================================================

    /** One native connection: TCP, TLS, or a Unix domain socket. */
    private static final class Conn implements Closeable {
        final Socket socket;          // TCP / TLS; null for Unix sockets
        final SocketChannel channel;  // Unix socket; null for TCP / TLS
        final OutputStream out;
        final DataInputStream in;
        int soTimeout = OP_TIMEOUT_MS;

        Conn(Socket socket) throws IOException {
            this.socket = socket;
            this.channel = null;
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(soTimeout);
            this.out = socket.getOutputStream();
            this.in = new DataInputStream(socket.getInputStream());
        }

        Conn(SocketChannel channel) throws IOException {
            this.socket = null;
            this.channel = channel;
            try {
                channel.socket().setSoTimeout(soTimeout);
            } catch (RuntimeException ignored) {
                // Some channel sockets refuse option tweaks; reads stay blocking.
            }
            this.out = Channels.newOutputStream(channel);
            this.in = new DataInputStream(Channels.newInputStream(channel));
        }

        void setTimeout(int millis) throws IOException {
            if (millis == soTimeout) return;
            soTimeout = millis;
            try {
                if (socket != null) socket.setSoTimeout(millis);
                else if (channel != null) channel.socket().setSoTimeout(millis);
            } catch (RuntimeException ignored) {
                // Keep going with the previous timeout behavior.
            }
        }

        void writeFully(byte[] b) throws IOException {
            out.write(b);
            out.flush();
        }

        void readFully(byte[] b) throws IOException {
            in.readFully(b);
        }

        /** Read one [status:1][vlen:4][value] envelope. */
        KuttiDBProtocol.Reply readReply() throws IOException {
            byte[] head = new byte[5];
            in.readFully(head);
            int status = head[0] & 0xFF;
            long vlen = KuttiDBProtocol.u32At(head, 1);
            if (vlen > KuttiDBProtocol.MAX_VALUE) throw new IOException("response too large");
            byte[] value = new byte[(int) vlen];
            if (vlen > 0) in.readFully(value);
            return new KuttiDBProtocol.Reply(status, value);
        }

        @Override
        public void close() throws IOException {
            if (socket != null) socket.close();
            else if (channel != null) channel.close();
        }
    }

    private final String host;        // TCP / TLS endpoint
    private final int port;
    private final String unixPath;    // direct Unix socket endpoint
    private final byte[] authToken;
    private final SSLContext tlsContext;
    private final int poolSize;

    private final Deque<Conn> idle = new ArrayDeque<>();
    private final Object lock = new Object();
    private volatile boolean closed;

    private Conn stateConn;
    private final Object stateLock = new Object();

    // ---- construction -------------------------------------------------------

    public KuttiDBClient(String host, int port) throws IOException {
        this(host, port, null, null, 0);
    }

    /** Connect and authenticate with a 1..1024 byte pre-shared token. */
    public KuttiDBClient(String host, int port, byte[] authToken) throws IOException {
        this(host, port, authToken, null, 0);
    }

    /** Connect over TLS with hostname verification, then optionally AUTH. */
    public KuttiDBClient(String host, int port, byte[] authToken, SSLContext tlsContext) throws IOException {
        this(host, port, authToken, tlsContext, 0);
    }

    /**
     * Pooled client. {@code poolSize} idle connections are dialed eagerly
     * (0 selects the default of 4); extra connections are created on demand
     * under concurrency and closed when the pool is full again.
     */
    public KuttiDBClient(String host, int port, byte[] authToken, SSLContext tlsContext, int poolSize)
            throws IOException {
        this(host, port, null, authToken, tlsContext, poolSize);
    }

    /** Connect to a Unix domain socket with optional AUTH (default pool size). */
    public static KuttiDBClient unix(String socketPath) throws IOException {
        return unix(socketPath, null, 0);
    }

    public static KuttiDBClient unix(String socketPath, byte[] authToken) throws IOException {
        return unix(socketPath, authToken, 0);
    }

    /** Pooled Unix-socket client; the pool is dialed eagerly. */
    public static KuttiDBClient unix(String socketPath, byte[] authToken, int poolSize) throws IOException {
        if (socketPath == null || socketPath.isEmpty()) {
            throw new KuttiDBException("unix socket path is required");
        }
        return new KuttiDBClient(null, 0, socketPath, authToken, null, poolSize);
    }

    private KuttiDBClient(String host, int port, String unixPath, byte[] authToken,
                          SSLContext tlsContext, int poolSize) throws IOException {
        if (host == null && unixPath == null) throw new KuttiDBException("an endpoint is required");
        if (authToken != null && (authToken.length == 0 || authToken.length > 1024)) {
            throw new KuttiDBException("auth token must contain 1..1024 bytes");
        }
        this.host = host;
        this.port = port;
        this.unixPath = unixPath;
        this.authToken = authToken;
        this.tlsContext = tlsContext;
        this.poolSize = poolSize > 0 ? poolSize : DEFAULT_POOL_SIZE;
        // Eager fill: fail fast on an unreachable server and let managed
        // instances keep their native lifecycle lease while idle.
        List<Conn> initial = new ArrayList<>(this.poolSize);
        try {
            for (int i = 0; i < this.poolSize; i++) initial.add(dial());
        } catch (IOException e) {
            closeAll(initial);
            throw e;
        }
        synchronized (lock) {
            idle.addAll(initial);
        }
    }

    private static void closeAll(List<Conn> conns) {
        for (Conn cn : conns) {
            try { cn.close(); } catch (IOException ignored) {}
        }
    }

    private Conn dial() throws IOException {
        Conn cn;
        if (unixPath != null) {
            SocketChannel channel = SocketChannel.open(UnixDomainSocketAddress.of(unixPath));
            try {
                cn = new Conn(channel);
            } catch (IOException e) {
                try { channel.close(); } catch (IOException ignored) {}
                throw e;
            }
        } else if (tlsContext != null) {
            SSLSocket tlsSock = (SSLSocket) tlsContext.getSocketFactory().createSocket();
            try {
                SSLParameters params = tlsSock.getSSLParameters();
                params.setEndpointIdentificationAlgorithm("HTTPS");
                tlsSock.setSSLParameters(params);
                tlsSock.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
                tlsSock.startHandshake();
                cn = new Conn(tlsSock);
            } catch (IOException e) {
                try { tlsSock.close(); } catch (IOException ignored) {}
                throw e;
            }
        } else {
            Socket sock = new Socket();
            try {
                sock.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
                cn = new Conn(sock);
            } catch (IOException e) {
                try { sock.close(); } catch (IOException ignored) {}
                throw e;
            }
        }
        try {
            authenticate(cn);
            return cn;
        } catch (IOException e) {
            try { cn.close(); } catch (IOException ignored) {}
            throw e;
        }
    }

    private void authenticate(Conn cn) throws IOException {
        if (authToken == null) return;
        cn.writeFully(KuttiDBProtocol.frame(KuttiDBProtocol.OP_AUTH, authToken, new byte[0]));
        KuttiDBProtocol.Reply r = cn.readReply();
        if (!r.ok()) throw new KuttiDBException("authentication failed");
    }

    private Conn acquire() throws IOException {
        synchronized (lock) {
            if (closed) throw new KuttiDBException("client is closed");
            Conn cn = idle.pollFirst();
            if (cn != null) return cn;
        }
        return dial();
    }

    private void release(Conn cn) {
        boolean keep;
        synchronized (lock) {
            keep = !closed && idle.size() < poolSize;
            if (keep) idle.addFirst(cn);
        }
        if (!keep) killQuietly(cn);
    }

    private void kill(Conn cn) {
        killQuietly(cn);
    }

    private static void killQuietly(Conn cn) {
        try { cn.close(); } catch (IOException ignored) {}
    }

    /** Write one frame and read its [status][vlen][value] envelope. */
    private static KuttiDBProtocol.Reply roundTrip(Conn cn, byte[] frame, int readTimeoutMillis)
            throws IOException {
        cn.setTimeout(readTimeoutMillis);
        cn.writeFully(frame);
        return cn.readReply();
    }

    /** Pooled request with the default operation timeout. */
    KuttiDBProtocol.Reply pooledRequest(byte[] frame) throws IOException {
        return pooledRequest(frame, OP_TIMEOUT_MS);
    }

    KuttiDBProtocol.Reply pooledRequest(byte[] frame, int readTimeoutMillis) throws IOException {
        Conn cn = acquire();
        boolean keep = false;
        try {
            KuttiDBProtocol.Reply r = roundTrip(cn, frame, readTimeoutMillis);
            keep = true;
            return r;
        } finally {
            if (keep) release(cn);
            else kill(cn);
        }
    }

    /**
     * Request on the dedicated state connection. Queue deliveries, prefetch,
     * single-flight leases and waits, and stream group membership are owned
     * by one native connection; a failed I/O round trip replaces it.
     */
    KuttiDBProtocol.Reply stateRequest(byte[] frame) throws IOException {
        return stateRequest(frame, OP_TIMEOUT_MS);
    }

    KuttiDBProtocol.Reply stateRequest(byte[] frame, int readTimeoutMillis) throws IOException {
        synchronized (stateLock) {
            if (closed) throw new KuttiDBException("client is closed");
            if (stateConn == null) stateConn = dial();
            try {
                return roundTrip(stateConn, frame, readTimeoutMillis);
            } catch (IOException e) {
                kill(stateConn);
                stateConn = null;
                throw e;
            }
        }
    }

    /**
     * One-shot request on a connection that is closed instead of pooled.
     *
     * The server grants every native connection exactly one deferred
     * WAIT_FOR_KEY slot for its lifetime; after the wake is delivered the
     * slot stays consumed and any further wait on the same connection is
     * refused. Hosting a wait therefore disqualifies a connection from
     * reuse, so each wait runs on a throwaway connection and repeated waits
     * keep working.
     */
    KuttiDBProtocol.Reply ephemeralRequest(byte[] frame, int readTimeoutMillis) throws IOException {
        Conn cn = acquire();
        try {
            return roundTrip(cn, frame, readTimeoutMillis);
        } finally {
            kill(cn);
        }
    }

    // ======================================================================
    // Discovery: capabilities, health, server info
    // ======================================================================

    /**
     * Ask the server for its protocol version and feature bitset. MISS means
     * an incompatible major version.
     */
    public Capabilities capabilities() throws IOException {
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.u16(KuttiDBProtocol.PROTOCOL_MAJOR),
                KuttiDBProtocol.u16(KuttiDBProtocol.PROTOCOL_MINOR));
        KuttiDBProtocol.Reply r = pooledRequest(KuttiDBProtocol.frame(KuttiDBProtocol.OP_CAPABILITIES,
                new byte[0], payload));
        if (r.miss()) throw new KuttiDBException("incompatible protocol major");
        KuttiDBProtocol.requireOK(r, "capabilities");
        if (r.value.length != 12) throw new KuttiDBException("malformed capabilities response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        int major = d.u16();
        int minor = d.u16();
        long features = d.u64();
        d.done();
        return new Capabilities(major, minor, features);
    }

    /**
     * True only while the cache, queue, and stream persistence engines are
     * all writable. MISS and ERROR both report unhealthy.
     */
    public boolean health() throws IOException {
        KuttiDBProtocol.Reply r = pooledRequest(KuttiDBProtocol.frame(KuttiDBProtocol.OP_HEALTH,
                new byte[0], new byte[0]));
        return r.ok();
    }

    /** Throw {@link KuttiDBException} when the server lacks a feature bit. */
    void requireFeature(long bit, String name) throws IOException {
        if (!capabilities().hasFeature(bit)) {
            throw new KuttiDBException("server does not support " + name);
        }
    }

    // ======================================================================
    // Cache single ops
    // ======================================================================

    /** Put with a time-to-live in milliseconds (0 = no expiry). */
    public void put(String key, byte[] value, long ttlMillis) throws IOException {
        byte[] kb = KuttiDBProtocol.keyBytes(key);
        KuttiDBProtocol.checkValueLength(value);
        long ttl = KuttiDBProtocol.ttlMs(ttlMillis, "ttl");
        byte[] frame = KuttiDBProtocol.concat(new byte[]{(byte) KuttiDBProtocol.OP_PUT_TTL},
                KuttiDBProtocol.u16(kb.length), KuttiDBProtocol.u32(value.length),
                KuttiDBProtocol.u32(ttl), kb, value);
        KuttiDBProtocol.requireOK(pooledRequest(frame), "put");
    }

    public void put(String key, byte[] value) throws IOException {
        byte[] kb = KuttiDBProtocol.keyBytes(key);
        KuttiDBProtocol.checkValueLength(value);
        KuttiDBProtocol.requireOK(pooledRequest(KuttiDBProtocol.frame(KuttiDBProtocol.OP_PUT, kb, value)), "put");
    }

    /** {@code null} on miss. */
    public byte[] get(String key) throws IOException {
        byte[] kb = KuttiDBProtocol.keyBytes(key);
        KuttiDBProtocol.Reply r = pooledRequest(KuttiDBProtocol.frame(KuttiDBProtocol.OP_GET, kb, new byte[0]));
        if (r.miss()) return null;
        KuttiDBProtocol.requireOK(r, "get");
        return r.value;
    }

    /** {@code true} when the key existed. */
    public boolean delete(String key) throws IOException {
        byte[] kb = KuttiDBProtocol.keyBytes(key);
        KuttiDBProtocol.Reply r = pooledRequest(KuttiDBProtocol.frame(KuttiDBProtocol.OP_DELETE, kb, new byte[0]));
        if (r.status == KuttiDBProtocol.ST_ERR) throw new KuttiDBException("delete failed");
        return r.ok();
    }

    /** Server STATS JSON payload. */
    public String stats() throws IOException {
        KuttiDBProtocol.Reply r = pooledRequest(KuttiDBProtocol.frame(KuttiDBProtocol.OP_STATS,
                new byte[0], new byte[0]));
        KuttiDBProtocol.requireOK(r, "stats");
        return new String(r.value, StandardCharsets.UTF_8);
    }

    // ======================================================================
    // KV batches
    // ======================================================================

    /** Batched put: one round trip per {@link #BATCH_SIZE} pairs. */
    public void putMany(Map<String, byte[]> pairs) throws IOException {
        List<String> keys = new ArrayList<>(pairs.keySet());
        for (int start = 0; start < keys.size(); start += BATCH_SIZE) {
            List<String> chunk = keys.subList(start, Math.min(start + BATCH_SIZE, keys.size()));
            byte[][] keyBytes = new byte[chunk.size()][];
            byte[][] vals = new byte[chunk.size()][];
            long size = 7;
            for (int i = 0; i < chunk.size(); i++) {
                keyBytes[i] = KuttiDBProtocol.keyBytes(chunk.get(i));
                vals[i] = pairs.get(chunk.get(i));
                if (vals[i] == null) throw new KuttiDBException("putMany value must not be null");
                KuttiDBProtocol.checkValueLength(vals[i]);
                size += 6L + keyBytes[i].length + vals[i].length;
                if (size > KuttiDBProtocol.MAX_VALUE) throw new KuttiDBException("batch too large");
            }
            byte[] head = KuttiDBProtocol.concat(new byte[]{(byte) KuttiDBProtocol.OP_PUT_BATCH},
                    new byte[2], KuttiDBProtocol.u32(chunk.size()));
            List<byte[]> parts = new ArrayList<>();
            parts.add(head);
            for (int i = 0; i < chunk.size(); i++) {
                parts.add(KuttiDBProtocol.concat(KuttiDBProtocol.u16(keyBytes[i].length),
                        KuttiDBProtocol.u32(vals[i].length), keyBytes[i], vals[i]));
            }
            byte[] frame = KuttiDBProtocol.concat(parts.toArray(new byte[0][]));
            Conn cn = acquire();
            boolean keep = false;
            try {
                cn.writeFully(frame);
                int status = cn.in.readUnsignedByte();
                keep = true;
                if (status != KuttiDBProtocol.ST_OK) throw new KuttiDBException("putMany failed");
            } finally {
                if (keep) release(cn);
                else kill(cn);
            }
        }
    }

    /** Batched put with per-item TTL: one round trip per {@link #BATCH_SIZE} items. */
    public void putManyTTL(List<Item> items) throws IOException {
        for (int start = 0; start < items.size(); start += BATCH_SIZE) {
            List<Item> chunk = items.subList(start, Math.min(start + BATCH_SIZE, items.size()));
            byte[][] keyBytes = new byte[chunk.size()][];
            long[] ttls = new long[chunk.size()];
            long size = 7;
            for (int i = 0; i < chunk.size(); i++) {
                Item it = chunk.get(i);
                keyBytes[i] = KuttiDBProtocol.keyBytes(it.key);
                if (it.value == null) throw new KuttiDBException("putManyTTL value must not be null");
                KuttiDBProtocol.checkValueLength(it.value);
                ttls[i] = KuttiDBProtocol.ttlMs(it.ttlMillis, "ttl");
                size += 10L + keyBytes[i].length + it.value.length;
                if (size > KuttiDBProtocol.MAX_VALUE) throw new KuttiDBException("batch too large");
            }
            byte[] head = KuttiDBProtocol.concat(new byte[]{(byte) KuttiDBProtocol.OP_PUT_BATCH_TTL},
                    new byte[2], KuttiDBProtocol.u32(chunk.size()));
            List<byte[]> parts = new ArrayList<>();
            parts.add(head);
            for (int i = 0; i < chunk.size(); i++) {
                Item it = chunk.get(i);
                parts.add(KuttiDBProtocol.concat(KuttiDBProtocol.u16(keyBytes[i].length),
                        KuttiDBProtocol.u32(it.value.length), KuttiDBProtocol.u32(ttls[i]),
                        keyBytes[i], it.value));
            }
            byte[] frame = KuttiDBProtocol.concat(parts.toArray(new byte[0][]));
            Conn cn = acquire();
            boolean keep = false;
            try {
                cn.writeFully(frame);
                int status = cn.in.readUnsignedByte();
                keep = true;
                if (status != KuttiDBProtocol.ST_OK) throw new KuttiDBException("putManyTTL failed");
            } finally {
                if (keep) release(cn);
                else kill(cn);
            }
        }
    }

    /** Batched get: one round trip per {@link #BATCH_SIZE} keys; misses are {@code null}. */
    public byte[][] getMany(List<String> keys) throws IOException {
        byte[][] result = new byte[keys.size()][];
        for (int start = 0; start < keys.size(); start += BATCH_SIZE) {
            List<String> chunk = keys.subList(start, Math.min(start + BATCH_SIZE, keys.size()));
            long size = 7;
            for (String k : chunk) {
                byte[] kb = KuttiDBProtocol.keyBytes(k);
                size += 2L + kb.length;
            }
            byte[] head = KuttiDBProtocol.concat(new byte[]{(byte) KuttiDBProtocol.OP_GET_BATCH},
                    new byte[2], KuttiDBProtocol.u32(chunk.size()));
            List<byte[]> parts = new ArrayList<>();
            parts.add(head);
            for (String k : chunk) {
                byte[] kb = KuttiDBProtocol.keyBytes(k);
                parts.add(KuttiDBProtocol.u16(kb.length));
                parts.add(kb);
            }
            byte[] frame = KuttiDBProtocol.concat(parts.toArray(new byte[0][]));
            Conn cn = acquire();
            boolean keep = false;
            try {
                cn.writeFully(frame);
                byte[] cnt = new byte[4];
                cn.in.readFully(cnt);
                long n = KuttiDBProtocol.u32At(cnt, 0);
                if (n > chunk.size()) throw new KuttiDBException("malformed getMany response");
                for (int i = 0; i < n; i++) {
                    KuttiDBProtocol.Reply item = cn.readReply();
                    if (item.status == KuttiDBProtocol.ST_ERR) {
                        throw new KuttiDBException("getMany failed");
                    }
                    if (item.status == KuttiDBProtocol.ST_OK) {
                        result[start + i] = item.value;
                    }
                }
                keep = true;
            } finally {
                if (keep) release(cn);
                else kill(cn);
            }
        }
        return result;
    }

    // ======================================================================
    // Managed local lifecycle
    // ======================================================================

    /** Builder-style immutable settings for the opt-in local lifecycle. */
    public static final class ManagedServerOptions {
        public final Path dataDir;
        public final Path executable;
        public final long idleTimeoutMillis;
        public final long startupTimeoutMillis;
        public final byte[] authToken;
        public final String transport;
        public final String host;
        public final int port;
        public final int poolSize;

        public ManagedServerOptions(Path dataDir, Path executable, long idleTimeoutMillis,
                                    long startupTimeoutMillis, byte[] authToken) {
            this(dataDir, executable, idleTimeoutMillis, startupTimeoutMillis, authToken, "unix", "127.0.0.1", 7379, 0);
        }

        public ManagedServerOptions(Path dataDir, Path executable, long idleTimeoutMillis,
                                    long startupTimeoutMillis, byte[] authToken,
                                    String transport, String host, int port) {
            this(dataDir, executable, idleTimeoutMillis, startupTimeoutMillis, authToken, transport, host, port, 0);
        }

        /** {@code poolSize} of 0 selects the default pool size. */
        public ManagedServerOptions(Path dataDir, Path executable, long idleTimeoutMillis,
                                    long startupTimeoutMillis, byte[] authToken,
                                    String transport, String host, int port, int poolSize) {
            this.dataDir = dataDir.toAbsolutePath();
            this.executable = executable;
            this.idleTimeoutMillis = idleTimeoutMillis <= 0 ? 60000 : idleTimeoutMillis;
            this.startupTimeoutMillis = startupTimeoutMillis <= 0 ? 10000 : startupTimeoutMillis;
            this.authToken = authToken;
            this.transport = transport == null ? "unix" : transport;
            this.host = host == null ? "127.0.0.1" : host;
            this.port = port == 0 ? 7379 : port;
            this.poolSize = poolSize;
        }
    }

    /** Ensure, connect, and prove the identity of a local Unix or loopback-TCP instance. */
    public static KuttiDBClient connectManaged(ManagedServerOptions options) throws IOException {
        boolean tcp = "tcp".equals(options.transport);
        if (!tcp && !"unix".equals(options.transport)) {
            throw new KuttiDBException("managed transport must be unix or tcp");
        }
        if (tcp && (!literalLoopbackV4(options.host) || options.port < 1 || options.port > 65535)) {
            throw new KuttiDBException("managed TCP requires a literal IPv4 loopback endpoint");
        }
        Path socketPath = options.dataDir.resolve("kuttidb.sock");
        String endpoint = tcp ? "tcp:" + options.host + ":" + options.port : "unix:" + socketPath;
        String expected = null;
        Path idPath = options.dataDir.resolve("instance.id");
        if (Files.exists(idPath)) expected = Files.readString(idPath).trim();
        boolean absent = !tcp && Files.notExists(socketPath);
        if (!absent) {
            if (tcp) {
                try (Socket probe = new Socket()) {
                    probe.connect(new InetSocketAddress(options.host, options.port), 250);
                } catch (ConnectException refused) {
                    absent = true;
                }
            } else {
                try (SocketChannel probe = SocketChannel.open(UnixDomainSocketAddress.of(socketPath))) {
                    // reachable
                } catch (ConnectException refused) {
                    absent = true;
                }
            }
        }
        if (absent) {
            String executable = options.executable != null ? options.executable.toString()
                    : System.getenv().getOrDefault("KUTTIDB_SERVER", "kuttidb");
            Process process = new ProcessBuilder(executable, "ensure",
                    "--data-dir", options.dataDir.toString(),
                    "--listen", endpoint,
                    "--idle-timeout-ms", Long.toString(options.idleTimeoutMillis),
                    "--startup-timeout-ms", Long.toString(options.startupTimeoutMillis),
                    "--json")
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start();
            try {
                if (!process.waitFor(options.startupTimeoutMillis + 1000, TimeUnit.MILLISECONDS)
                        || process.exitValue() != 0) {
                    throw new KuttiDBException("managed server startup failed");
                }
            } catch (InterruptedException e) {
                process.destroyForcibly();
                Thread.currentThread().interrupt();
                throw new KuttiDBException("managed startup interrupted", e);
            }
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int at = output.indexOf("\"instance_id\":\"");
            if (at < 0 || output.length() < at + 47) {
                throw new KuttiDBException("invalid managed launcher response");
            }
            expected = output.substring(at + 15, at + 47);
        }
        if (expected == null || !expected.matches("[0-9a-f]{32}")) {
            throw new KuttiDBException("managed instance identity unavailable");
        }
        KuttiDBClient client = tcp
                ? new KuttiDBClient(options.host, options.port, options.authToken, null, options.poolSize)
                : new KuttiDBClient(null, 0, socketPath.toString(), options.authToken, null, options.poolSize);
        try {
            client.verifyManaged(expected);
        } catch (IOException e) {
            try { client.close(); } catch (IOException ignored) {}
            throw e;
        }
        return client;
    }

    private static boolean literalLoopbackV4(String host) {
        if (host == null) return false;
        String[] octets = host.split("\\.", -1);
        if (octets.length != 4) return false;
        for (int i = 0; i < octets.length; i++) {
            if (octets[i].isEmpty() || octets[i].length() > 3) return false;
            int value = 0;
            for (int j = 0; j < octets[i].length(); j++) {
                char c = octets[i].charAt(j);
                if (c < '0' || c > '9') return false;
                value = value * 10 + (c - '0');
            }
            if (value > 255 || (i == 0 && value != 127)) return false;
        }
        return true;
    }

    /** SERVER_INFO identity check against the expected managed instance id. */
    private void verifyManaged(String expected) throws IOException {
        Conn cn = acquire();
        boolean keep = false;
        try {
            cn.writeFully(KuttiDBProtocol.frame(KuttiDBProtocol.OP_SERVER_INFO, new byte[0], new byte[0]));
            KuttiDBProtocol.Reply r = cn.readReply();
            if (!r.ok() || r.value.length != 52) {
                throw new KuttiDBException("managed server identity unavailable");
            }
            byte[] value = r.value;
            if (value[0] != 1 || value[1] != 32
                    || !new String(value, 2, 32, StandardCharsets.US_ASCII).equals(expected)) {
                throw new KuttiDBException("managed endpoint belongs to another instance");
            }
            keep = true;
        } finally {
            if (keep) release(cn);
            else kill(cn);
        }
    }

    // ======================================================================
    // Messaging, atomic, single-flight, and streams (see feature classes)
    // ======================================================================

    /** Declare a queue with durability, depth bound, and dead-letter options. */
    public void queueDeclare(String name, QueueOptions options) throws IOException {
        KuttiDBQueues.declare(this, name, options);
    }

    /** Snapshot of every queue: name, depth, and in-flight deliveries. */
    public List<QueueInfo> queueList() throws IOException {
        return KuttiDBQueues.list(this);
    }

    /** Publish one message; returns the message id. */
    public long queuePublish(String name, byte[] value) throws IOException {
        return KuttiDBQueues.publish(this, name, value, null);
    }

    /** Publish one message with per-message time-to-live in milliseconds. */
    public long queuePublish(String name, byte[] value, long ttlMillis) throws IOException {
        return KuttiDBQueues.publish(this, name, value, ttlMillis);
    }

    /** Consume one delivery with a visibility timeout; {@code null} when empty. */
    public QueueDelivery queueConsume(String name, long visibilityMillis) throws IOException {
        return KuttiDBQueues.consume(this, name, null, visibilityMillis);
    }

    /** Consume as a durable named consumer; {@code null} when empty. */
    public QueueDelivery queueConsumeAs(String name, String consumer, long visibilityMillis) throws IOException {
        return KuttiDBQueues.consume(this, name, consumer, visibilityMillis);
    }

    /** ACK one delivery; {@code false} when the tag is unknown. */
    public boolean queueAck(String name, long deliveryTag) throws IOException {
        return KuttiDBQueues.ack(this, name, deliveryTag);
    }

    /** NACK one delivery: requeue or dead-letter, with an optional delay. */
    public boolean queueNack(String name, long deliveryTag, boolean requeue, long delayMillis) throws IOException {
        return KuttiDBQueues.nack(this, name, deliveryTag, requeue, delayMillis);
    }

    /** Queue depth/inflight, or {@code null} when the queue does not exist. */
    public QueueStats queueStats(String name) throws IOException {
        return KuttiDBQueues.stats(this, name);
    }

    /** Set this connection's unacknowledged-delivery prefetch bound. */
    public void queuePrefetch(int count) throws IOException {
        KuttiDBQueues.prefetch(this, count);
    }

    /** Cancel this connection's consumer registration and prefetch state. */
    public void queueCancel() throws IOException {
        KuttiDBQueues.cancel(this);
    }

    /** Register a durable named consumer; returns its lease id. */
    public long queueConsumerRegister(String name) throws IOException {
        return KuttiDBQueues.consumerRegister(this, name);
    }

    public void queueConsumerUnregister(String name) throws IOException {
        KuttiDBQueues.consumerUnregister(this, name);
    }

    /** Publish 1..256 messages in one round trip; returns the message ids. */
    public long[] queuePublishBatch(String name, List<byte[]> values) throws IOException {
        return KuttiDBQueues.publishBatch(this, name, values);
    }

    /** Consume up to 1..256 deliveries in one round trip. */
    public List<QueueDelivery> queueConsumeBatch(String name, int maxCount) throws IOException {
        return KuttiDBQueues.consumeBatch(this, name, maxCount);
    }

    /** ACK 1..256 delivery tags in one round trip; returns the applied count. */
    public int queueAckBatch(String name, long[] deliveryTags) throws IOException {
        return KuttiDBQueues.dispositionBatch(this, name, deliveryTags, (byte) 0);
    }

    /** NACK 1..256 delivery tags in one round trip; returns the applied count. */
    public int queueNackBatch(String name, long[] deliveryTags, boolean requeue) throws IOException {
        return KuttiDBQueues.dispositionBatch(this, name, deliveryTags, (byte) (requeue ? 1 : 2));
    }

    /** Declare an exchange with type, durability, and alternate-exchange routing. */
    public void exchangeDeclare(String name, ExchangeOptions options) throws IOException {
        KuttiDBExchanges.declare(this, name, options);
    }

    /** Bind a queue to an exchange under a routing key pattern. */
    public void exchangeBind(String exchange, String queue, String routingKey) throws IOException {
        KuttiDBExchanges.bind(this, exchange, queue, routingKey);
    }

    /** Remove a binding; {@code false} when it did not exist. */
    public boolean exchangeUnbind(String exchange, String queue, String routingKey) throws IOException {
        return KuttiDBExchanges.unbind(this, exchange, queue, routingKey);
    }

    /** Publish through an exchange; returns routed copies (0 = unroutable). */
    public int exchangePublish(String exchange, String routingKey, byte[] value) throws IOException {
        return KuttiDBExchanges.publish(this, exchange, routingKey, value, 0);
    }

    /** Publish through an exchange with per-message TTL; returns routed copies. */
    public int exchangePublish(String exchange, String routingKey, byte[] value, long ttlMillis) throws IOException {
        return KuttiDBExchanges.publish(this, exchange, routingKey, value, ttlMillis);
    }

    /** Commit a cache put plus an exchange publish under one durable commit id. */
    public AtomicResult putAndPublish(String key, byte[] value, String exchange, String routingKey,
                                      long ttlMillis) throws IOException {
        return KuttiDBExchanges.atomicExchange(KuttiDBProtocol.OP_ATOMIC_PUT_PUBLISH, this, key, value,
                exchange, routingKey, ttlMillis);
    }

    /** Commit a cache put plus a direct queue enqueue under one durable commit id. */
    public AtomicResult putAndEnqueue(String key, byte[] value, String queue, long ttlMillis) throws IOException {
        return KuttiDBExchanges.atomicEnqueue(this, key, value, queue, ttlMillis);
    }

    /** Commit a cache delete plus an exchange publish under one durable commit id. */
    public AtomicResult deleteAndPublish(String key, String exchange, String routingKey,
                                         byte[] message) throws IOException {
        return KuttiDBExchanges.atomicDeletePublish(this, key, exchange, routingKey, message);
    }

    /** Update an existing key and emit an event; MISS commits nothing. */
    public AtomicResult updateAndEmit(String key, byte[] value, String exchange, String routingKey,
                                      long ttlMillis) throws IOException {
        return KuttiDBExchanges.atomicExchange(KuttiDBProtocol.OP_ATOMIC_UPDATE_EMIT, this, key, value,
                exchange, routingKey, ttlMillis);
    }

    /** Claim a missing key for loading; see docs/design/PROTOCOL.md single-flight. */
    public SingleFlightResult getOrClaim(String key, long leaseMillis) throws IOException {
        return KuttiDBStreams.getOrClaim(this, key, leaseMillis);
    }

    /** Wait for another connection's load to finish (server-side deadline). */
    public SingleFlightResult waitForKey(String key, long timeoutMillis) throws IOException {
        return KuttiDBStreams.waitForKey(this, key, timeoutMillis);
    }

    /** Store the loaded value and wake this key's waiters. */
    public void putAndRelease(String key, byte[] value, long ttlMillis, boolean negative) throws IOException {
        KuttiDBStreams.putAndRelease(this, key, value, ttlMillis, negative);
    }

    /** Abandon a claim; waiters receive the released state. */
    public void releaseClaim(String key) throws IOException {
        KuttiDBStreams.releaseClaim(this, key);
    }

    /** SWR-aware read: fresh, stale, refresh-ahead, or the claim/wait machinery. */
    public SingleFlightResult getOrRefresh(String key, long leaseMillis) throws IOException {
        return KuttiDBStreams.getOrRefresh(this, key, leaseMillis);
    }

    /** Store a value with stale-while-revalidate windows in milliseconds. */
    public void putSWR(String key, byte[] value, long ttlMillis, long staleMillis,
                       long refreshMillis) throws IOException {
        KuttiDBStreams.putSWR(this, key, value, ttlMillis, staleMillis, refreshMillis);
    }

    /** Single-flight loader: one concurrent load per missing key. */
    public byte[] getOrLoad(String key, Loader loader, long ttlMillis, long leaseMillis,
                            long waitMillis) throws IOException {
        return KuttiDBStreams.getOrLoad(this, key, loader, ttlMillis, leaseMillis, waitMillis);
    }

    /** SWR loader: stale reads while one holder revalidates. */
    public byte[] getOrLoadSWR(String key, Loader loader, long ttlMillis, long staleMillis,
                               long refreshMillis, long leaseMillis, long waitMillis) throws IOException {
        return KuttiDBStreams.getOrLoadSWR(this, key, loader, ttlMillis, staleMillis,
                refreshMillis, leaseMillis, waitMillis);
    }

    /** Declare a partitioned stream topic. */
    public void streamDeclare(String topic, StreamOptions options) throws IOException {
        KuttiDBStreams.declare(this, topic, options);
    }

    /** Inventory of declared streams. */
    public List<StreamInfo> streamList() throws IOException {
        return KuttiDBStreams.list(this);
    }

    /** Inventory of consumer groups. */
    public List<StreamGroupInfo> streamGroupList() throws IOException {
        return KuttiDBStreams.groupList(this);
    }

    /**
     * Append one record. A {@code null} key appends an unkeyed record and a
     * {@code null} partition lets the server choose.
     */
    public StreamPosition streamAppend(String topic, byte[] value, byte[] key,
                                       Integer partition) throws IOException {
        return KuttiDBStreams.append(this, topic, value, key, partition);
    }

    /** Append 1..1024 records in one round trip. */
    public List<StreamPosition> streamAppendBatch(String topic, List<StreamAppend> items,
                                                  Integer partition) throws IOException {
        return KuttiDBStreams.appendBatch(this, topic, items, partition);
    }

    /** Replay up to 1..1024 records; returns records with keys when supported. */
    public List<StreamRecord> streamFetch(String topic, int partition, long offset,
                                          int maxRecords) throws IOException {
        return KuttiDBStreams.fetch(this, topic, partition, offset, maxRecords);
    }

    /** Commit one consumed offset for a consumer group. */
    public void streamCommit(String topic, String group, int partition, long offset) throws IOException {
        KuttiDBStreams.commit(this, topic, group, partition, offset);
    }

    /** Commit offsets for 1..256 partitions in one round trip. */
    public void streamCommitBatch(String topic, String group, List<StreamCommit> commits) throws IOException {
        KuttiDBStreams.commitBatch(this, topic, group, commits);
    }

    /** Committed offset, or {@code null} when the group has not consumed. */
    public Long streamGroupOffset(String topic, String group, int partition) throws IOException {
        return KuttiDBStreams.groupValue(this, KuttiDBProtocol.OP_STREAM_GROUP_OFFSET, topic, group, partition);
    }

    /** Committed-offset lag, or {@code null} when the group does not exist. */
    public Long streamGroupLag(String topic, String group, int partition) throws IOException {
        return KuttiDBStreams.groupValue(this, KuttiDBProtocol.OP_STREAM_GROUP_LAG, topic, group, partition);
    }

    /** Join (or heartbeat) a consumer group; returns the partition assignment. */
    public StreamAssignment streamGroupJoin(String topic, String group, long leaseMillis) throws IOException {
        return KuttiDBStreams.groupJoin(this, topic, group, leaseMillis);
    }

    /** Leave a consumer group gracefully. */
    public void streamGroupLeave(String topic, String group) throws IOException {
        KuttiDBStreams.groupLeave(this, topic, group);
    }

    // ======================================================================
    // Lifecycle
    // ======================================================================

    @Override
    public void close() throws IOException {
        List<Conn> toClose = new ArrayList<>();
        synchronized (lock) {
            closed = true;
            toClose.addAll(idle);
            idle.clear();
        }
        synchronized (stateLock) {
            if (stateConn != null) {
                toClose.add(stateConn);
                stateConn = null;
            }
        }
        closeAll(toClose);
    }
}
