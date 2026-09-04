import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * Java client for the KuttiDB binary protocol.
 *
 * Not thread-safe: use one instance per thread. Batched methods
 * (putMany/getMany) send up to BATCH_SIZE operations per round trip.
 */
public class KuttiDBClient implements AutoCloseable {

    public static final int BATCH_SIZE = 256;
    private static final int MAX_KEY = (1 << 16) - 1;
    private static final int MAX_VALUE = 64 << 20;

    private static final byte OP_PUT = 0x01, OP_GET = 0x02, OP_DELETE = 0x03,
            OP_STATS = 0x04, OP_PUT_TTL = 0x05, OP_AUTH = 0x06, OP_PUT_BATCH = 0x11,
            OP_GET_BATCH = 0x12, OP_PUT_BATCH_TTL = 0x13;

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
    private static final byte ST_OK = 0x00, ST_MISS = 0x01;

    private final java.io.Closeable sock;
    private final OutputStream out;
    private final DataInputStream in;
    private final ByteBuffer lenBuf = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);

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

    public KuttiDBClient(String host, int port) throws IOException {
        this(host, port, null);
    }

    /** Connect and authenticate with a 1..1024 byte pre-shared token. */
    public KuttiDBClient(String host, int port, byte[] authToken) throws IOException {
        this(host, port, authToken, null);
    }

    /** Connect over TLS with hostname verification, then optionally AUTH. */
    public KuttiDBClient(String host, int port, byte[] authToken, SSLContext tlsContext)
            throws IOException {
        if (tlsContext == null) {
            this.sock = new Socket(host, port);
        } else {
            SSLSocket tlsSock = (SSLSocket) tlsContext.getSocketFactory().createSocket();
            SSLParameters params = tlsSock.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            tlsSock.setSSLParameters(params);
            tlsSock.connect(new InetSocketAddress(host, port));
            tlsSock.startHandshake();
            this.sock = tlsSock;
        }
        ((Socket) sock).setTcpNoDelay(true);
        this.out = ((Socket) sock).getOutputStream();
        this.in = new DataInputStream(((Socket) sock).getInputStream());
        if (authToken != null) {
            if (authToken.length == 0 || authToken.length > 1024) {
                sock.close();
                throw new IOException("auth token must contain 1..1024 bytes");
            }
            out.write(singleHeader(OP_AUTH, authToken, 0));
            int[] head = readResponseHead();
            if (head[0] != ST_OK) {
                sock.close();
                throw new IOException("authentication failed");
            }
        }
    }

    /** Builder-style immutable settings for the opt-in local lifecycle. */
    public static final class ManagedServerOptions {
        public final Path dataDir; public final Path executable; public final long idleTimeoutMillis; public final long startupTimeoutMillis; public final byte[] authToken;
        public final String transport; public final String host; public final int port;
        public ManagedServerOptions(Path dataDir, Path executable, long idleTimeoutMillis, long startupTimeoutMillis, byte[] authToken) { this(dataDir, executable, idleTimeoutMillis, startupTimeoutMillis, authToken, "unix", "127.0.0.1", 7379); }
        public ManagedServerOptions(Path dataDir, Path executable, long idleTimeoutMillis, long startupTimeoutMillis, byte[] authToken, String transport, String host, int port) {
            this.dataDir=dataDir.toAbsolutePath(); this.executable=executable; this.idleTimeoutMillis=idleTimeoutMillis <= 0 ? 60000 : idleTimeoutMillis; this.startupTimeoutMillis=startupTimeoutMillis <= 0 ? 10000 : startupTimeoutMillis; this.authToken=authToken;
            this.transport=transport == null ? "unix" : transport; this.host=host == null ? "127.0.0.1" : host; this.port=port == 0 ? 7379 : port;
        }
    }

    /** Ensure, connect, and prove the identity of a local Unix or loopback-TCP instance. */
    public static KuttiDBClient connectManaged(ManagedServerOptions options) throws IOException {
        boolean tcp = "tcp".equals(options.transport);
        if (!tcp && !"unix".equals(options.transport)) throw new IOException("managed transport must be unix or tcp");
        if (tcp && (!literalLoopbackV4(options.host) || options.port < 1 || options.port > 65535))
            throw new IOException("managed TCP requires a literal IPv4 loopback endpoint");
        Path socketPath = options.dataDir.resolve("kuttidb.sock");
        String endpoint = tcp ? "tcp:" + options.host + ":" + options.port : "unix:" + socketPath;
        String expected = null;
        Path idPath = options.dataDir.resolve("instance.id");
        if (Files.exists(idPath)) expected = Files.readString(idPath).trim();
        boolean absent = !tcp && Files.notExists(socketPath);
        if (!absent) {
            if (tcp) {
                try (Socket probe = new Socket()) { probe.connect(new InetSocketAddress(options.host, options.port), 250); }
                catch (ConnectException refused) { absent = true; }
            } else {
                try (SocketChannel probe = SocketChannel.open(UnixDomainSocketAddress.of(socketPath))) { }
                catch (ConnectException refused) { absent = true; }
            }
        }
        if (absent) {
            String executable = options.executable != null ? options.executable.toString() : System.getenv().getOrDefault("KUTTIDB_SERVER", "kuttidb");
            Process process = new ProcessBuilder(executable, "ensure", "--data-dir", options.dataDir.toString(), "--listen", endpoint, "--idle-timeout-ms", Long.toString(options.idleTimeoutMillis), "--startup-timeout-ms", Long.toString(options.startupTimeoutMillis), "--json").redirectError(ProcessBuilder.Redirect.DISCARD).start();
            try { if (!process.waitFor(options.startupTimeoutMillis + 1000, java.util.concurrent.TimeUnit.MILLISECONDS) || process.exitValue() != 0) throw new IOException("managed server startup failed"); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("managed startup interrupted", e); }
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int at = output.indexOf("\"instance_id\":\""); if (at < 0 || output.length() < at + 47) throw new IOException("invalid managed launcher response"); expected = output.substring(at + 15, at + 47);
        }
        if (expected == null || !expected.matches("[0-9a-f]{32}")) throw new IOException("managed instance identity unavailable");
        KuttiDBClient client;
        if (tcp) client = new KuttiDBClient(options.host, options.port, options.authToken);
        else {
            SocketChannel channel = SocketChannel.open(UnixDomainSocketAddress.of(socketPath));
            client = new KuttiDBClient(channel, Channels.newOutputStream(channel), new DataInputStream(Channels.newInputStream(channel)), options.authToken);
        }
        client.verifyManaged(expected); return client;
    }

    private KuttiDBClient(java.io.Closeable socket, OutputStream output, DataInputStream input, byte[] authToken) throws IOException { this.sock=socket; this.out=output; this.in=input; if (authToken != null) { out.write(singleHeader(OP_AUTH, authToken, 0)); if (readResponseHead()[0] != ST_OK) throw new IOException("authentication failed"); } }
    private void verifyManaged(String expected) throws IOException { out.write(new byte[]{0x0c,0,0,0,0,0,0}); int[] h=readResponseHead(); if (h[0] != ST_OK || h[1] != 52) throw new IOException("managed identity unavailable"); byte[] value=new byte[52]; in.readFully(value); if (value[0] != 1 || value[1] != 32 || !new String(value,2,32,StandardCharsets.US_ASCII).equals(expected)) throw new IOException("managed endpoint belongs to another instance"); }

    private byte[] u16(int v) {
        return new byte[]{(byte) v, (byte) (v >>> 8)};
    }

    private byte[] u32(long v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }

    private byte[] singleHeader(int op, byte[] key, int vlen) throws IOException {
        if (key.length > MAX_KEY) throw new IOException("key too large");
        if (vlen < 0 || vlen > MAX_VALUE) throw new IOException("value too large");
        byte[] req = new byte[7 + key.length + vlen];
        req[0] = (byte) op;
        req[1] = (byte) key.length;
        req[2] = (byte) (key.length >>> 8);
        req[3] = (byte) vlen;
        req[4] = (byte) (vlen >>> 8);
        req[5] = (byte) (vlen >>> 16);
        req[6] = (byte) (vlen >>> 24);
        System.arraycopy(key, 0, req, 7, key.length);
        return req;
    }

    private int[] readResponseHead() throws IOException {
        byte[] head = new byte[5];
        in.readFully(head);
        ByteBuffer bb = ByteBuffer.wrap(head).order(ByteOrder.LITTLE_ENDIAN);
        int vlen = bb.getInt(1);
        if (vlen < 0 || vlen > MAX_VALUE) throw new IOException("response too large");
        return new int[]{bb.get(), vlen};
    }

    private byte[] singleHeaderTTL(int op, byte[] key, int vlen, long ttlMs) throws IOException {
        if (key.length > MAX_KEY) throw new IOException("key too large");
        if (vlen < 0 || vlen > MAX_VALUE) throw new IOException("value too large");
        byte[] req = new byte[11 + key.length + vlen];
        req[0] = (byte) op;
        req[1] = (byte) key.length;
        req[2] = (byte) (key.length >>> 8);
        req[3] = (byte) vlen;
        req[4] = (byte) (vlen >>> 8);
        req[5] = (byte) (vlen >>> 16);
        req[6] = (byte) (vlen >>> 24);
        req[7] = (byte) ttlMs;
        req[8] = (byte) (ttlMs >>> 8);
        req[9] = (byte) (ttlMs >>> 16);
        req[10] = (byte) (ttlMs >>> 24);
        System.arraycopy(key, 0, req, 11, key.length);
        return req;
    }

    /** Put with a time-to-live in milliseconds. */
    public void put(String key, byte[] value, long ttlMillis) throws IOException {
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        byte[] req = singleHeaderTTL(OP_PUT_TTL, kb, value.length, ttlMillis);
        System.arraycopy(value, 0, req, 11 + kb.length, value.length);
        out.write(req);
        int[] head = readResponseHead();
        if (head[0] != ST_OK) throw new IOException("server error");
    }

    public void put(String key, byte[] value) throws IOException {
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        byte[] req = singleHeader(OP_PUT, kb, value.length);
        System.arraycopy(value, 0, req, 7 + kb.length, value.length);
        out.write(req);
        int[] head = readResponseHead();
        if (head[0] != ST_OK) throw new IOException("server error");
    }

    public byte[] get(String key) throws IOException {
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        out.write(singleHeader(OP_GET, kb, 0));
        int[] head = readResponseHead();
        int vlen = head[1];
        if (head[0] == ST_MISS) return null;
        if (head[0] != ST_OK) throw new IOException("server error");
        byte[] val = new byte[vlen];
        in.readFully(val);
        return val;
    }

    public boolean delete(String key) throws IOException {
        byte[] kb = key.getBytes(StandardCharsets.UTF_8);
        out.write(singleHeader(OP_DELETE, kb, 0));
        int[] head = readResponseHead();
        if (head[0] != ST_OK && head[0] != ST_MISS) throw new IOException("server error");
        return head[0] == ST_OK;
    }

    public String stats() throws IOException {
        out.write(singleHeader(OP_STATS, new byte[0], 0));
        int[] head = readResponseHead();
        byte[] val = new byte[head[1]];
        in.readFully(val);
        return new String(val, StandardCharsets.UTF_8);
    }

    /** Batched put: one round trip per BATCH_SIZE pairs. */
    public void putMany(Map<String, byte[]> pairs) throws IOException {
        List<String> keys = new ArrayList<>(pairs.keySet());
        for (int start = 0; start < keys.size(); start += BATCH_SIZE) {
            List<String> chunk = keys.subList(start, Math.min(start + BATCH_SIZE, keys.size()));
            long size = 7;
            for (String k : chunk) {
                byte[] kb = k.getBytes(StandardCharsets.UTF_8);
                byte[] value = pairs.get(k);
                if (kb.length > MAX_KEY) throw new IOException("key too large");
                if (value.length > MAX_VALUE) throw new IOException("value too large");
                size += 6L + kb.length + value.length;
                if (size > MAX_VALUE) throw new IOException("batch too large");
            }
            ByteBuffer req = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
            req.put(OP_PUT_BATCH).putShort((short) 0).putInt(chunk.size());
            for (String k : chunk) {
                byte[] kb = k.getBytes(StandardCharsets.UTF_8);
                byte[] v = pairs.get(k);
                req.putShort((short) kb.length).putInt(v.length).put(kb).put(v);
            }
            out.write(req.array());
            int status = in.readUnsignedByte();
            if (status != ST_OK) throw new IOException("server error in putMany");
        }
    }

    /** Batched put with per-item TTL: one round trip per BATCH_SIZE items. */
    public void putManyTTL(List<Item> items) throws IOException {
        for (int start = 0; start < items.size(); start += BATCH_SIZE) {
            List<Item> chunk = items.subList(start, Math.min(start + BATCH_SIZE, items.size()));
            long size = 7;
            for (Item it : chunk) {
                byte[] kb = it.key.getBytes(StandardCharsets.UTF_8);
                if (kb.length > MAX_KEY) throw new IOException("key too large");
                if (it.value.length > MAX_VALUE) throw new IOException("value too large");
                size += 10L + kb.length + it.value.length;
                if (size > MAX_VALUE) throw new IOException("batch too large");
            }
            ByteBuffer req = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
            req.put(OP_PUT_BATCH_TTL).putShort((short) 0).putInt(chunk.size());
            for (Item it : chunk) {
                byte[] kb = it.key.getBytes(StandardCharsets.UTF_8);
                req.putShort((short) kb.length).putInt(it.value.length)
                   .putInt((int) it.ttlMillis).put(kb).put(it.value);
            }
            out.write(req.array());
            int status = in.readUnsignedByte();
            if (status != ST_OK) throw new IOException("server error in putManyTTL");
        }
    }

    /** Batched get: one round trip per BATCH_SIZE keys; misses are null. */
    public byte[][] getMany(List<String> keys) throws IOException {
        byte[][] result = new byte[keys.size()][];
        for (int start = 0; start < keys.size(); start += BATCH_SIZE) {
            List<String> chunk = keys.subList(start, Math.min(start + BATCH_SIZE, keys.size()));
            long size = 7;
            for (String k : chunk) {
                byte[] kb = k.getBytes(StandardCharsets.UTF_8);
                if (kb.length > MAX_KEY) throw new IOException("key too large");
                size += 2L + kb.length;
            }
            ByteBuffer req = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
            req.put(OP_GET_BATCH).putShort((short) 0).putInt(chunk.size());
            for (String k : chunk) {
                byte[] kb = k.getBytes(StandardCharsets.UTF_8);
                req.putShort((short) kb.length).put(kb);
            }
            out.write(req.array());
            byte[] cnt = new byte[4];
            in.readFully(cnt);
            int n = ByteBuffer.wrap(cnt).order(ByteOrder.LITTLE_ENDIAN).getInt();
            for (int i = 0; i < n; i++) {
                byte[] sh = new byte[5];
                in.readFully(sh);
                ByteBuffer shb = ByteBuffer.wrap(sh).order(ByteOrder.LITTLE_ENDIAN);
                int status = shb.get();
                int vlen = shb.getInt(1);
                if (vlen < 0 || vlen > MAX_VALUE) throw new IOException("response too large");
                if (status == ST_OK && vlen > 0) {
                    byte[] val = new byte[vlen];
                    in.readFully(val);
                    result[start + i] = val;
                }
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        sock.close();
    }
}
