package io.github.kuttidb.client;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * One stable logical completion: caller-owned operation id plus the full
 * semantic request. Serializing this object before submission is the
 * supported recovery path for lost responses; retries must reuse the same id
 * and the same fields. JSON encoding is lossless: 64-bit identity and version
 * fields are decimal strings (safe beyond 2^53), byte spans are Base64. The
 * ephemeral delivery proof is deliberately not serialized.
 *
 * <p>Immutable. Compose with {@link #builder(UUID)} or from a delivery via
 * {@link JobDelivery#toIntent()}; the delivery helper generates the operation
 * id once at composition, and a built intent keeps it for every retry.
 */
public final class JobCompletionIntent {

    /** Compact-string format tag. */
    private static final String FORMAT = "KJC1";

    private final UUID operationId;
    private final String queue;
    private final long queueIncarnation;
    private final long messageId;
    private final byte[] stateKey;
    private final long expectedVersion;
    private final byte[] stateValue;
    /** {@code null} when the completion publishes no output message. */
    private final String outputQueue;
    private final long outputIncarnation;
    private final byte[] outputValue;

    private JobCompletionIntent(Builder b) {
        this.operationId = b.operationId;
        this.queue = b.queue;
        this.queueIncarnation = b.queueIncarnation;
        this.messageId = b.messageId;
        this.stateKey = b.stateKey;
        this.expectedVersion = b.expectedVersion;
        this.stateValue = b.stateValue;
        this.outputQueue = b.outputQueue;
        this.outputIncarnation = b.outputIncarnation;
        this.outputValue = b.outputValue;
    }

    /**
     * A builder that generates a fresh operation id now — once, at
     * composition. Every {@link #build()} on this builder reuses that id.
     */
    public static Builder builder() {
        return new Builder(UUID.randomUUID());
    }

    /** A builder pinned to an existing operation id (the exact-retry path). */
    public static Builder builder(UUID operationId) {
        return new Builder(operationId);
    }

    public UUID operationId() {
        return operationId;
    }

    public String queue() {
        return queue;
    }

    public long queueIncarnation() {
        return queueIncarnation;
    }

    public long messageId() {
        return messageId;
    }

    public byte[] stateKey() {
        return stateKey;
    }

    public long expectedVersion() {
        return expectedVersion;
    }

    public byte[] stateValue() {
        return stateValue;
    }

    /** {@code null} when the completion publishes no output message. */
    public String outputQueue() {
        return outputQueue;
    }

    public long outputIncarnation() {
        return outputIncarnation;
    }

    public byte[] outputValue() {
        return outputValue;
    }

    // ---- lossless serialization --------------------------------------------

    /**
     * Lossless JSON map form. 64-bit identity and version fields serialize as
     * decimal strings, byte spans as Base64; the delivery proof is excluded.
     */
    public Map<String, Object> toJSON() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("operation_id", operationId.toString());
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("queue", queue);
        input.put("queue_incarnation", Long.toString(queueIncarnation));
        input.put("message_id", Long.toString(messageId));
        out.put("input", input);
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("key", b64(stateKey));
        state.put("expected_version", Long.toString(expectedVersion));
        state.put("value", b64(stateValue));
        out.put("state", state);
        if (outputQueue == null) {
            out.put("output", null);
        } else {
            Map<String, Object> output = new LinkedHashMap<>();
            output.put("queue", outputQueue);
            output.put("queue_incarnation", Long.toString(outputIncarnation));
            output.put("value", b64(outputValue));
            out.put("output", output);
        }
        return out;
    }

    /** Rebuild an intent from {@link #toJSON()} output. */
    @SuppressWarnings("unchecked")
    public static JobCompletionIntent fromJSON(Map<String, Object> data) {
        Objects.requireNonNull(data, "data");
        Object output = data.get("output");
        Map<String, Object> outputMap = (Map<String, Object>) output;
        Map<String, Object> input = requireMap(data, "input");
        Map<String, Object> state = requireMap(data, "state");
        return new Builder(uuid(requireString(data, "operation_id")))
                .queue(requireString(input, "queue"))
                .queueIncarnation(longValue(input.get("queue_incarnation")))
                .messageId(longValue(input.get("message_id")))
                .stateKeyBytes(b64d(requireString(state, "key")))
                .expectedVersion(longValue(state.get("expected_version")))
                .stateValue(b64d(state.get("value") == null ? "" : state.get("value").toString()))
                .outputQueue(outputMap == null ? null : requireString(outputMap, "queue"))
                .outputIncarnation(outputMap == null ? 0 : longValue(outputMap.get("queue_incarnation")))
                .outputValue(outputMap == null ? new byte[0] : b64d(outputMap.get("value") == null
                        ? "" : outputMap.get("value").toString()))
                .build();
    }

    /**
     * Compact lossless one-line form: pipe-separated fields with Base64
     * payloads, safe for logs and flat files. Round-trips through
     * {@link #fromCompactString(String)}.
     */
    public String toCompactString() {
        StringBuilder s = new StringBuilder(FORMAT);
        s.append('|').append(operationId).append('|').append(b64(utf8(queue)))
                .append('|').append(queueIncarnation).append('|').append(messageId)
                .append('|').append(b64(stateKey)).append('|').append(expectedVersion)
                .append('|').append(b64(stateValue));
        if (outputQueue != null) {
            s.append('|').append(b64(utf8(outputQueue))).append('|').append(outputIncarnation)
                    .append('|').append(b64(outputValue));
        }
        return s.toString();
    }

    /** Rebuild an intent from {@link #toCompactString()} output. */
    public static JobCompletionIntent fromCompactString(String text) {
        // KJC1|uuid|b64(queue)|incarnation|messageId|b64(key)|expectedVersion|b64(value)
        // [|b64(outQueue)|outIncarnation|b64(outValue)]
        String[] f = text.split("\\|", -1);
        if (f.length != 9 && f.length != 11) {
            throw new IllegalArgumentException("invalid intent encoding: " + text);
        }
        if (!FORMAT.equals(f[0])) {
            throw new IllegalArgumentException("unsupported intent format: " + f[0]);
        }
        Builder b = new Builder(UUID.fromString(f[1]))
                .queue(utf8Text(b64d(f[2])))
                .queueIncarnation(Long.parseLong(f[3]))
                .messageId(Long.parseLong(f[4]))
                .stateKeyBytes(b64d(f[5]))
                .expectedVersion(Long.parseLong(f[6]))
                .stateValue(b64d(f[7]));
        if (f.length == 9) return b.build();
        return b.outputQueue(utf8Text(b64d(f[8])))
                .outputIncarnation(Long.parseLong(f[9]))
                .outputValue(b64d(f[10]))
                .build();
    }

    // ---- Object contract ----------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobCompletionIntent)) return false;
        JobCompletionIntent other = (JobCompletionIntent) o;
        return operationId.equals(other.operationId)
                && queue.equals(other.queue)
                && queueIncarnation == other.queueIncarnation
                && messageId == other.messageId
                && Arrays.equals(stateKey, other.stateKey)
                && expectedVersion == other.expectedVersion
                && Arrays.equals(stateValue, other.stateValue)
                && Objects.equals(outputQueue, other.outputQueue)
                && outputIncarnation == other.outputIncarnation
                && Arrays.equals(outputValue, other.outputValue);
    }

    @Override
    public int hashCode() {
        int result = operationId.hashCode();
        result = 31 * result + queue.hashCode();
        result = 31 * result + Long.hashCode(queueIncarnation);
        result = 31 * result + Long.hashCode(messageId);
        result = 31 * result + Arrays.hashCode(stateKey);
        result = 31 * result + Long.hashCode(expectedVersion);
        result = 31 * result + Arrays.hashCode(stateValue);
        result = 31 * result + Objects.hashCode(outputQueue);
        result = 31 * result + Long.hashCode(outputIncarnation);
        result = 31 * result + Arrays.hashCode(outputValue);
        return result;
    }

    @Override
    public String toString() {
        return "JobCompletionIntent{operationId=" + operationId + ", queue=" + queue
                + ", queueIncarnation=" + queueIncarnation + ", messageId=" + messageId
                + ", stateKey=" + new String(stateKey, StandardCharsets.UTF_8)
                + ", expectedVersion=" + expectedVersion
                + ", stateValueLength=" + stateValue.length
                + ", outputQueue=" + outputQueue + ", outputIncarnation=" + outputIncarnation
                + ", outputValueLength=" + outputValue.length + "}";
    }

    /** Mutable composition step; {@link #build()} validates and freezes. */
    public static final class Builder {

        private final UUID operationId;
        private String queue;
        private long queueIncarnation;
        private long messageId;
        private byte[] stateKey = new byte[0];
        private long expectedVersion;
        private byte[] stateValue = new byte[0];
        private String outputQueue;
        private long outputIncarnation;
        private byte[] outputValue = new byte[0];

        private Builder(UUID operationId) {
            this.operationId = Objects.requireNonNull(operationId, "operationId");
        }

        public Builder queue(String v) { this.queue = v; return this; }
        public Builder queueIncarnation(long v) { this.queueIncarnation = v; return this; }
        public Builder messageId(long v) { this.messageId = v; return this; }
        public Builder stateKey(String v) { this.stateKey = utf8(v); return this; }
        public Builder stateKeyBytes(byte[] v) { this.stateKey = v == null ? new byte[0] : v.clone(); return this; }
        public Builder expectedVersion(long v) { this.expectedVersion = v; return this; }
        public Builder stateValue(byte[] v) { this.stateValue = v == null ? new byte[0] : v.clone(); return this; }
        public Builder outputQueue(String v) { this.outputQueue = v; return this; }
        public Builder outputIncarnation(long v) { this.outputIncarnation = v; return this; }
        public Builder outputValue(byte[] v) { this.outputValue = v == null ? new byte[0] : v.clone(); return this; }
        public JobCompletionIntent build() {
            if (queue == null || queue.isEmpty() || utf8(queue).length > 255) {
                throw new IllegalArgumentException("intent requires a valid input queue name");
            }
            if (stateKey.length == 0 || stateKey.length > 65535) {
                throw new IllegalArgumentException("intent requires a 1..65535 byte state key");
            }
            if (expectedVersion < 0) {
                throw new IllegalArgumentException("expectedVersion must be non-negative");
            }
            if (outputQueue != null) {
                if (outputQueue.isEmpty() || utf8(outputQueue).length > 255) {
                    throw new IllegalArgumentException("intent requires a valid output queue name");
                }
                if (outputIncarnation == 0) {
                    throw new IllegalArgumentException("output intent requires its queue incarnation");
                }
            }
            return new JobCompletionIntent(this);
        }
    }

    // ---- helpers ------------------------------------------------------------

    private static Map<String, Object> requireMap(Map<String, Object> data, String field) {
        Object v = data.get(field);
        if (!(v instanceof Map)) throw new IllegalArgumentException("intent field missing: " + field);
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) v;
        return map;
    }

    private static String requireString(Map<String, Object> data, String field) {
        Object v = data.get(field);
        if (!(v instanceof String)) throw new IllegalArgumentException("intent field missing: " + field);
        return (String) v;
    }

    /** 64-bit fields accept decimal strings (canonical) and integral numbers. */
    private static long longValue(Object v) {
        if (v instanceof Number) return ((Number) v).longValue();
        if (v instanceof String) return Long.parseLong(((String) v).trim());
        throw new IllegalArgumentException("intent 64-bit field must be a decimal string or number");
    }

    private static UUID uuid(String text) {
        return UUID.fromString(text);
    }

    private static byte[] utf8(String s) {
        return (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
    }

    private static String utf8Text(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }

    private static String b64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    private static byte[] b64d(String text) {
        return Base64.getDecoder().decode(text);
    }
}
