package io.github.kuttidb.client;

import java.nio.charset.StandardCharsets;

/**
 * Completion-capable delivery. The opaque {@link #proof} is the only
 * credential; native owner tokens and delivery tags stay private to the
 * server. The lease deadline is a wall-clock mirror for display and logging
 * only — fencing uses the server's monotonic lease.
 *
 * <p>The proof is one-use: a committed completion retires it. Closing the
 * connection does not unregister the consumer.
 */
public final class JobDelivery {

    public final byte[] storeId;
    public final String queue;
    public final long queueIncarnation;
    public final long messageId;
    public final int attempts;
    public final boolean redelivered;
    public final long leaseDeadlineMs;
    public final byte[] proof;
    public final byte[] value;

    public JobDelivery(byte[] storeId, String queue, long queueIncarnation, long messageId,
                       int attempts, boolean redelivered, long leaseDeadlineMs,
                       byte[] proof, byte[] value) {
        this.storeId = storeId;
        this.queue = queue;
        this.queueIncarnation = queueIncarnation;
        this.messageId = messageId;
        this.attempts = attempts;
        this.redelivered = redelivered;
        this.leaseDeadlineMs = leaseDeadlineMs;
        this.proof = proof;
        this.value = value;
    }

    /**
     * Start composing one completion intent from this delivery. The operation
     * id is generated here, once; persist the intent before submitting.
     * State and output fields are set on the returned builder.
     */
    public JobCompletionIntent.Builder toIntent() {
        return JobCompletionIntent.builder()
                .queue(queue)
                .queueIncarnation(queueIncarnation)
                .messageId(messageId);
    }

    /**
     * Convenience composition: {@code toIntent().stateKey(stateKey)
     * .expectedVersion(expectedVersion).stateValue(stateValue).build()}.
     */
    public JobCompletionIntent toIntent(String stateKey, long expectedVersion, byte[] stateValue) {
        return toIntent().stateKey(stateKey).expectedVersion(expectedVersion)
                .stateValue(stateValue).build();
    }

    /** Convenience composition including one output publish. */
    public JobCompletionIntent toIntent(String stateKey, long expectedVersion, byte[] stateValue,
                                        String outputQueue, long outputIncarnation,
                                        byte[] outputValue) {
        return toIntent()
                .stateKey(stateKey).expectedVersion(expectedVersion).stateValue(stateValue)
                .outputQueue(outputQueue).outputIncarnation(outputIncarnation)
                .outputValue(outputValue)
                .build();
    }

    @Override
    public String toString() {
        return "JobDelivery{queue=" + queue + ", incarnation=" + queueIncarnation
                + ", messageId=" + messageId + ", attempts=" + attempts
                + ", redelivered=" + redelivered + ", leaseDeadlineMs=" + leaseDeadlineMs
                + ", value=" + quote(value) + "}";
    }

    private static String quote(byte[] value) {
        if (value == null) return "null";
        return "\"" + new String(value, StandardCharsets.UTF_8) + "\"";
    }
}
