package io.github.kuttidb.client;

/**
 * Result of one committed atomic completion. {@link #replayed} may differ
 * between the first success and a matched retry; every other field is
 * identical across retries while the receipt is retained.
 */
public final class JobCompletionResult {

    public final long commitId;
    public final long stateVersion;
    /** Message id of the committed output publish, or 0 when none was requested. */
    public final long outputMessageId;
    public final long completedAtMs;
    public final long receiptExpiresAtMs;
    public final boolean replayed;

    public JobCompletionResult(long commitId, long stateVersion, long outputMessageId,
                               long completedAtMs, long receiptExpiresAtMs, boolean replayed) {
        this.commitId = commitId;
        this.stateVersion = stateVersion;
        this.outputMessageId = outputMessageId;
        this.completedAtMs = completedAtMs;
        this.receiptExpiresAtMs = receiptExpiresAtMs;
        this.replayed = replayed;
    }

    @Override
    public String toString() {
        return "JobCompletionResult{commitId=" + commitId + ", stateVersion=" + stateVersion
                + ", outputMessageId=" + outputMessageId + ", completedAtMs=" + completedAtMs
                + ", receiptExpiresAtMs=" + receiptExpiresAtMs + ", replayed=" + replayed + "}";
    }
}
