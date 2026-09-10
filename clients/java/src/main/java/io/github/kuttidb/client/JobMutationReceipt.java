package io.github.kuttidb.client;

import java.util.UUID;

/**
 * Receipt of one direct durable-state mutation ({@code state_put} or
 * {@code state_delete}). Retrying the same operation id unchanged returns the
 * retained receipt with {@link #replayed} set; the shared ledger is visible
 * through {@link KuttiDBClient#durableOperation(UUID)}.
 */
public final class JobMutationReceipt {

    public final UUID operationId;
    /** {@code "state_put"} or {@code "state_delete"}. */
    public final String kind;
    public final long commitId;
    public final long stateVersion;
    public final long completedAtMs;
    public final long receiptExpiresAtMs;
    public final boolean replayed;

    public JobMutationReceipt(UUID operationId, String kind, long commitId, long stateVersion,
                              long completedAtMs, long receiptExpiresAtMs, boolean replayed) {
        this.operationId = operationId;
        this.kind = kind;
        this.commitId = commitId;
        this.stateVersion = stateVersion;
        this.completedAtMs = completedAtMs;
        this.receiptExpiresAtMs = receiptExpiresAtMs;
        this.replayed = replayed;
    }

    @Override
    public String toString() {
        return "JobMutationReceipt{operationId=" + operationId + ", kind=" + kind
                + ", commitId=" + commitId + ", stateVersion=" + stateVersion
                + ", completedAtMs=" + completedAtMs + ", receiptExpiresAtMs=" + receiptExpiresAtMs
                + ", replayed=" + replayed + "}";
    }
}
