package io.github.kuttidb.client;

/**
 * Retained receipt of a direct durable-state mutation, returned by
 * {@link KuttiDBClient#durableOperation(java.util.UUID)} from the shared
 * operation-id ledger.
 */
public final class DurableOperationReceipt {

    public final String kind;
    public final long commitId;
    public final long stateVersion;
    public final long completedAtMs;
    public final long receiptExpiresAtMs;

    public DurableOperationReceipt(String kind, long commitId, long stateVersion,
                                   long completedAtMs, long receiptExpiresAtMs) {
        this.kind = kind;
        this.commitId = commitId;
        this.stateVersion = stateVersion;
        this.completedAtMs = completedAtMs;
        this.receiptExpiresAtMs = receiptExpiresAtMs;
    }

    @Override
    public String toString() {
        return "DurableOperationReceipt{kind=" + kind + ", commitId=" + commitId
                + ", stateVersion=" + stateVersion + ", completedAtMs=" + completedAtMs
                + ", receiptExpiresAtMs=" + receiptExpiresAtMs + "}";
    }
}
