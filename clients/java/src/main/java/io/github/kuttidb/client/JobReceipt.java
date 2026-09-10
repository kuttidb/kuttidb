package io.github.kuttidb.client;

import java.util.UUID;

/**
 * Retained receipt of one committed completion, returned by
 * {@link KuttiDBClient#jobCompletion(UUID)}.
 *
 * <p>Authenticated lookup never requires the (now stale) delivery proof and
 * works after a restart. A miss means "no retained receipt" — absence is
 * never proof that the operation never executed.
 */
public final class JobReceipt {

    public final UUID operationId;
    public final long commitId;
    public final long stateVersion;
    public final long outputMessageId;
    public final long completedAtMs;
    public final long receiptExpiresAtMs;

    public JobReceipt(UUID operationId, long commitId, long stateVersion,
                      long outputMessageId, long completedAtMs, long receiptExpiresAtMs) {
        this.operationId = operationId;
        this.commitId = commitId;
        this.stateVersion = stateVersion;
        this.outputMessageId = outputMessageId;
        this.completedAtMs = completedAtMs;
        this.receiptExpiresAtMs = receiptExpiresAtMs;
    }

    @Override
    public String toString() {
        return "JobReceipt{operationId=" + operationId + ", commitId=" + commitId
                + ", stateVersion=" + stateVersion + ", outputMessageId=" + outputMessageId
                + ", completedAtMs=" + completedAtMs + ", receiptExpiresAtMs=" + receiptExpiresAtMs + "}";
    }
}
