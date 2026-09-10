package io.github.kuttidb.client;

import java.util.UUID;

/**
 * Options for direct durable-state mutations
 * ({@link KuttiDBClient#statePut(String, byte[], StateOptions)} and
 * {@link KuttiDBClient#stateDelete(String, StateOptions)}):
 * {@code new StateOptions().expectedVersion(1).operationId(uuid)}.
 */
public final class StateOptions {

    /**
     * Version gate: 0 creates only, a positive value must match the current
     * version exactly (no unchecked overwrite path exists).
     */
    public long expectedVersion;

    /**
     * Caller-owned deduplication id; {@code null} generates one at call time
     * and returns it in the receipt. Retries must retain the same value.
     */
    public UUID operationId;

    public StateOptions() {}

    public StateOptions expectedVersion(long v) {
        this.expectedVersion = v;
        return this;
    }

    public StateOptions operationId(UUID v) {
        this.operationId = v;
        return this;
    }
}
