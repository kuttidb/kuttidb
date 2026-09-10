package io.github.kuttidb.client;

/**
 * One durable-state entry read with {@link KuttiDBClient#stateGet(String)}:
 * exact value bytes, its version, and the commit id that last wrote it. The
 * {@code durable} keyspace is fixed, non-evictable, and never expires.
 */
public final class StateValue {

    public final byte[] value;
    public final long version;
    public final long commitId;

    public StateValue(byte[] value, long version, long commitId) {
        this.value = value;
        this.version = version;
        this.commitId = commitId;
    }

    @Override
    public String toString() {
        return "StateValue{version=" + version + ", commitId=" + commitId
                + ", valueLength=" + value.length + "}";
    }
}
