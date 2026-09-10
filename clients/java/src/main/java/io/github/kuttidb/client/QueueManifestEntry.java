package io.github.kuttidb.client;

/**
 * One entry of {@link KuttiDBClient#queueManifest()}: stable queue identity,
 * durability, capacity, and revision per live queue.
 *
 * <p>Incarnation ids are required to compose completion intents for output
 * queues and are stable across restarts, changing only when a queue is
 * deleted and recreated.
 */
public final class QueueManifestEntry {

    public final String name;
    public final boolean durable;
    /** Stable identity: changes only when the queue is deleted and recreated. */
    public final long incarnation;
    public final long depth;
    public final long inflight;
    public final long maxDepth;
    public final long revision;

    public QueueManifestEntry(String name, boolean durable, long incarnation,
                              long depth, long inflight, long maxDepth, long revision) {
        this.name = name;
        this.durable = durable;
        this.incarnation = incarnation;
        this.depth = depth;
        this.inflight = inflight;
        this.maxDepth = maxDepth;
        this.revision = revision;
    }

    @Override
    public String toString() {
        return "QueueManifestEntry{name=" + name + ", durable=" + durable
                + ", incarnation=" + incarnation + ", depth=" + depth
                + ", inflight=" + inflight + ", maxDepth=" + maxDepth
                + ", revision=" + revision + "}";
    }
}
