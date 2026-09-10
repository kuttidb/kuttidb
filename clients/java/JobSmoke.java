import io.github.kuttidb.client.DurableOperationReceipt;
import io.github.kuttidb.client.JobCompletionIntent;
import io.github.kuttidb.client.JobCompletionResult;
import io.github.kuttidb.client.JobDelivery;
import io.github.kuttidb.client.JobDeliveryExpiredException;
import io.github.kuttidb.client.JobIdempotencyConflictException;
import io.github.kuttidb.client.JobMutationReceipt;
import io.github.kuttidb.client.JobReceipt;
import io.github.kuttidb.client.JobStateVersionConflictException;
import io.github.kuttidb.client.JobUnsupportedFeatureException;
import io.github.kuttidb.client.KuttiDBClient;
import io.github.kuttidb.client.KuttiDBJobException;
import io.github.kuttidb.client.QueueManifestEntry;
import io.github.kuttidb.client.StateOptions;
import io.github.kuttidb.client.StateValue;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Atomic job completion smoke test for the Java client. Spawns
 * {@code ./kuttidb <port> <wal> --job-completion} on an ephemeral port and
 * covers queue manifest discovery, completion-capable consumption, lossless
 * intent serialization (persisted before submission), the atomic commit,
 * state read-back, RESTART (SIGTERM + respawn on the same WAL), same-intent
 * replay, direct state mutations with receipts, the typed conflict
 * exceptions, and the unsupported-feature error against a server without
 * --job-completion. Also verifies that managed options forward the new
 * flags to {@code kuttidb ensure}.
 *
 * Usage: java JobSmoke [path-to-kuttidb-binary]  (default ../../kuttidb)
 */
public class JobSmoke {

    private static final String HOST = "127.0.0.1";

    // Persisted-before-submit state, exercising the documented recovery path:
    // the intent (including the operation id) is serialized BEFORE submitting.
    private static JobCompletionResult firstResult;
    private static Map<String, Object> persistedJson;
    private static String persistedLine;

    /** One spawned server process plus its endpoint and data directory. */
    static final class Server {
        final Process process;
        final int port;
        final Path dir;

        Server(Process process, int port, Path dir) {
            this.process = process;
            this.port = port;
            this.dir = dir;
        }

        KuttiDBClient connect() throws Exception {
            return new KuttiDBClient(HOST, port, null, null, 2);
        }
    }

    public static void main(String[] args) throws Exception {
        Path serverBin = args.length >= 1 ? Path.of(args[0]).toAbsolutePath()
                : Path.of("..", "..", "kuttidb").toAbsolutePath();
        Path work = Files.createTempDirectory("kuttidb-job-smoke");
        Server jobs = null;
        Server plain = null;
        try {
            jobs = start(serverBin, work.resolve("jobs"), true);
            long[] incarnations;
            try (KuttiDBClient c = jobs.connect()) {
                incarnations = manifestAndCapabilities(c);
                consumeAndComplete(c, incarnations[0], incarnations[1]);
            }
            jobs = restart(jobs, serverBin);
            try (KuttiDBClient c = jobs.connect()) {
                replayAfterRestart(c, incarnations);
                stateMutations(c);
                expiredDelivery(c);
            }
            stop(jobs);

            plain = start(serverBin, work.resolve("plain"), false);
            try (KuttiDBClient c = plain.connect()) {
                unsupportedFeature(c);
            }
            managedOptions(serverBin, work);
            System.out.println("JAVA JOB CLIENT OK");
        } finally {
            stop(jobs);
            stop(plain);
            deleteRecursively(work);
        }
    }

    // ---- feature scenario blocks ----------------------------------------------

    /** Capabilities plus queue manifest discovery; returns the incarnations. */
    static long[] manifestAndCapabilities(KuttiDBClient c) throws Exception {
        KuttiDBClient.Capabilities caps = c.capabilities();
        if (caps.minor < 8) throw new AssertionError("protocol minor " + caps.minor);
        if (!caps.hasFeature(KuttiDBClient.FEATURE_JOBS)) {
            throw new AssertionError("server lacks CAP_JOBS: " + caps);
        }

        c.queueDeclare("job-in", new KuttiDBClient.QueueOptions().durable(true).maxDepth(100));
        c.queueDeclare("job-out", new KuttiDBClient.QueueOptions().durable(true));
        List<QueueManifestEntry> manifest = c.queueManifest();
        if (manifest.size() < 2) throw new AssertionError("manifest missing queues: " + manifest);
        Map<String, QueueManifestEntry> byName = new HashMap<>();
        for (QueueManifestEntry entry : manifest) byName.put(entry.name, entry);
        QueueManifestEntry in = byName.get("job-in");
        QueueManifestEntry out = byName.get("job-out");
        if (in == null || out == null) throw new AssertionError("manifest missing job queues");
        if (!in.durable || !out.durable) throw new AssertionError("manifest durability wrong");
        if (in.incarnation == 0 || out.incarnation == 0) {
            throw new AssertionError("manifest incarnations must be non-zero");
        }
        if (in.maxDepth != 100) throw new AssertionError("manifest maxDepth " + in.maxDepth);
        if (in.depth != 0 || in.inflight != 0) throw new AssertionError("manifest counters");
        if (in.revision <= 0 || out.revision <= 0) throw new AssertionError("manifest revisions");
        return new long[]{in.incarnation, out.incarnation};
    }

    /** Consume with a proof, persist the intent, then commit atomically. */
    static void consumeAndComplete(KuttiDBClient c, long inIncarnation, long outIncarnation)
            throws Exception {
        c.queueConsumerRegister("job-worker");
        long messageId = c.queuePublish("job-in", "pdf-bytes".getBytes(StandardCharsets.UTF_8));

        JobDelivery delivery = c.jobConsume("job-in", "job-worker", Duration.ofSeconds(30));
        if (delivery == null) throw new AssertionError("job consume returned null");
        if (delivery.messageId != messageId) throw new AssertionError("delivery message id");
        if (delivery.queueIncarnation != inIncarnation) throw new AssertionError("delivery incarnation");
        if (delivery.proof.length != 16) throw new AssertionError("proof must be 16 bytes");
        if (delivery.storeId.length != 16) throw new AssertionError("store id must be 16 bytes");
        if (delivery.attempts < 1 || delivery.redelivered) throw new AssertionError("delivery flags");
        if (delivery.leaseDeadlineMs <= 0) throw new AssertionError("lease deadline missing");
        if (!"pdf-bytes".equals(new String(delivery.value, StandardCharsets.UTF_8))) {
            throw new AssertionError("delivery value");
        }

        // Compose + persist the intent BEFORE submitting (the recovery path).
        JobCompletionIntent intent = delivery.toIntent("job:42", 0,
                "extracted".getBytes(StandardCharsets.UTF_8), "job-out", outIncarnation,
                "job:42".getBytes(StandardCharsets.UTF_8));
        persistedJson = intent.toJSON();
        persistedLine = intent.toCompactString();
        if (!JobCompletionIntent.fromJSON(persistedJson).equals(intent)) {
            throw new AssertionError("JSON round trip must preserve the intent");
        }
        if (!JobCompletionIntent.fromCompactString(persistedLine).equals(intent)) {
            throw new AssertionError("compact string round trip must preserve the intent");
        }

        // 64-bit fields serialize as decimal strings: lossless beyond 2^53.
        JobCompletionIntent huge = JobCompletionIntent.builder()
                .queue("job-in").queueIncarnation(9007199254740993L).messageId(Long.MAX_VALUE)
                .stateKey("k").expectedVersion(9007199254740993L).stateValue(new byte[]{1})
                .outputQueue("job-out").outputIncarnation(9007199254740993L)
                .outputValue(new byte[0])
                .build();
        Map<String, Object> hugeJson = huge.toJSON();
        @SuppressWarnings("unchecked")
        Map<String, Object> input = (Map<String, Object>) hugeJson.get("input");
        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) hugeJson.get("output");
        if (!"9007199254740993".equals(input.get("queue_incarnation"))) {
            throw new AssertionError("queue incarnation must be a decimal string");
        }
        if (!Long.toString(Long.MAX_VALUE).equals(input.get("message_id"))) {
            throw new AssertionError("message id must be a decimal string");
        }
        if (!"9007199254740993".equals(output.get("queue_incarnation"))) {
            throw new AssertionError("output incarnation must be a decimal string");
        }
        if (!JobCompletionIntent.fromJSON(hugeJson).equals(huge)
                || !JobCompletionIntent.fromCompactString(huge.toCompactString()).equals(huge)) {
            throw new AssertionError("64-bit intent must round trip losslessly");
        }

        firstResult = c.jobComplete(intent, delivery.proof);
        if (firstResult.replayed) throw new AssertionError("first commit must not be replayed");
        if (firstResult.commitId == 0 || firstResult.stateVersion != 1
                || firstResult.outputMessageId == 0) {
            throw new AssertionError("completion result: " + firstResult);
        }
        if (firstResult.completedAtMs <= 0
                || firstResult.receiptExpiresAtMs < firstResult.completedAtMs) {
            throw new AssertionError("receipt timestamps: " + firstResult);
        }

        // State read-back through the committed completion.
        StateValue state = c.stateGet("job:42");
        if (state == null || !"extracted".equals(new String(state.value, StandardCharsets.UTF_8))) {
            throw new AssertionError("state read-back value");
        }
        if (state.version != firstResult.stateVersion || state.commitId != firstResult.commitId) {
            throw new AssertionError("state version/commit id");
        }
        StateValue binaryRead = c.stateGet("job:42".getBytes(StandardCharsets.UTF_8));
        if (binaryRead == null || binaryRead.version != state.version) {
            throw new AssertionError("binary-key state read");
        }
        KuttiDBClient.QueueStats inStats = c.queueStats("job-in");
        KuttiDBClient.QueueStats outStats = c.queueStats("job-out");
        if (inStats == null || inStats.depth != 0) throw new AssertionError("input queue must be empty");
        if (outStats == null || outStats.depth != 1) throw new AssertionError("output queue must hold one");

        JobReceipt receipt = c.jobCompletion(intent.operationId());
        if (receipt == null || receipt.commitId != firstResult.commitId
                || receipt.stateVersion != firstResult.stateVersion
                || receipt.outputMessageId != firstResult.outputMessageId) {
            throw new AssertionError("receipt lookup: " + receipt);
        }
    }

    /** SIGTERM + respawn on the same WAL directory; returns the new handle. */
    static Server restart(Server old, Path serverBin) throws Exception {
        old.process.destroy(); // SIGTERM: graceful teardown, exit 0
        if (!old.process.waitFor(15, java.util.concurrent.TimeUnit.SECONDS)) {
            throw new AssertionError("server did not exit on SIGTERM");
        }
        if (old.process.exitValue() != 0) {
            throw new AssertionError("server exit code " + old.process.exitValue());
        }
        return start(serverBin, old.dir, true); // same WAL, fresh ephemeral port
    }

    /** Same-intent replay after the restart: identical immutable result. */
    static void replayAfterRestart(KuttiDBClient c, long[] incarnations) throws Exception {
        if (persistedJson == null || persistedLine == null) {
            throw new AssertionError("persisted intent missing");
        }
        JobCompletionIntent rebuilt = JobCompletionIntent.fromJSON(persistedJson);
        if (!rebuilt.equals(JobCompletionIntent.fromCompactString(persistedLine))) {
            throw new AssertionError("persisted forms must agree");
        }
        if (rebuilt.queueIncarnation() != incarnations[0]) {
            throw new AssertionError("queue incarnation is stable across restarts");
        }

        JobCompletionResult replay = c.jobComplete(rebuilt, new byte[16]);
        if (!replay.replayed) throw new AssertionError("expected replayed=true, got " + replay);
        if (replay.commitId != firstResult.commitId || replay.stateVersion != firstResult.stateVersion
                || replay.outputMessageId != firstResult.outputMessageId) {
            throw new AssertionError("replay diverged: " + replay + " vs " + firstResult);
        }

        JobReceipt receipt = c.jobCompletion(rebuilt.operationId());
        if (receipt == null || receipt.commitId != firstResult.commitId) {
            throw new AssertionError("receipt lookup after restart");
        }
        KuttiDBClient.QueueStats out = c.queueStats("job-out");
        if (out == null || out.depth != 1) throw new AssertionError("output depth must stay 1");

        if (c.jobCompletion(UUID.randomUUID()) != null) {
            throw new AssertionError("unknown operation id must miss");
        }
    }

    /** Direct state mutations with receipts, conflicts, and the shared ledger. */
    static void stateMutations(KuttiDBClient c) throws Exception {
        JobMutationReceipt put = c.statePut("job:42", "corrected".getBytes(StandardCharsets.UTF_8),
                new StateOptions().expectedVersion(1));
        if (put.replayed || put.stateVersion != 2 || put.commitId == 0) {
            throw new AssertionError("state put receipt: " + put);
        }
        if (!"state_put".equals(put.kind) || put.operationId == null) {
            throw new AssertionError("state put receipt fields: " + put);
        }

        JobMutationReceipt replayPut = c.statePut("job:42", "corrected".getBytes(StandardCharsets.UTF_8),
                new StateOptions().expectedVersion(1).operationId(put.operationId));
        if (!replayPut.replayed || replayPut.commitId != put.commitId
                || replayPut.stateVersion != 2) {
            throw new AssertionError("state put replay: " + replayPut);
        }

        JobIdempotencyConflictException conflict = expect(() ->
                        c.statePut("job:42", "different".getBytes(StandardCharsets.UTF_8),
                                new StateOptions().expectedVersion(2).operationId(put.operationId)),
                JobIdempotencyConflictException.class);
        if (conflict.getCode() != KuttiDBJobException.CODE_IDEMPOTENCY_CONFLICT
                || !"idempotency_conflict".equals(conflict.codeName())
                || conflict.getOutcome() != KuttiDBJobException.OUTCOME_NOT_COMMITTED) {
            throw new AssertionError("idempotency conflict envelope: " + conflict);
        }

        JobStateVersionConflictException stale = expect(() ->
                        c.statePut("job:42", "x".getBytes(StandardCharsets.UTF_8),
                                new StateOptions().expectedVersion(99)),
                JobStateVersionConflictException.class);
        if (stale.getCode() != KuttiDBJobException.CODE_STATE_VERSION_CONFLICT) {
            throw new AssertionError("state version conflict code " + stale.getCode());
        }

        DurableOperationReceipt ledger = c.durableOperation(put.operationId);
        if (ledger == null || !"state_put".equals(ledger.kind) || ledger.stateVersion != 2
                || ledger.commitId != put.commitId) {
            throw new AssertionError("durable operation lookup: " + ledger);
        }
        if (c.durableOperation(UUID.randomUUID()) != null) {
            throw new AssertionError("unknown durable operation must miss");
        }

        JobMutationReceipt deletion = c.stateDelete("job:42",
                new StateOptions().expectedVersion(2));
        if (deletion.replayed || !"state_delete".equals(deletion.kind)) {
            throw new AssertionError("state delete receipt: " + deletion);
        }
        if (c.stateGet("job:42") != null) throw new AssertionError("deleted key must miss");

        JobMutationReceipt retryDelete = c.stateDelete("job:42",
                new StateOptions().expectedVersion(2).operationId(deletion.operationId));
        if (!retryDelete.replayed || retryDelete.commitId != deletion.commitId) {
            throw new AssertionError("state delete replay: " + retryDelete);
        }

        KuttiDBJobException notFound = expect(() ->
                        c.stateDelete("job:42", new StateOptions().expectedVersion(2)),
                KuttiDBJobException.class);
        if (notFound.getCode() != KuttiDBJobException.CODE_NOT_FOUND) {
            throw new AssertionError("absent delete must be not_found, got " + notFound.codeName());
        }

        // Binary-key overloads behave like the String-key ones.
        byte[] binKey = new byte[]{'b', 'i', 'n', 0, 'k'};
        JobMutationReceipt binPut = c.statePut(binKey, new byte[]{1, 2, 3}, new StateOptions());
        StateValue binValue = c.stateGet(binKey);
        if (binValue == null || binValue.version != binPut.stateVersion
                || binValue.commitId != binPut.commitId) {
            throw new AssertionError("binary key state read-back");
        }
        c.stateDelete(binKey, new StateOptions().expectedVersion(binPut.stateVersion));
    }

    /** A fresh delivery with a tiny lease expires before it can commit. */
    static void expiredDelivery(KuttiDBClient c) throws Exception {
        c.queuePublish("job-in", "second".getBytes(StandardCharsets.UTF_8));
        JobDelivery delivery = c.jobConsume("job-in", "job-worker", Duration.ofMillis(1));
        if (delivery == null) throw new AssertionError("second delivery missing");
        // The lease (1 ms) is certainly past before the commit, but the
        // delivery is still in flight: the fence must answer delivery_expired.
        Thread.sleep(50);
        JobDeliveryExpiredException expired = expect(() ->
                        c.jobComplete(delivery.toIntent("job:42", 0,
                                        "late".getBytes(StandardCharsets.UTF_8)),
                                delivery.proof),
                JobDeliveryExpiredException.class);
        if (expired.getCode() != KuttiDBJobException.CODE_DELIVERY_EXPIRED) {
            throw new AssertionError("delivery expired code " + expired.getCode());
        }
    }

    /** A server without --job-completion answers the typed unsupported envelope. */
    static void unsupportedFeature(KuttiDBClient c) throws Exception {
        JobUnsupportedFeatureException unsupported = expect(() ->
                        c.jobConsume("job-in", "job-worker", Duration.ofSeconds(5)),
                JobUnsupportedFeatureException.class);
        if (unsupported.getCode() != KuttiDBJobException.CODE_UNSUPPORTED_FEATURE
                || unsupported.getOutcome() != KuttiDBJobException.OUTCOME_NOT_COMMITTED) {
            throw new AssertionError("unsupported feature envelope: " + unsupported);
        }
        if (!unsupported.getMessage().contains("unsupported_feature (not_committed)")) {
            throw new AssertionError("exception message: " + unsupported.getMessage());
        }
    }

    /** Managed `ensure` propagates --job-completion; the server reports CAP_JOBS. */
    static void managedOptions(Path serverBin, Path work) throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) {
            port = reservation.getLocalPort();
        }
        KuttiDBClient.ManagedServerOptions options = new KuttiDBClient.ManagedServerOptions(
                work.resolve("managed"), serverBin, 250, 5000, null, "tcp", HOST, port)
                .jobCompletion(true)
                .jobReceiptsMaxCount(100000);
        try (KuttiDBClient client = KuttiDBClient.connectManaged(options)) {
            if (!client.capabilities().hasFeature(KuttiDBClient.FEATURE_JOBS)) {
                throw new AssertionError("managed instance lacks CAP_JOBS");
            }
        }
    }

    // ---- server plumbing -------------------------------------------------------

    static Server start(Path serverBin, Path dir, boolean jobs) throws Exception {
        Files.createDirectories(dir);
        int port = freePort();
        List<String> command = new ArrayList<>(List.of(serverBin.toString(),
                String.valueOf(port), dir.resolve("kuttidb.wal").toString()));
        if (jobs) command.add("--job-completion");
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(dir.resolve("server.log").toFile())
                .start();
        Server server = new Server(process, port, dir);
        waitHealthy(server);
        return server;
    }

    static void stop(Server server) {
        if (server == null) return;
        server.process.destroy();
        try {
            if (!server.process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                server.process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static void waitHealthy(Server server) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        StringBuilder lastError = new StringBuilder();
        while (System.currentTimeMillis() < deadline) {
            if (!server.process.isAlive()) {
                throw new AssertionError("server exited: " + serverLog(server));
            }
            try (KuttiDBClient probe = new KuttiDBClient(HOST, server.port, null, null, 1)) {
                if (probe.health()) return;
                lastError.append("health false; ");
            } catch (Exception e) {
                lastError.append(e).append("; ");
            }
            Thread.sleep(50);
        }
        throw new AssertionError("server did not become healthy: " + lastError + serverLog(server));
    }

    static String serverLog(Server server) throws Exception {
        Path log = server.dir.resolve("server.log");
        return Files.exists(log) ? Files.readString(log) : "<no log>";
    }

    static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) return;
        try (var stream = Files.walk(path)) {
            for (Path p : stream.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    // ---- assertion helpers -------------------------------------------------------

    interface Op {
        void run() throws Exception;
    }

    static <E extends Exception> E expect(Op op, Class<E> type) throws Exception {
        try {
            op.run();
        } catch (Exception e) {
            if (type.isInstance(e)) return type.cast(e);
            throw e;
        }
        throw new AssertionError("expected " + type.getSimpleName() + " but nothing was thrown");
    }
}
