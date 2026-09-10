package io.github.kuttidb.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Atomic job completion operations (opcodes 0x70&ndash;0x77, capability bit
 * 1&lt;&lt;16): completion-capable consumption, all-or-nothing completions,
 * receipt lookups, direct durable-state mutations, and queue manifest
 * discovery. The typed error envelope
 * {@code [status 0x02][len:4][code:1][outcome:1][detail]} maps onto
 * {@link KuttiDBJobException} subclasses; a MISS is "empty / absent" where
 * the contract defines it, never a silent failure. Wire layouts are specified
 * in docs/design/ATOMIC_JOB_COMPLETION.md and docs/design/PROTOCOL.md.
 *
 * <p>Not part of the public API; see {@link KuttiDBClient} for entry points.
 */
final class KuttiDBJobs {

    private KuttiDBJobs() {}

    private static final int MAX_NAME = 255;
    private static final int MAX_KEY = KuttiDBProtocol.MAX_KEY;
    private static final int OP_ID_LEN = 16;
    private static final int PROOF_LEN = 16;

    // ---- queue manifest -----------------------------------------------------

    static List<QueueManifestEntry> manifest(KuttiDBClient client) throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_QUEUE_MANIFEST, new byte[0], new byte[0]));
        requireOK(r, "queue manifest");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        int n = d.u16();
        List<QueueManifestEntry> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String name = d.utf8(d.u16());
            boolean durable = d.u8() != 0;
            long incarnation = d.u64();
            long depth = d.u64();
            long inflight = d.u64();
            long maxDepth = d.u64();
            long revision = d.u64();
            out.add(new QueueManifestEntry(name, durable, incarnation, depth, inflight,
                    maxDepth, revision));
        }
        d.done();
        return out;
    }

    // ---- consume + complete -------------------------------------------------

    static JobDelivery jobConsume(KuttiDBClient client, String queue, String consumer,
                                  long visibilityMillis) throws IOException {
        byte[] kb = nameBytes(queue);
        if (consumer == null || consumer.isEmpty() || utf8(consumer).length > MAX_NAME) {
            throw new KuttiDBException("invalid consumer");
        }
        if (visibilityMillis < 0) throw new KuttiDBException("visibility must be non-negative");
        byte[] payload = KuttiDBProtocol.concat(KuttiDBProtocol.l16String(consumer, "consumer"),
                KuttiDBProtocol.u64(visibilityMillis));
        KuttiDBProtocol.Reply r = client.stateRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_JOB_CONSUME, kb, payload));
        if (r.miss()) return null; // queue empty (or this connection's prefetch bound is hit)
        requireOK(r, "job consume");
        // [store_id:16][incarnation:8][message_id:8][attempts:4][redelivered:1]
        // [lease_deadline:8][proof:16][payload] = 61 + payload bytes.
        if (r.value.length < 61) throw new KuttiDBException("malformed job consume response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        byte[] storeId = d.bytes(16);
        long incarnation = d.u64();
        long messageId = d.u64();
        int attempts = (int) d.u32();
        boolean redelivered = d.u8() != 0;
        long leaseDeadline = d.u64();
        byte[] proof = d.bytes(PROOF_LEN);
        byte[] value = d.bytes(d.remaining());
        return new JobDelivery(storeId, queue, incarnation, messageId, attempts, redelivered,
                leaseDeadline, proof, value);
    }

    static JobCompletionResult jobComplete(KuttiDBClient client, JobCompletionIntent intent,
                                           byte[] proof) throws IOException {
        if (intent == null) throw new KuttiDBException("completion intent is required");
        if (proof == null || proof.length != PROOF_LEN) {
            throw new KuttiDBException("delivery proof must be 16 bytes");
        }
        byte[] kb = nameBytes(intent.queue());
        List<byte[]> parts = new ArrayList<>();
        parts.add(uuidBytes(intent.operationId()));
        parts.add(KuttiDBProtocol.u64(intent.queueIncarnation()));
        parts.add(KuttiDBProtocol.u64(intent.messageId()));
        parts.add(proof);
        parts.add(KuttiDBProtocol.u16(intent.stateKey().length));
        parts.add(intent.stateKey());
        parts.add(KuttiDBProtocol.u64(intent.expectedVersion()));
        parts.add(KuttiDBProtocol.u32(intent.stateValue().length));
        parts.add(intent.stateValue());
        if (intent.outputQueue() == null) {
            parts.add(new byte[]{0});
        } else {
            byte[] oq = utf8(intent.outputQueue());
            if (oq.length > MAX_NAME) throw new KuttiDBException("output queue name too large");
            parts.add(new byte[]{1});
            parts.add(KuttiDBProtocol.u16(oq.length));
            parts.add(oq);
            parts.add(KuttiDBProtocol.u64(intent.outputIncarnation()));
            parts.add(KuttiDBProtocol.u32(intent.outputValue().length));
            parts.add(intent.outputValue());
        }
        byte[] value = KuttiDBProtocol.concat(parts.toArray(new byte[0][]));
        KuttiDBProtocol.checkValueLength(value);
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_JOB_COMPLETE, kb, value));
        requireOK(r, "job completion");
        return decodeCompletion(r.value);
    }

    /** Response body: [commit:8][state_version:8][output_msg:8][completed:8][expires:8][replayed:1]. */
    private static JobCompletionResult decodeCompletion(byte[] value) throws KuttiDBException {
        if (value.length != 41) throw new KuttiDBException("malformed job completion response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(value);
        return new JobCompletionResult(d.u64(), d.u64(), d.u64(), d.u64(), d.u64(), d.u8() != 0);
    }

    // ---- receipts ------------------------------------------------------------

    static JobReceipt jobCompletion(KuttiDBClient client, UUID operationId) throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_JOB_RECEIPT, new byte[0], opIdBytes(operationId)));
        if (r.miss()) return null;
        requireOK(r, "job receipt lookup");
        if (r.value.length != 41) throw new KuttiDBException("malformed job receipt response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        return new JobReceipt(operationId, d.u64(), d.u64(), d.u64(), d.u64(), d.u64());
    }

    static DurableOperationReceipt durableOperation(KuttiDBClient client, UUID operationId)
            throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_DURABLE_OPERATION, new byte[0], opIdBytes(operationId)));
        if (r.miss()) return null;
        requireOK(r, "durable operation lookup");
        if (r.value.length != 33) throw new KuttiDBException("malformed durable operation response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        int kindCode = d.u8();
        return new DurableOperationReceipt(kindName(kindCode), d.u64(), d.u64(), d.u64(), d.u64());
    }

    // ---- direct durable state ------------------------------------------------

    static StateValue stateGet(KuttiDBClient client, byte[] key) throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                KuttiDBProtocol.OP_STATE_GET, stateKeyBytes(key), new byte[0]));
        if (r.miss()) return null;
        requireOK(r, "state get");
        if (r.value.length < 16) throw new KuttiDBException("malformed state read response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        long version = d.u64();
        long commitId = d.u64();
        return new StateValue(d.bytes(d.remaining()), version, commitId);
    }

    static JobMutationReceipt statePut(KuttiDBClient client, byte[] key, byte[] value,
                                       StateOptions options) throws IOException {
        if (value == null) value = new byte[0];
        long expected = options == null ? 0 : options.expectedVersion;
        if (expected < 0) throw new KuttiDBException("expectedVersion must be non-negative");
        KuttiDBProtocol.checkValueLength(value);
        UUID opId = resolveOperationId(options);
        byte[] payload = KuttiDBProtocol.concat(uuidBytes(opId),
                KuttiDBProtocol.u64(expected), value);
        return mutation(client, KuttiDBProtocol.OP_STATE_PUT, "state_put", key, payload, opId);
    }

    static JobMutationReceipt stateDelete(KuttiDBClient client, byte[] key, StateOptions options)
            throws IOException {
        long expected = options == null ? 0 : options.expectedVersion;
        if (expected <= 0) {
            throw new KuttiDBException("state delete requires the entry's current positive version");
        }
        UUID opId = resolveOperationId(options);
        byte[] payload = KuttiDBProtocol.concat(uuidBytes(opId), KuttiDBProtocol.u64(expected));
        return mutation(client, KuttiDBProtocol.OP_STATE_DELETE, "state_delete", key, payload, opId);
    }

    private static JobMutationReceipt mutation(KuttiDBClient client, int op, String kind,
                                               byte[] key, byte[] payload, UUID opId)
            throws IOException {
        KuttiDBProtocol.Reply r = client.pooledRequest(KuttiDBProtocol.frame(
                op, stateKeyBytes(key), payload));
        if (r.miss()) {
            // Definite not-found: no retained receipt and no durable effect.
            throw KuttiDBJobException.fromEnvelope(KuttiDBJobException.CODE_NOT_FOUND,
                    KuttiDBJobException.OUTCOME_NOT_COMMITTED, null);
        }
        requireOK(r, "state mutation");
        if (r.value.length != 33) throw new KuttiDBException("malformed state mutation response");
        KuttiDBProtocol.Decoder d = new KuttiDBProtocol.Decoder(r.value);
        return new JobMutationReceipt(opId, kind, d.u64(), d.u64(), d.u64(), d.u64(), d.u8() != 0);
    }

    // ---- error envelope ------------------------------------------------------

    /** Decode the typed job error envelope body: [code:1][outcome:1][detail]. */
    private static KuttiDBJobException jobError(byte[] body) {
        int code = body.length >= 1 ? body[0] & 0xFF : 0;
        int outcome = body.length >= 2 ? body[1] & 0xFF : 0;
        String detail = body.length > 2
                ? new String(body, 2, body.length - 2, java.nio.charset.StandardCharsets.UTF_8)
                : null;
        return KuttiDBJobException.fromEnvelope(code, outcome, detail);
    }

    /** Fail unless the reply is OK; ERROR maps to the typed job exception. */
    private static KuttiDBProtocol.Reply requireOK(KuttiDBProtocol.Reply r, String what)
            throws IOException {
        if (r.status == KuttiDBProtocol.ST_ERR) throw jobError(r.value);
        if (!r.ok()) {
            throw new KuttiDBException(what + " failed: unexpected status 0x"
                    + Integer.toHexString(r.status));
        }
        return r;
    }

    private static String kindName(int kindCode) {
        switch (kindCode) {
            case 2: return "state_put";
            case 3: return "state_delete";
            default: return "kind_" + kindCode;
        }
    }

    private static UUID resolveOperationId(StateOptions options) {
        return options != null && options.operationId != null
                ? options.operationId
                : java.util.UUID.randomUUID();
    }

    private static byte[] opIdBytes(UUID operationId) throws KuttiDBException {
        if (operationId == null) throw new KuttiDBException("operation id is required");
        return uuidBytes(operationId);
    }

    static byte[] uuidBytes(UUID uuid) {
        return KuttiDBProtocol.concat(KuttiDBProtocol.u64(uuid.getMostSignificantBits()),
                KuttiDBProtocol.u64(uuid.getLeastSignificantBits()));
    }

    static UUID uuidFromBytes(byte[] b) throws KuttiDBException {
        if (b == null || b.length != OP_ID_LEN) {
            throw new KuttiDBException("operation id must be 16 bytes");
        }
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) msb = (msb << 8) | (b[i] & 0xFFL);
        for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (b[i] & 0xFFL);
        return new UUID(msb, lsb);
    }

    private static byte[] stateKeyBytes(byte[] key) throws KuttiDBException {
        if (key == null || key.length == 0 || key.length > MAX_KEY) {
            throw new KuttiDBException("invalid durable state key");
        }
        return key;
    }

    private static byte[] nameBytes(String name) throws KuttiDBException {
        if (name == null || name.isEmpty() || utf8(name).length > MAX_NAME) {
            throw new KuttiDBException("invalid queue name");
        }
        return utf8(name);
    }

    private static byte[] utf8(String s) {
        return KuttiDBProtocol.utf8(s);
    }
}
