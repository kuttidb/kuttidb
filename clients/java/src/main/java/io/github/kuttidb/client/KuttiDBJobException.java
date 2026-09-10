package io.github.kuttidb.client;

/**
 * Typed atomic-job-completion failure, mirroring the wire error envelope
 * {@code [status 0x02][len:4][code:1][outcome:1][detail]} carried by the job
 * opcodes (0x70&ndash;0x77). See docs/design/ATOMIC_JOB_COMPLETION.md.
 *
 * <p>The outcome byte separates "definitely not committed" from "unknown" (a
 * possibly committed append whose durability could not be resolved): never
 * conflate a conflict, an absence, and an unknown outcome. On an unknown
 * outcome the original intent (operation id and semantic request) must be
 * preserved for reconciliation; the receipt lookup
 * ({@link KuttiDBClient#jobCompletion(java.util.UUID)}) is the safe next step.
 *
 * <p>This is a {@link KuttiDBException} (an {@link java.io.IOException})
 * subclass so existing callers that catch IOException keep working.
 */
public class KuttiDBJobException extends KuttiDBException {

    private static final long serialVersionUID = 1L;

    // ---- stable wire codes (docs/design/ATOMIC_JOB_COMPLETION.md section 9) --
    public static final int CODE_UNSUPPORTED_FEATURE = 1;
    public static final int CODE_VALIDATION_FAILED = 2;
    public static final int CODE_REQUEST_TOO_LARGE = 3;
    public static final int CODE_IDEMPOTENCY_CONFLICT = 4;
    public static final int CODE_STATE_VERSION_CONFLICT = 5;
    public static final int CODE_DELIVERY_EXPIRED = 6;
    public static final int CODE_DELIVERY_NOT_OWNED = 7;
    public static final int CODE_RESOURCE_EXHAUSTED = 8;
    /** Reserved; not emitted by the current single-commit design. */
    public static final int CODE_OPERATION_IN_PROGRESS = 9;
    public static final int CODE_OPERATION_IN_DOUBT = 10;
    public static final int CODE_PERSISTENCE_UNAVAILABLE = 11;
    public static final int CODE_NOT_FOUND = 12;

    /** Outcome: the durable effect did not happen. */
    public static final int OUTCOME_NOT_COMMITTED = 0;
    /** Outcome: a possibly committed append whose durability is unresolved. */
    public static final int OUTCOME_UNKNOWN = 1;

    private final int code;
    private final int outcome;
    private final String detail;

    KuttiDBJobException(int code, int outcome, String detail) {
        super(message(code, outcome, detail));
        this.code = code;
        this.outcome = outcome;
        this.detail = detail;
    }

    private static String message(int code, int outcome, String detail) {
        StringBuilder text = new StringBuilder("job operation failed: ")
                .append(codeName(code)).append(" (").append(outcomeName(outcome)).append(')');
        if (detail != null && !detail.isEmpty()) text.append(": ").append(detail);
        return text.toString();
    }

    /** The stable wire code number. */
    public int getCode() {
        return code;
    }

    /** {@link #OUTCOME_NOT_COMMITTED} (0) or {@link #OUTCOME_UNKNOWN} (1). */
    public int getOutcome() {
        return outcome;
    }

    /** The server's optional detail text, or {@code null}. */
    public String getDetail() {
        return detail;
    }

    /** Stable wire name of this error code, e.g. {@code "idempotency_conflict"}. */
    public String codeName() {
        return codeName(code);
    }

    /** {@code true} when the commit outcome is unknown and only a receipt
     *  lookup or an exact same-id retry may resolve it. */
    public boolean isUnknownOutcome() {
        return outcome == OUTCOME_UNKNOWN;
    }

    /** Stable wire name for a numeric code; unknown codes render as {@code code_N}. */
    public static String codeName(int code) {
        switch (code) {
            case CODE_UNSUPPORTED_FEATURE: return "unsupported_feature";
            case CODE_VALIDATION_FAILED: return "validation_failed";
            case CODE_REQUEST_TOO_LARGE: return "request_too_large";
            case CODE_IDEMPOTENCY_CONFLICT: return "idempotency_conflict";
            case CODE_STATE_VERSION_CONFLICT: return "state_version_conflict";
            case CODE_DELIVERY_EXPIRED: return "delivery_expired";
            case CODE_DELIVERY_NOT_OWNED: return "delivery_not_owned";
            case CODE_RESOURCE_EXHAUSTED: return "resource_exhausted";
            case CODE_OPERATION_IN_PROGRESS: return "operation_in_progress";
            case CODE_OPERATION_IN_DOUBT: return "operation_in_doubt";
            case CODE_PERSISTENCE_UNAVAILABLE: return "persistence_unavailable";
            case CODE_NOT_FOUND: return "not_found";
            default: return "code_" + code;
        }
    }

    /** {@code "not_committed"} or {@code "unknown"} for a wire outcome byte. */
    public static String outcomeName(int outcome) {
        return outcome == OUTCOME_UNKNOWN ? "unknown" : "not_committed";
    }

    /** Build the typed exception for one decoded error envelope body. */
    static KuttiDBJobException fromEnvelope(int code, int outcome, String detail) {
        switch (code) {
            case CODE_UNSUPPORTED_FEATURE: return new JobUnsupportedFeatureException(outcome, detail);
            case CODE_VALIDATION_FAILED: return new JobValidationFailedException(outcome, detail);
            case CODE_REQUEST_TOO_LARGE: return new JobRequestTooLargeException(outcome, detail);
            case CODE_IDEMPOTENCY_CONFLICT: return new JobIdempotencyConflictException(outcome, detail);
            case CODE_STATE_VERSION_CONFLICT: return new JobStateVersionConflictException(outcome, detail);
            case CODE_DELIVERY_EXPIRED: return new JobDeliveryExpiredException(outcome, detail);
            case CODE_DELIVERY_NOT_OWNED: return new JobDeliveryNotOwnedException(outcome, detail);
            case CODE_RESOURCE_EXHAUSTED: return new JobResourceExhaustedException(outcome, detail);
            case CODE_OPERATION_IN_DOUBT: return new JobOperationInDoubtException(outcome, detail);
            case CODE_PERSISTENCE_UNAVAILABLE: return new JobPersistenceUnavailableException(outcome, detail);
            default: return new KuttiDBJobException(code, outcome, detail);
        }
    }
}
