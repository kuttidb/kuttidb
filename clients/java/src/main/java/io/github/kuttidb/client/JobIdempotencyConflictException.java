package io.github.kuttidb.client;

/**
 * A retained operation id was reused for a different request (wire code 4).
 * Stop: never generate a replacement id automatically.
 */
public class JobIdempotencyConflictException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobIdempotencyConflictException(int outcome, String detail) {
        super(CODE_IDEMPOTENCY_CONFLICT, outcome, detail);
    }
}
