package io.github.kuttidb.client;

/**
 * The version-checked state write was rejected (wire code 5). Re-read the
 * state and let the application decide; the input delivery is untouched.
 */
public class JobStateVersionConflictException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobStateVersionConflictException(int outcome, String detail) {
        super(CODE_STATE_VERSION_CONFLICT, outcome, detail);
    }
}
