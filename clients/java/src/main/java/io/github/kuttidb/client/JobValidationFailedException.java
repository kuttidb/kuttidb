package io.github.kuttidb.client;

/**
 * A malformed or semantically invalid job request (wire code 2). Nothing was
 * committed; fix the request before retrying.
 */
public class JobValidationFailedException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobValidationFailedException(int outcome, String detail) {
        super(CODE_VALIDATION_FAILED, outcome, detail);
    }
}
