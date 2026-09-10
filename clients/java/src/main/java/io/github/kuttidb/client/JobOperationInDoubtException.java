package io.github.kuttidb.client;

/**
 * The commit outcome is unknown (wire code 10): the same-id receipt lookup or
 * an exact retry of the preserved intent is the only safe continuation.
 */
public class JobOperationInDoubtException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobOperationInDoubtException(int outcome, String detail) {
        super(CODE_OPERATION_IN_DOUBT, outcome, detail);
    }
}
