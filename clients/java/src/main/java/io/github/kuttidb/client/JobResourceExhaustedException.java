package io.github.kuttidb.client;

/**
 * Admission was refused before any record was appended (wire code 8):
 * a capacity budget (state bytes, receipt memory/count, queue depth) is full.
 */
public class JobResourceExhaustedException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobResourceExhaustedException(int outcome, String detail) {
        super(CODE_RESOURCE_EXHAUSTED, outcome, detail);
    }
}
