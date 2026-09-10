package io.github.kuttidb.client;

/**
 * The canonical operation exceeded {@code --job-completion-max-bytes}
 * (wire code 3). Admission was refused before any record was appended.
 */
public class JobRequestTooLargeException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobRequestTooLargeException(int outcome, String detail) {
        super(CODE_REQUEST_TOO_LARGE, outcome, detail);
    }
}
