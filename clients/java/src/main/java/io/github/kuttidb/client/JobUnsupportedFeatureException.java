package io.github.kuttidb.client;

/**
 * The server lacks the atomic job completion feature or runs with
 * {@code --job-completion} off (wire code 1).
 */
public class JobUnsupportedFeatureException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobUnsupportedFeatureException(int outcome, String detail) {
        super(CODE_UNSUPPORTED_FEATURE, outcome, detail);
    }
}
