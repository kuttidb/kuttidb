package io.github.kuttidb.client;

/**
 * The job persistence engine is latched failed or currently unwritable
 * (wire code 11).
 */
public class JobPersistenceUnavailableException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobPersistenceUnavailableException(int outcome, String detail) {
        super(CODE_PERSISTENCE_UNAVAILABLE, outcome, detail);
    }
}
