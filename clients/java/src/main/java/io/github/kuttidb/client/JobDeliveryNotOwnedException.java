package io.github.kuttidb.client;

/**
 * The proof does not belong to the current live delivery (wire code 7).
 * Obtain a valid attempt; never guess credentials.
 */
public class JobDeliveryNotOwnedException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobDeliveryNotOwnedException(int outcome, String detail) {
        super(CODE_DELIVERY_NOT_OWNED, outcome, detail);
    }
}
