package io.github.kuttidb.client;

/**
 * The delivery's visibility lease expired before the commit (wire code 6).
 * Nothing was committed; obtain a fresh delivery to try again.
 */
public class JobDeliveryExpiredException extends KuttiDBJobException {

    private static final long serialVersionUID = 1L;

    public JobDeliveryExpiredException(int outcome, String detail) {
        super(CODE_DELIVERY_EXPIRED, outcome, detail);
    }
}
