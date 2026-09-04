package io.github.kuttidb.client;

import java.io.IOException;

/**
 * A KuttiDB-level failure: the server answered ERROR (0x02), refused a
 * capability-dependent operation, or produced a malformed response.
 *
 * Per the protocol's fail-closed rule, an ERROR means the durable effect of
 * the request did not happen (or is unknown until recovery reconciles).
 * Misses are not exceptions: they map onto {@code null}, {@code false}, zero,
 * or an empty result per operation, mirroring the other native clients.
 *
 * This is an {@link IOException} subclass so existing callers that catch
 * IOException keep working.
 */
public class KuttiDBException extends IOException {

    private static final long serialVersionUID = 1L;

    public KuttiDBException(String message) {
        super(message);
    }

    public KuttiDBException(String message, Throwable cause) {
        super(message, cause);
    }
}
