#!/bin/sh
set -eu

# Kubernetes/Docker secret projections are normally root-owned. KuttiDB
# intentionally rejects those for an auth file, so create a private copy for
# the non-root server process before it starts.
if [ -n "${KUTTIDB_AUTH_SOURCE:-}" ]; then
    : "${KUTTIDB_AUTH_DEST:=/var/lib/kuttidb/auth.token}"
    mkdir -p "$(dirname "$KUTTIDB_AUTH_DEST")"
    umask 077
    cp "$KUTTIDB_AUTH_SOURCE" "$KUTTIDB_AUTH_DEST"
    chmod 0600 "$KUTTIDB_AUTH_DEST"
fi

# Same treatment for the optional metrics bearer token.
if [ -n "${KUTTIDB_METRICS_TOKEN_SOURCE:-}" ]; then
    : "${KUTTIDB_METRICS_TOKEN_DEST:=/var/lib/kuttidb/metrics.token}"
    mkdir -p "$(dirname "$KUTTIDB_METRICS_TOKEN_DEST")"
    umask 077
    cp "$KUTTIDB_METRICS_TOKEN_SOURCE" "$KUTTIDB_METRICS_TOKEN_DEST"
    chmod 0600 "$KUTTIDB_METRICS_TOKEN_DEST"
fi

exec /usr/local/bin/kuttidb "$@"
