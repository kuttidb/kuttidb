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

# Compose exposes the completion engine as a deliberate opt-in.  Preserve
# explicit command arguments so that `docker run ... --job-completion` and
# the environment switch can safely be used together.
case "${KUTTIDB_JOB_COMPLETION:-0}" in
    0|"") ;;
    1)
        has_job_completion=0
        for arg in "$@"; do
            if [ "$arg" = "--job-completion" ]; then
                has_job_completion=1
                break
            fi
        done
        if [ "$has_job_completion" -eq 0 ]; then
            set -- "$@" --job-completion
        fi
        ;;
    *)
        echo "KUTTIDB_JOB_COMPLETION must be 0 or 1" >&2
        exit 2
        ;;
esac

exec /usr/local/bin/kuttidb "$@"
