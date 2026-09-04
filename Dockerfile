# Minimal Linux image for the network server.  TLS is intentionally disabled in
# this baseline image so it has no OpenSSL runtime dependency; terminate TLS at
# an ingress or build a site-specific image with `make TLS=1`.
FROM alpine:3.21 AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY Makefile ./
COPY src ./src
RUN make TLS=0 kuttidb

FROM alpine:3.21
RUN addgroup -S -g 10001 kuttidb && adduser -S -D -H -u 10001 -G kuttidb kuttidb \
    && mkdir -p /var/lib/kuttidb && chown kuttidb:kuttidb /var/lib/kuttidb
COPY --from=build /src/kuttidb /usr/local/bin/kuttidb
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
RUN chmod 0755 /usr/local/bin/docker-entrypoint
USER 10001:10001
WORKDIR /var/lib/kuttidb
EXPOSE 7379
# TCP liveness: the process accepts connections. The durability-aware probe
# is the authenticated HTTP /ready on the optional metrics port (see
# KUBERNETES.md); a container-level check cannot carry a bearer token.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z 127.0.0.1 7379 || exit 1
ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]
CMD ["7379", "/var/lib/kuttidb/kuttidb.wal", "100", "-", "64", "--durability", "always", "--queue-wal", "/var/lib/kuttidb/queue.wal"]
