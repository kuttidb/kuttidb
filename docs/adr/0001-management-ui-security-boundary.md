# Management UI security boundary

Status: accepted

The Management UI is a same-origin React application served by a single
Fastify gateway. Browser code never calls a KuttiDB Management API endpoint
directly and never receives an administrator token after it is submitted.

## Decisions

1. The gateway keeps tokens only in bounded process memory, indexed by a
   cryptographically random HttpOnly, SameSite=Strict browser-session cookie.
   Disconnect, lock, expiry, eviction, and shutdown clear token buffers.
2. A connection profile contains only label, origin, color, and optional last
   connection time. It may be stored under a versioned browser-storage key;
   tokens are not eligible for browser persistence or export.
3. The first console release has one gateway replica. Horizontal scaling needs
   a separately reviewed, audited ephemeral session design and sticky routing.
4. Every target is validated as an origin before a connection is attempted.
   Production requires an allowlist and HTTPS; loopback HTTP is only an
   explicit local-development or sidecar option. The gateway never follows
   upstream redirects and only proxies allowlisted Management API methods,
   headers, and paths.

The remaining SSRF work validates resolved and connected socket addresses;
the first foundation only performs syntactic origin and literal-IP policy
validation, so it must not be used as a production gateway until that work is
complete.
