# KuttiDB Management Console

Self-hosted web console for [KuttiDB](https://kuttidb.com)'s Management API:
keyspace entries, queues, routing, stream tails, consumer groups, atomic
operations, and maintenance jobs.

This package ships a pre-built console and a tiny local server. No clone, no
build step -- run it with `npx`.

## Run it

**1. Start KuttiDB with the Management API enabled** (see the
[main README](https://github.com/kuttidb/kuttidb#management-api--web-console)
for the full walkthrough):

```sh
umask 077
printf '%s\n' 'replace-with-a-long-random-token' > admin.token
./kuttidb 7379 kuttidb.wal \
  --admin-bind 127.0.0.1:7380 \
  --admin-token-file admin.token \
  --admin-audit-log admin-audit.jsonl
```

**2. In a second terminal, start the console:**

```sh
npx @kuttidb/management-ui
```

This opens `http://127.0.0.1:8080` in your browser. Connect to
`http://127.0.0.1:7380` using the token from `admin.token`. The token is held
in the console's own memory for this session only and is never written to
disk or browser storage.

## Options

```
npx @kuttidb/management-ui [options]

-p, --port <port>            Port to listen on (default: 8080)
-H, --host <host>            Host to bind to (default: 127.0.0.1)
    --no-open                Do not open a browser automatically
    --allow-private-targets  Allow connecting to private (RFC1918) targets, not just loopback
    --strict-https           Require HTTPS even for loopback targets
    --target-allowlist <hosts>  Comma-separated hostnames this console may connect to
-v, --version                Print the version and exit
-h, --help                   Show this help and exit
```

## Security notes

- The console never runs KuttiDB itself; it only proxies to a Management API
  you started and control.
- By default it binds to `127.0.0.1` and accepts plaintext HTTP only to
  loopback targets, matching the Management API's own default posture.
- Administrator tokens are held in bounded server memory for the life of a
  browser session; nothing is persisted to disk.

## More

- [Management API and security model](https://github.com/kuttidb/kuttidb/blob/main/docs/api/MANAGEMENT_API.md)
- [Console source](https://github.com/kuttidb/kuttidb/tree/main/apps/management-ui)
- [KuttiDB](https://github.com/kuttidb/kuttidb)
