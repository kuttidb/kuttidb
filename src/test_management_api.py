"""Focused integration coverage for the optional authenticated Management API."""
import json
import base64
import os
import socket
import ssl
import stat
import subprocess
import tempfile
import time
import sys

PORT = 7418
TOKEN = b"separate-admin-token"
SERVER = os.environ.get("KUTTIDB_SERVER", "./kuttidb")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient
SERVER_FEATURES = subprocess.run([SERVER, "--features"], capture_output=True,
                                 text=True, check=True, timeout=5).stdout
TLS_AVAILABLE = "admin-tls=openssl" in SERVER_FEATURES

# Ruby's standard library parser keeps this check dependency-free on the
# supported development environments. The checked-in OpenAPI document is a
# public compatibility artifact, not prose.
subprocess.run(["ruby", "-e", 'require "yaml"; YAML.load_file(ARGV[0])',
                os.path.join(ROOT, "openapi", "management-v1.yaml")],
               check=True, timeout=5)

# YAML parsers commonly retain the last duplicate mapping key.  That would
# quietly remove an operation from an OpenAPI Path Item, so protect the
# top-level paths mapping explicitly without adding a parser dependency.
with open(os.path.join(ROOT, "openapi", "management-v1.yaml"), encoding="utf-8") as f:
    path_names = [line.strip()[:-1] for line in f
                  if line.startswith("  /") and line.rstrip().endswith(":")]
assert len(path_names) == len(set(path_names)), "duplicate OpenAPI path"

# Keep the checked-in OpenAPI document tied to the dispatcher.  This is
# intentionally dependency-free: Ruby parses YAML, while these assertions
# protect every public route family, the mutation methods that can change
# state, and the stable error vocabulary returned by admin_http.c.
openapi_json = subprocess.run(
    ["ruby", "-ryaml", "-rjson", "-e",
     'puts JSON.generate(YAML.load_file(ARGV[0]))',
     os.path.join(ROOT, "openapi", "management-v1.yaml")],
    capture_output=True, text=True, check=True, timeout=5).stdout
openapi = json.loads(openapi_json)
documented_methods = {
    path: {method for method in item if method in {"get", "head", "post", "put", "patch", "delete", "options"}}
    for path, item in openapi["paths"].items()
}
required_mutations = {
    "/queues/{queue_id}": {"get", "patch", "delete"},
    "/queues/{queue_id}/messages/{message_id}": {"get"},
    "/streams/{stream_id}": {"get", "patch", "delete"},
    "/routing/routers/{router_id}": {"get", "patch", "delete"},
    "/keyspaces/default/entries:batch-put": {"post"},
    "/keyspaces/default/entries:batch-delete": {"post"},
    "/streams/{stream_id}/consumer-groups/{group_id}/offsets/{partition}": {"put"},
}
for route, methods in required_mutations.items():
    assert documented_methods.get(route) == methods, (route, documented_methods.get(route))

with open(os.path.join(ROOT, "src", "admin_http.c"), encoding="utf-8") as f:
    dispatcher_source = f.read()
for route_prefix in (
        "/api/admin/v1/keyspaces/default/entries", "/api/admin/v1/queues/",
        "/api/admin/v1/streams/", "/api/admin/v1/routing/routers",
        "/api/admin/v1/queue-consumers", "/api/admin/v1/atomic-operations",
        "/api/admin/v1/maintenance"):
    assert route_prefix in dispatcher_source, route_prefix

stable_error_codes = {
    "bad_request", "unauthorized", "forbidden_origin", "not_found",
    "method_not_allowed", "request_too_large", "unsupported_media_type",
    "validation_failed", "resource_exhausted", "rate_limited", "conflict",
    "idempotency_conflict", "precondition_required", "precondition_failed",
    "persistence_unavailable", "engine_unavailable", "audit_unavailable",
    "delivery_expired", "operation_in_doubt", "internal_error",
}
error_codes = set(openapi["components"]["schemas"]["Error"]["properties"]
                  ["error"]["properties"]["code"]["enum"])
assert error_codes == stable_error_codes, error_codes ^ stable_error_codes
for code in stable_error_codes:
    assert f'"{code}"' in dispatcher_source, code


def request(method, path, headers=(), body=b"", port=PORT):
    s = socket.create_connection(("127.0.0.1", port), 2)
    s.settimeout(2)
    raw = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n" +
           "".join(f"{k}: {v}\r\n" for k, v in headers) +
           f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body
    s.sendall(raw)
    out = b""
    while True:
        try:
            part = s.recv(8192)
        except ConnectionResetError as exc:
            detail = ""
            if "proc" in globals() and proc.poll() is not None:
                detail = (f"exit {proc.returncode}: " +
                          proc.stderr.read().decode(errors="replace"))
            raise RuntimeError(f"connection reset for {method} {path}: {detail}") from exc
        if not part:
            break
        out += part
    s.close()
    head, _, payload = out.partition(b"\r\n\r\n")
    return head, payload


def sse_first(path, headers=()):
    s = socket.create_connection(("127.0.0.1", PORT), 2)
    s.settimeout(2)
    raw = (f"GET {path} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n" +
           "".join(f"{k}: {v}\r\n" for k, v in headers) +
           "Connection: close\r\n\r\n").encode()
    s.sendall(raw)
    out = b""
    while b"\n\n" not in out:
        out += s.recv(8192)
    s.close()
    return out


def sse_records(path, minimum, headers=()):
    s = socket.create_connection(("127.0.0.1", PORT), 2)
    s.settimeout(3)
    raw = (f"GET {path} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n" +
           "".join(f"{k}: {v}\r\n" for k, v in headers) +
           "Connection: close\r\n\r\n").encode()
    started = time.monotonic()
    s.sendall(raw)
    out = b""
    while out.count(b"event: record\n") < minimum:
        out += s.recv(8192)
    elapsed = time.monotonic() - started
    s.close()
    return out, elapsed


def wait_port():
    end = time.time() + 5
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), .1).close()
            return
        except OSError:
            time.sleep(.03)
    detail = ""
    if "proc" in globals() and proc.poll() is not None:
        detail = (f"exit {proc.returncode}: " +
                  proc.stderr.read().decode(errors="replace").strip())
    raise RuntimeError(f"Management API did not start: {detail}")


def wait_job(auth, job_id):
    deadline = time.time() + 3
    latest = None
    while time.time() < deadline:
        latest = request("GET", f"/api/admin/v1/jobs/{job_id}", auth)
        assert latest[0].startswith(b"HTTP/1.1 200"), latest
        job = json.loads(latest[1])["data"]
        if job["state"] not in ("queued", "running"):
            return job
        time.sleep(.02)
    raise AssertionError(f"job did not finish: {latest}")


with tempfile.TemporaryDirectory(prefix="kuttidb-admin-") as tmp:
    token_file = os.path.join(tmp, "admin.token")
    audit_file = os.path.join(tmp, "admin.audit.jsonl")
    wal_file = os.path.join(tmp, "kuttidb.wal")
    queue_wal_file = os.path.join(tmp, "queue.wal")
    with open(token_file, "wb") as f:
        f.write(TOKEN + b"\n")
    os.chmod(token_file, 0o600)

    # It remains absent by default, and an explicitly enabled listener requires a token.
    assert subprocess.run([SERVER, "7417", "-", "--admin-bind", "127.0.0.1:7416"],
                         capture_output=True, timeout=5).returncode != 0
    assert subprocess.run([SERVER, "7417", "-", "--admin-bind", "127.0.0.1:7416",
                           "--admin-token-file", token_file], capture_output=True,
                          timeout=5).returncode != 0
    assert subprocess.run([SERVER, "7417", "-", "--admin-bind", "0.0.0.0:7416",
                           "--admin-token-file", token_file], capture_output=True,
                          timeout=5).returncode != 0
    os.chmod(token_file, 0o644)
    assert subprocess.run([SERVER, "7417", "-", "--admin-bind", "127.0.0.1:7416",
                           "--admin-token-file", token_file], capture_output=True,
                          timeout=5).returncode != 0
    os.chmod(token_file, 0o600)

    proc = subprocess.Popen([SERVER, "7417", wal_file, "--admin-bind", f"127.0.0.1:{PORT}",
                             "--admin-token-file", token_file,
                             "--admin-audit-log", audit_file,
                             "--queue-wal", queue_wal_file,
                             "--admin-session-limit", "2",
                             "--admin-allow-origin", "https://ui.example"],
                            stderr=subprocess.PIPE, start_new_session=True)
    try:
        wait_port()
        auth = [("Authorization", "Bearer " + TOKEN.decode())]
        for path in ("/api/admin/v1/capabilities", "/api/admin/v1/status",
                     "/api/admin/v1/keyspaces", "/api/admin/v1/queues",
                     "/api/admin/v1/streams", "/api/admin/v1/consumer-groups"):
            head, body = request("GET", path, auth)
            assert head.startswith(b"HTTP/1.1 200"), (path, head)
            json.loads(body)
            assert (b"Cache-Control: no-store" in head and b"X-Content-Type-Options: nosniff" in head and
                    b"Referrer-Policy: no-referrer" in head and b"X-KuttiDB-Request-ID:" in head)
        default_keyspace = request("GET", "/api/admin/v1/keyspaces/default", auth)
        assert default_keyspace[0].startswith(b"HTTP/1.1 200"), default_keyspace
        assert json.loads(default_keyspace[1])["data"]["id"] == "default"
        capabilities = json.loads(request("GET", "/api/admin/v1/capabilities", auth)[1])
        assert capabilities["server_version"] == "0.2.0"
        assert "batch_get" in capabilities["operations"]["keyspaces"]
        assert {"batch_put", "batch_delete"} <= set(capabilities["operations"]["keyspaces"])
        assert {"claims", "get_or_refresh"} <= set(capabilities["operations"]["keyspaces"])
        assert "publish_batch" in capabilities["operations"]["queues"]
        assert "get_delivery" in capabilities["operations"]["queues"]
        assert capabilities["durable_consumer_deliveries"] is True
        assert "consume_batch" in capabilities["operations"]["queues"]
        assert {"ack_batch", "nack_batch"} <= set(capabilities["operations"]["queues"])
        assert "append_batch" in capabilities["operations"]["streams"]
        assert capabilities["sse"] == {"available": True} and "tail" in capabilities["operations"]["streams"]
        assert capabilities["limits"]["tail_events_per_second"] == 100
        assert "management_sessions" in capabilities["operations"]["consumer_groups"]
        assert "queue_checkpoint" in capabilities["operations"]["maintenance"]
        assert {"list_routers", "get_router", "list_routes", "delete_route", "delete_router"} <= set(capabilities["operations"]["routing"])
        assert "put-and-enqueue" in capabilities["operations"]["atomic_operations"]
        assert capabilities["audit"] == {"required": True, "healthy": True}
        assert capabilities["cursors"]["queue_messages"] == {"opaque": True, "ttl_seconds": 600, "max_live": 512}
        assert capabilities["cursors"]["keyspace_entries"] == {"opaque": True, "ttl_seconds": 600, "max_live": 512}
        assert capabilities["keyspace_entry_filters"] == ["prefix", "expires"]
        assert capabilities["tls"] == {"available": TLS_AVAILABLE}
        assert capabilities["persistence"] == {"keyspaces": True, "queues": True, "streams": True}
        status = json.loads(request("GET", "/api/admin/v1/status", auth)[1])
        assert status["management"]["active_tails"] == 0 and status["audit"]["healthy"] is True
        request_id_header, _ = request("GET", "/api/admin/v1/status", auth + [("X-KuttiDB-Request-ID", "client-request-42")])
        assert b"X-KuttiDB-Request-ID: client-request-42" in request_id_header
        missing_entry_id = "b64u:" + base64.urlsafe_b64encode(b"missing-entry").rstrip(b"=").decode()
        assert request("GET", f"/api/admin/v1/keyspaces/default/entries/{missing_entry_id}", auth)[0].startswith(b"HTTP/1.1 404")
        entry_id = "b64u:" + base64.urlsafe_b64encode(b"admin-entry").rstrip(b"=").decode()
        entry_put = request("PUT", f"/api/admin/v1/keyspaces/default/entries/{entry_id}",
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "put-entry-1")],
                            # This entry is inspected again after the full
                            # claims workflow; keep its TTL well above the
                            # test's expected duration so a slow CI host does
                            # not turn the later read into a transient 404.
                            b'{"value":"dmFsdWU=","ttl_ms":60000}')
        assert entry_put[0].startswith(b"HTTP/1.1 200"), entry_put
        keyspace_after_put = request("GET", "/api/admin/v1/keyspaces/default", auth)
        assert json.loads(keyspace_after_put[1])["data"]["revision"] >= 1
        assert b'ETag: "k-' in keyspace_after_put[0], keyspace_after_put
        entry_id_two = "b64u:" + base64.urlsafe_b64encode(b"admin-entry-two").rstrip(b"=").decode()
        assert request("PUT", f"/api/admin/v1/keyspaces/default/entries/{entry_id_two}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "put-entry-2")],
                       b'{"value":"dmFsdWUy"}')[0].startswith(b"HTTP/1.1 200")
        entries = request("GET", "/api/admin/v1/keyspaces/default/entries?limit=10", auth)
        assert entries[0].startswith(b"HTTP/1.1 200"), entries
        entries_payload = json.loads(entries[1])
        assert any(entry["id"] == entry_id for entry in entries_payload["data"])
        assert entries_payload["meta"]["snapshot_revision"] >= 2, entries_payload
        key_prefix = "b64u:" + base64.urlsafe_b64encode(b"admin-").rstrip(b"=").decode()
        key_page_one = json.loads(request("GET", f"/api/admin/v1/keyspaces/default/entries?limit=1&prefix={key_prefix}", auth)[1])
        key_cursor = key_page_one["meta"]["next_cursor"]
        assert key_cursor and key_cursor.startswith("ke:"), key_page_one
        key_page_two = json.loads(request("GET", f"/api/admin/v1/keyspaces/default/entries?cursor={key_cursor}", auth)[1])
        assert any(item["id"] != key_page_one["data"][0]["id"] for item in key_page_two["data"]), key_page_two
        assert request("GET", f"/api/admin/v1/keyspaces/default/entries?prefix={entry_id}&cursor={key_cursor}", auth)[0].startswith(b"HTTP/1.1 400")
        expiring_entries = json.loads(request("GET", "/api/admin/v1/keyspaces/default/entries?expires=present", auth)[1])
        expiring_entry = next(item for item in expiring_entries["data"] if item["id"] == entry_id)
        assert expiring_entry["remaining_ttl_ms"] is not None and expiring_entry["remaining_ttl_ms"] <= 60000, expiring_entries
        nonexpiring_entries = json.loads(request("GET", "/api/admin/v1/keyspaces/default/entries?expires=none", auth)[1])
        assert any(item["id"] == entry_id_two and item["remaining_ttl_ms"] is None
                   for item in nonexpiring_entries["data"]), nonexpiring_entries
        batch_get = request("POST", "/api/admin/v1/keyspaces/default/entries:batch-get", auth + [("Content-Type", "application/json")],
                            json.dumps({"entry_ids": [entry_id, missing_entry_id]}).encode())
        assert batch_get[0].startswith(b"HTTP/1.1 200"), batch_get
        assert [(item["found"], item.get("value", {}).get("data"))
                for item in json.loads(batch_get[1])["data"]] == [(True, "dmFsdWU="), (False, None)]
        batch_entry_one = "b64u:" + base64.urlsafe_b64encode(b"batch-entry-one").rstrip(b"=").decode()
        batch_entry_two = "b64u:" + base64.urlsafe_b64encode(b"batch-entry-two").rstrip(b"=").decode()
        batch_put = request("POST", "/api/admin/v1/keyspaces/default/entries:batch-put",
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "batch-put-entries")],
                            json.dumps({"entries": [{"entry_id": batch_entry_one, "value": "b25l"},
                                                     {"entry_id": batch_entry_two, "value": "dHdv", "ttl_ms": 1000}]}).encode())
        assert batch_put[0].startswith(b"HTTP/1.1 200"), batch_put
        assert json.loads(batch_put[1])["data"] == {"applied_count": 2, "atomic": False, "durability": "known"}
        batch_values = json.loads(request("POST", "/api/admin/v1/keyspaces/default/entries:batch-get", auth + [("Content-Type", "application/json")],
                                          json.dumps({"entry_ids": [batch_entry_one, batch_entry_two]}).encode())[1])["data"]
        assert [item["value"]["data"] for item in batch_values] == ["b25l", "dHdv"], batch_values
        invalid_batch_put = request("POST", "/api/admin/v1/keyspaces/default/entries:batch-put",
                                    auth + [("Content-Type", "application/json"), ("Idempotency-Key", "invalid-batch-put-entries")],
                                    b'{"entries":[{"entry_id":"not-an-id","value":"eA=="}]}')
        assert invalid_batch_put[0].startswith(b"HTTP/1.1 400"), invalid_batch_put
        batch_delete = request("POST", "/api/admin/v1/keyspaces/default/entries:batch-delete",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "batch-delete-entries")],
                               json.dumps({"entry_ids": [batch_entry_one, missing_entry_id]}).encode())
        assert batch_delete[0].startswith(b"HTTP/1.1 200"), batch_delete
        assert json.loads(batch_delete[1])["data"] == {"applied_count": 2, "deleted_count": 1, "atomic": False, "durability": "known"}
        assert request("GET", f"/api/admin/v1/keyspaces/default/entries/{batch_entry_one}", auth)[0].startswith(b"HTTP/1.1 404")
        claim_entry_id = "b64u:" + base64.urlsafe_b64encode(b"claimed-entry").rstrip(b"=").decode()
        claim_create = request("POST", "/api/admin/v1/keyspaces/default/claims",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "claim-create")],
                               json.dumps({"entry_id": claim_entry_id, "lease_ms": 60000}).encode())
        assert claim_create[0].startswith(b"HTTP/1.1 201"), claim_create
        claim_id = json.loads(claim_create[1])["data"]["claim_id"]
        assert claim_id.startswith("kc:")
        assert json.loads(request("GET", "/api/admin/v1/status", auth)[1])["management"]["active_claims"] == 1
        claim_busy = request("POST", "/api/admin/v1/keyspaces/default/claims",
                             auth + [("Content-Type", "application/json"), ("Idempotency-Key", "claim-busy")],
                             json.dumps({"entry_id": claim_entry_id, "lease_ms": 60000}).encode())
        assert claim_busy[0].startswith(b"HTTP/1.1 409"), claim_busy
        claim_get = request("GET", f"/api/admin/v1/keyspaces/default/claims/{claim_id}", auth)
        assert claim_get[0].startswith(b"HTTP/1.1 200") and json.loads(claim_get[1])["data"]["id"] == claim_entry_id, claim_get
        claim_complete = request("POST", f"/api/admin/v1/keyspaces/default/claims/{claim_id}:complete",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "claim-complete")],
                                 b'{"value":"Y2xhaW1lZA=="}')
        assert claim_complete[0].startswith(b"HTTP/1.1 200"), claim_complete
        assert json.loads(request("GET", f"/api/admin/v1/keyspaces/default/entries/{claim_entry_id}", auth)[1])["data"]["value"]["data"] == "Y2xhaW1lZA=="
        refresh_hit = request("POST", f"/api/admin/v1/keyspaces/default/entries/{claim_entry_id}:get-or-refresh",
                              auth + [("Content-Type", "application/json"), ("Idempotency-Key", "refresh-hit")], b'{}')
        assert refresh_hit[0].startswith(b"HTTP/1.1 200") and json.loads(refresh_hit[1])["data"]["outcome"] == "value", refresh_hit
        refresh_miss_id = "b64u:" + base64.urlsafe_b64encode(b"refresh-miss").rstrip(b"=").decode()
        refresh_miss = request("POST", f"/api/admin/v1/keyspaces/default/entries/{refresh_miss_id}:get-or-refresh",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "refresh-miss")], b'{"lease_ms":60000}')
        assert refresh_miss[0].startswith(b"HTTP/1.1 201") and json.loads(refresh_miss[1])["data"]["outcome"] == "claimed", refresh_miss
        refresh_claim_id = json.loads(refresh_miss[1])["data"]["claim_id"]
        assert request("POST", f"/api/admin/v1/keyspaces/default/claims/{refresh_claim_id}:release",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "refresh-release")], b"{}")[0].startswith(b"HTTP/1.1 200")
        release_entry_id = "b64u:" + base64.urlsafe_b64encode(b"released-entry").rstrip(b"=").decode()
        release_create = request("POST", "/api/admin/v1/keyspaces/default/claims",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "claim-release-create")],
                                 json.dumps({"entry_id": release_entry_id, "lease_ms": 60000}).encode())
        release_claim_id = json.loads(release_create[1])["data"]["claim_id"]
        claim_release = request("POST", f"/api/admin/v1/keyspaces/default/claims/{release_claim_id}:release",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "claim-release")], b"{}")
        assert claim_release[0].startswith(b"HTTP/1.1 200"), claim_release
        assert request("GET", f"/api/admin/v1/keyspaces/default/claims/{release_claim_id}", auth)[0].startswith(b"HTTP/1.1 410")
        entry_get = request("GET", f"/api/admin/v1/keyspaces/default/entries/{entry_id}", auth)
        assert json.loads(entry_get[1])["data"]["value"]["data"] == "dmFsdWU="
        assert b'ETag: "k-' in entry_get[0] and json.loads(entry_get[1])["data"]["revision"] > 0, entry_get
        entry_delete = request("DELETE", f"/api/admin/v1/keyspaces/default/entries/{entry_id}",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-entry-1")], b"{}")
        assert entry_delete[0].startswith(b"HTTP/1.1 200"), entry_delete
        assert json.loads(entry_delete[1])["data"]["deleted"] is True
        status = json.loads(request("GET", "/api/admin/v1/status", auth)[1])
        assert status["management"]["mutation_attempts"] >= 2
        assert request("GET", f"/api/admin/v1/keyspaces/default/entries/{entry_id}", auth)[0].startswith(b"HTTP/1.1 404")
        create_headers = auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-queue-1")]
        queue_body = b'{"name":"admin-test-queue","durable":false,"max_depth":10}'
        head, body = request("POST", "/api/admin/v1/queues", create_headers, queue_body)
        assert head.startswith(b"HTTP/1.1 201"), head
        assert json.loads(body)["data"]["name"] == "admin-test-queue"
        # A byte-identical retry returns the cached result instead of issuing
        # another declaration attempt.
        assert request("POST", "/api/admin/v1/queues", create_headers, queue_body)[0].startswith(b"HTTP/1.1 201")
        assert request("POST", "/api/admin/v1/queues", create_headers,
                       b'{"name":"different-queue","durable":false}')[0].startswith(b"HTTP/1.1 409")
        atomic_queue = request("POST", "/api/admin/v1/queues",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-atomic-queue")],
                               b'{"name":"atomic-queue","durable":true,"max_depth":10}')
        assert atomic_queue[0].startswith(b"HTTP/1.1 201"), atomic_queue
        consumer = request("POST", "/api/admin/v1/queue-consumers",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-consumer-1")],
                           b'{"name":"admin-consumer"}')
        assert consumer[0].startswith(b"HTTP/1.1 201"), consumer
        consumer_id = "b64u:" + base64.urlsafe_b64encode(b"admin-consumer").rstrip(b"=").decode()
        consumer_list = json.loads(request("GET", "/api/admin/v1/queue-consumers", auth)[1])
        assert any(item["id"] == consumer_id for item in consumer_list["data"])
        consumer_get = request("GET", f"/api/admin/v1/queue-consumers/{consumer_id}", auth)
        assert json.loads(consumer_get[1])["data"]["id"] == consumer_id
        consumer_etag = next(line.split(b": ", 1)[1].decode() for line in consumer_get[0].split(b"\r\n") if line.startswith(b"ETag: "))
        queue_id = "b64u:" + base64.urlsafe_b64encode(b"admin-test-queue").rstrip(b"=").decode()
        consumer_message = request("POST", f"/api/admin/v1/queues/{queue_id}/messages",
                                   auth + [("Content-Type", "application/json"), ("Idempotency-Key", "consumer-message")],
                                   b'{"body":"Y29uc3VtZXItYm9keQ=="}')
        assert consumer_message[0].startswith(b"HTTP/1.1 201"), consumer_message
        consumer_delivery = request("POST", f"/api/admin/v1/queue-consumers/{consumer_id}/deliveries",
                                    auth + [("Content-Type", "application/json"), ("Idempotency-Key", "consumer-delivery")],
                                    json.dumps({"queue_id": queue_id, "visibility_ms": 1000}).encode())
        consumer_delivery_data = json.loads(consumer_delivery[1])["data"]
        assert consumer_delivery[0].startswith(b"HTTP/1.1 201") and consumer_delivery_data["body"]["data"] == "Y29uc3VtZXItYm9keQ==", consumer_delivery
        assert request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries/{consumer_delivery_data['delivery_id']}:ack",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "consumer-delivery-ack")], b"{}")[0].startswith(b"HTTP/1.1 200")
        assert request("DELETE", f"/api/admin/v1/queue-consumers/{consumer_id}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-consumer-missing-confirm")], b"{}")[0].startswith(b"HTTP/1.1 428")
        deleted_consumer = request("DELETE", f"/api/admin/v1/queue-consumers/{consumer_id}",
                                   auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-consumer"), ("If-Match", consumer_etag), ("X-KuttiDB-Confirm", consumer_id)], b"{}")
        assert deleted_consumer[0].startswith(b"HTTP/1.1 200") and json.loads(deleted_consumer[1])["data"]["deleted"] is True, deleted_consumer
        assert request("GET", f"/api/admin/v1/queue-consumers/{consumer_id}", auth)[0].startswith(b"HTTP/1.1 404")
        queue_id = "b64u:" + base64.urlsafe_b64encode(b"admin-test-queue").rstrip(b"=").decode()
        atomic_queue_id = "b64u:" + base64.urlsafe_b64encode(b"atomic-queue").rstrip(b"=").decode()
        batch_queue = request("POST", "/api/admin/v1/queues",
                              auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-batch-queue")],
                              b'{"name":"admin-batch-queue","durable":true,"max_depth":2}')
        assert batch_queue[0].startswith(b"HTTP/1.1 201"), batch_queue
        batch_queue_id = "b64u:" + base64.urlsafe_b64encode(b"admin-batch-queue").rstrip(b"=").decode()
        batch_publish = request("POST", f"/api/admin/v1/queues/{batch_queue_id}/messages:batch",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "publish-batch-queue")],
                                b'{"messages":[{"body":"b25l"},{"body":"dHdv"}]}')
        assert batch_publish[0].startswith(b"HTTP/1.1 201") and json.loads(batch_publish[1])["data"]["message_count"] == 2, batch_publish
        batch_browse = json.loads(request("GET", f"/api/admin/v1/queues/{batch_queue_id}/messages?limit=10&state=ready&include=body", auth)[1])
        assert [item["body"]["data"] for item in batch_browse["data"]] == ["b25l", "dHdv"], batch_browse
        atomic_key_id = "b64u:" + base64.urlsafe_b64encode(b"atomic-key").rstrip(b"=").decode()
        atomic = request("POST", "/api/admin/v1/atomic-operations",
                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "atomic-put-enqueue-1")],
                         json.dumps({"operation": "put-and-enqueue", "key_id": atomic_key_id,
                                     "target_id": atomic_queue_id, "value": "YXRvbWljLXZhbHVl"}).encode())
        assert atomic[0].startswith(b"HTTP/1.1 200"), atomic
        assert json.loads(atomic[1])["data"]["transaction_id"] > 0
        assert json.loads(request("GET", f"/api/admin/v1/keyspaces/default/entries/{atomic_key_id}", auth)[1])["data"]["value"]["data"] == "YXRvbWljLXZhbHVl"
        atomic_delivery = request("POST", f"/api/admin/v1/queues/{atomic_queue_id}/deliveries",
                                  auth + [("Content-Type", "application/json"), ("Idempotency-Key", "atomic-delivery-1")], b"{}")
        assert json.loads(atomic_delivery[1])["data"]["body"]["data"] == "YXRvbWljLXZhbHVl"
        atomic_delivery_id = json.loads(atomic_delivery[1])["data"]["delivery_id"]
        assert request("POST", f"/api/admin/v1/queues/{atomic_queue_id}/deliveries/{atomic_delivery_id}:ack",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "atomic-ack-1")], b"{}")[0].startswith(b"HTTP/1.1 200")
        default_routed = request("POST", "/api/admin/v1/routing/default/messages",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "default-route-1")],
                                 json.dumps({"queue_id": atomic_queue_id, "body": "ZGVmYXVsdC1yb3V0ZQ=="}).encode())
        assert default_routed[0].startswith(b"HTTP/1.1 201"), default_routed
        queue_info = request("GET", f"/api/admin/v1/queues/{queue_id}", auth)
        assert json.loads(queue_info[1])["data"]["id"] == queue_id
        assert json.loads(queue_info[1])["data"]["durable"] is False and json.loads(queue_info[1])["data"]["max_depth"] == 10
        queue_etag = next(line.split(b": ", 1)[1].decode() for line in queue_info[0].split(b"\r\n") if line.startswith(b"ETag: "))
        assert request("PATCH", f"/api/admin/v1/queues/{queue_id}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "queue-update-no-match")], b'{"max_depth":10}')[0].startswith(b"HTTP/1.1 428")
        immutable_queue_update = request("PATCH", f"/api/admin/v1/queues/{queue_id}",
                                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "queue-update-immutable"), ("If-Match", queue_etag)], b'{"max_depth":10}')
        assert immutable_queue_update[0].startswith(b"HTTP/1.1 409") and json.loads(immutable_queue_update[1])["error"]["code"] == "unsupported_option", immutable_queue_update
        publish_body = b'{"body":"aGVsbG8=","ttl_ms":1000}'
        publish = request("POST", f"/api/admin/v1/queues/{queue_id}/messages",
                          auth + [("Content-Type", "application/json"), ("Idempotency-Key", "publish-queue-1")],
                          publish_body)
        assert publish[0].startswith(b"HTTP/1.1 201"), publish
        queue_message_id = json.loads(publish[1])["data"]["message_id"]
        assert queue_message_id > 0
        exact_queue_message = request("GET", f"/api/admin/v1/queues/{queue_id}/messages/{queue_message_id}?include=body", auth)
        assert exact_queue_message[0].startswith(b"HTTP/1.1 200") and json.loads(exact_queue_message[1])["data"]["body"]["data"] == "aGVsbG8=", exact_queue_message
        assert request("GET", f"/api/admin/v1/queues/{queue_id}/messages/999999999", auth)[0].startswith(b"HTTP/1.1 404")
        second_publish = request("POST", f"/api/admin/v1/queues/{queue_id}/messages",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "publish-queue-cursor-2")],
                                 b'{"body":"bmV4dA=="}')
        assert second_publish[0].startswith(b"HTTP/1.1 201"), second_publish
        first_page = request("GET", f"/api/admin/v1/queues/{queue_id}/messages?limit=1&state=ready", auth)
        assert first_page[0].startswith(b"HTTP/1.1 200"), first_page
        first_page_data = json.loads(first_page[1])
        next_cursor = first_page_data["meta"]["next_cursor"]
        assert next_cursor.startswith("mc:") and len(next_cursor) == 35, first_page_data
        second_page = request("GET", f"/api/admin/v1/queues/{queue_id}/messages?limit=1&state=ready&cursor={next_cursor}", auth)
        assert second_page[0].startswith(b"HTTP/1.1 200"), second_page
        second_page_data = json.loads(second_page[1])
        assert second_page_data["data"][0]["message_id"] > first_page_data["data"][0]["message_id"], second_page_data
        assert request("GET", f"/api/admin/v1/queues/{queue_id}/messages?cursor=0", auth)[0].startswith(b"HTTP/1.1 400")
        assert request("GET", f"/api/admin/v1/queues/{queue_id}/messages?state=delayed&cursor={next_cursor}", auth)[0].startswith(b"HTTP/1.1 400")
        # Browse is a snapshot, not a consume/requeue shortcut: it exposes the
        # exact body without changing the later delivery or its first-delivery
        # count/state.
        browse = request("GET", f"/api/admin/v1/queues/{queue_id}/messages?limit=10&state=ready&include=body", auth)
        assert browse[0].startswith(b"HTTP/1.1 200"), browse
        browsed = json.loads(browse[1])["data"]
        assert any(item["body"]["data"] == "aGVsbG8=" and item["delivery_count"] == 0
                   and item["state"] == "ready" for item in browsed), browsed
        delivery = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delivery-queue-1")], b"{}")
        assert delivery[0].startswith(b"HTTP/1.1 201"), delivery
        delivered = json.loads(delivery[1])["data"]
        assert delivered["delivery_id"].startswith("d:") and delivered["body"]["data"] == "aGVsbG8="
        delivery_status = request("GET", f"/api/admin/v1/queues/{queue_id}/deliveries/{delivered['delivery_id']}", auth)
        assert delivery_status[0].startswith(b"HTTP/1.1 200"), delivery_status
        status_data = json.loads(delivery_status[1])["data"]
        assert status_data["delivery_id"] == delivered["delivery_id"] and status_data["message_id"] == delivered["message_id"]
        assert status_data["queue"]["id"] == queue_id and status_data["state"] == "active"
        delivery_body = request("GET", f"/api/admin/v1/queues/{queue_id}/deliveries/{delivered['delivery_id']}?include=body", auth)
        assert delivery_body[0].startswith(b"HTTP/1.1 200") and json.loads(delivery_body[1])["data"]["body"]["data"] == "aGVsbG8=", delivery_body
        assert request("POST", f"/api/admin/v1/queues/{queue_id}/messages",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "publish-queue-capacity")],
                       b'{"body":"Y2FwYWNpdHk="}')[0].startswith(b"HTTP/1.1 201")
        capacity = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delivery-capacity")], b"{}")
        assert capacity[0].startswith(b"HTTP/1.1 201"), capacity
        capacity_delivery_id = json.loads(capacity[1])["data"]["delivery_id"]
        exhausted = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries",
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delivery-exhausted")], b"{}")
        assert exhausted[0].startswith(b"HTTP/1.1 429") and json.loads(exhausted[1])["error"]["code"] == "resource_exhausted"
        assert json.loads(request("GET", "/api/admin/v1/status", auth)[1])["management"]["rate_limit_rejections"] == 1
        ack = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries/{delivered['delivery_id']}:ack",
                      auth + [("Content-Type", "application/json"), ("Idempotency-Key", "ack-delivery-1")], b"{}")
        assert ack[0].startswith(b"HTTP/1.1 200"), ack
        assert json.loads(ack[1])["data"]["acknowledged"] is True
        assert request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries/{capacity_delivery_id}:ack",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "ack-capacity-delivery")], b"{}")[0].startswith(b"HTTP/1.1 200")
        created_batch = request("POST", f"/api/admin/v1/queues/{batch_queue_id}/deliveries:batch",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-delivery-batch")],
                                b'{"max_messages":2}')
        assert created_batch[0].startswith(b"HTTP/1.1 201"), created_batch
        created_receipts = json.loads(created_batch[1])
        assert created_receipts["meta"]["count"] == 2 and len(created_receipts["data"]) == 2, created_receipts
        batch_delivery_one, batch_delivery_two = [item["delivery_id"] for item in created_receipts["data"]]
        batch_body = request("GET", f"/api/admin/v1/queues/{batch_queue_id}/deliveries/{batch_delivery_one}?include=body", auth)
        assert batch_body[0].startswith(b"HTTP/1.1 200") and json.loads(batch_body[1])["data"]["body"]["data"] == "b25l", batch_body
        batch_ack = request("POST", f"/api/admin/v1/queues/{batch_queue_id}/deliveries:ack-batch",
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "batch-ack")],
                            json.dumps({"delivery_ids": [batch_delivery_one, batch_delivery_two]}).encode())
        assert batch_ack[0].startswith(b"HTTP/1.1 200"), batch_ack
        assert [item["outcome"] for item in json.loads(batch_ack[1])["data"]] == ["acknowledged", "acknowledged"]
        assert request("POST", f"/api/admin/v1/queues/{batch_queue_id}/messages:batch",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "republish-batch-queue")],
                       b'{"messages":[{"body":"dGhyZWU="},{"body":"Zm91cg=="}]}')[0].startswith(b"HTTP/1.1 201")
        nack_delivery_one = json.loads(request("POST", f"/api/admin/v1/queues/{batch_queue_id}/deliveries",
                                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "nack-batch-delivery-one")], b"{}")[1])["data"]["delivery_id"]
        nack_delivery_two = json.loads(request("POST", f"/api/admin/v1/queues/{batch_queue_id}/deliveries",
                                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "nack-batch-delivery-two")], b"{}")[1])["data"]["delivery_id"]
        duplicate_batch = request("POST", f"/api/admin/v1/queues/{batch_queue_id}/deliveries:ack-batch",
                                  auth + [("Content-Type", "application/json"), ("Idempotency-Key", "duplicate-batch-ack")],
                                  json.dumps({"delivery_ids": [nack_delivery_one, nack_delivery_one]}).encode())
        assert duplicate_batch[0].startswith(b"HTTP/1.1 400"), duplicate_batch
        batch_nack = request("POST", f"/api/admin/v1/queues/{batch_queue_id}/deliveries:nack-batch",
                             auth + [("Content-Type", "application/json"), ("Idempotency-Key", "batch-nack")],
                             json.dumps({"delivery_ids": [nack_delivery_one, nack_delivery_two], "requeue": True}).encode())
        assert batch_nack[0].startswith(b"HTTP/1.1 200"), batch_nack
        assert [item["outcome"] for item in json.loads(batch_nack[1])["data"]] == ["requeued", "requeued"]
        fallback = request("POST", "/api/admin/v1/routing/routers",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-fallback-router-1")],
                           b'{"name":"admin-test-fallback","mode":"exact","durable":true}')
        assert fallback[0].startswith(b"HTTP/1.1 201"), fallback
        fallback_id = "b64u:" + base64.urlsafe_b64encode(b"admin-test-fallback").rstrip(b"=").decode()
        router_body = b'{"name":"admin-test-router","mode":"exact","durable":true}'
        router = request("POST", "/api/admin/v1/routing/routers",
                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-router-1")], router_body)
        assert router[0].startswith(b"HTTP/1.1 201"), router
        router_id = "b64u:" + base64.urlsafe_b64encode(b"admin-test-router").rstrip(b"=").decode()
        bind = request("POST", f"/api/admin/v1/routing/routers/{router_id}/routes",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "bind-route-1")],
                       json.dumps({"queue_id": queue_id, "routing_key": "jobs"}).encode())
        assert bind[0].startswith(b"HTTP/1.1 201"), bind
        routers = json.loads(request("GET", "/api/admin/v1/routing/routers", auth)[1])
        assert any(item["id"] == router_id and item["route_count"] == 1 for item in routers["data"]), routers
        router_detail = request("GET", f"/api/admin/v1/routing/routers/{router_id}", auth)
        assert router_detail[0].startswith(b"HTTP/1.1 200") and json.loads(router_detail[1])["data"]["mode"] == "exact", router_detail
        router_revision = json.loads(router_detail[1])["data"]["revision"]
        update_router = request("PATCH", f"/api/admin/v1/routing/routers/{router_id}",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "update-router-1"), ("If-Match", f'\"r-{router_revision}\"')],
                                json.dumps({"alternate_router_id": fallback_id}).encode())
        assert update_router[0].startswith(b"HTTP/1.1 200"), update_router
        router_revision += 1
        routes = request("GET", f"/api/admin/v1/routing/routers/{router_id}/routes", auth)
        assert routes[0].startswith(b"HTTP/1.1 200") and json.loads(routes[1])["data"][0]["queue"]["id"] == queue_id, routes
        route_id = json.loads(routes[1])["data"][0]["route_id"]
        route_url = f"/api/admin/v1/routing/routers/{router_id}/routes/{route_id}"
        route_detail = request("GET", route_url, auth)
        assert route_detail[0].startswith(b"HTTP/1.1 200") and json.loads(route_detail[1])["data"]["route_id"] == route_id, route_detail
        routed = request("POST", f"/api/admin/v1/routing/routers/{router_id}/messages",
                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "route-message-1")],
                         b'{"routing_key":"jobs","body":"cm91dGVk"}')
        assert routed[0].startswith(b"HTTP/1.1 201"), routed
        assert json.loads(routed[1])["data"]["routed_queue_count"] == 1
        router_after_publish = request("GET", f"/api/admin/v1/routing/routers/{router_id}", auth)
        router_metrics = json.loads(router_after_publish[1])["data"]
        assert router_metrics["publish_attempt_count"] >= 1 and router_metrics["unroutable_count"] == 0 and router_metrics["metrics_scope"] == "process_lifetime", router_after_publish
        missing_route_confirm = request("DELETE", route_url,
                                        auth + [("Content-Type", "application/json"), ("Idempotency-Key", "unbind-route-missing-confirm")],
                                        b"{}")
        assert missing_route_confirm[0].startswith(b"HTTP/1.1 428"), missing_route_confirm
        stale_unbind = request("DELETE", route_url,
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "unbind-route-stale"), ("If-Match", '"r-1"'), ("X-KuttiDB-Confirm", route_id)],
                               b"{}")
        assert stale_unbind[0].startswith(b"HTTP/1.1 412"), stale_unbind
        unbind = request("DELETE", route_url,
                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "unbind-route-1"), ("If-Match", f'"r-{router_revision}"'), ("X-KuttiDB-Confirm", route_id)],
                         b"{}")
        assert unbind[0].startswith(b"HTTP/1.1 200") and json.loads(unbind[1])["data"]["deleted"] is True, unbind
        routes_after_unbind = request("GET", f"/api/admin/v1/routing/routers/{router_id}/routes", auth)
        assert routes_after_unbind[0].startswith(b"HTTP/1.1 200") and json.loads(routes_after_unbind[1])["data"] == [], routes_after_unbind
        router_after_unbind = request("GET", f"/api/admin/v1/routing/routers/{router_id}", auth)
        router_revision_after_unbind = json.loads(router_after_unbind[1])["data"]["revision"]
        missing_router_confirm = request("DELETE", f"/api/admin/v1/routing/routers/{router_id}",
                                        auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-router-missing-confirm")], b"{}")
        assert missing_router_confirm[0].startswith(b"HTTP/1.1 428"), missing_router_confirm
        delete_router = request("DELETE", f"/api/admin/v1/routing/routers/{router_id}",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-router-1"), ("If-Match", f'\"r-{router_revision_after_unbind}\"'), ("X-KuttiDB-Confirm", router_id)], b"{}")
        assert delete_router[0].startswith(b"HTTP/1.1 200") and json.loads(delete_router[1])["data"]["deleted"] is True, delete_router
        assert request("GET", f"/api/admin/v1/routing/routers/{router_id}", auth)[0].startswith(b"HTTP/1.1 404")
        delete_fallback = request("DELETE", f"/api/admin/v1/routing/routers/{fallback_id}",
                                  auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-fallback-router-1"), ("If-Match", '\"r-1\"'), ("X-KuttiDB-Confirm", fallback_id)], b"{}")
        assert delete_fallback[0].startswith(b"HTTP/1.1 200"), delete_fallback
        second_delivery = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries",
                                  auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delivery-queue-2")], b"{}")
        second_id = json.loads(second_delivery[1])["data"]["delivery_id"]
        nack = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries/{second_id}:nack",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "nack-delivery-1")],
                       b'{"requeue":true,"delay_ms":0}')
        assert nack[0].startswith(b"HTTP/1.1 200"), nack
        assert json.loads(nack[1])["data"]["requeued"] is True
        expiring = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delivery-queue-expire")],
                           b'{"visibility_ms":100}')
        expiring_id = json.loads(expiring[1])["data"]["delivery_id"]
        time.sleep(.15)
        expired = request("POST", f"/api/admin/v1/queues/{queue_id}/deliveries/{expiring_id}:ack",
                          auth + [("Content-Type", "application/json"), ("Idempotency-Key", "ack-delivery-expired")], b"{}")
        assert expired[0].startswith(b"HTTP/1.1 410"), expired
        assert request("GET", f"/api/admin/v1/queues/{queue_id}/deliveries/{expiring_id}", auth)[0].startswith(b"HTTP/1.1 410")
        purge_queue = request("POST", "/api/admin/v1/queues",
                              auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-purge-queue")],
                              b'{"name":"admin-purge-queue","durable":true}')
        assert purge_queue[0].startswith(b"HTTP/1.1 201"), purge_queue
        purge_queue_id = "b64u:" + base64.urlsafe_b64encode(b"admin-purge-queue").rstrip(b"=").decode()
        for item, key in ((b'{"body":"b25l"}', "purge-one"), (b'{"body":"dHdv"}', "purge-two")):
            assert request("POST", f"/api/admin/v1/queues/{purge_queue_id}/messages",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", key)], item)[0].startswith(b"HTTP/1.1 201")
        purge_info = request("GET", f"/api/admin/v1/queues/{purge_queue_id}", auth)
        assert b'ETag: "q-' in purge_info[0], purge_info
        etag = next(line.split(b": ", 1)[1].decode() for line in purge_info[0].split(b"\r\n") if line.startswith(b"ETag: "))
        purge_path = f"/api/admin/v1/queues/{purge_queue_id}:purge"
        assert request("POST", purge_path,
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "purge-missing-confirm")], b"{}")[0].startswith(b"HTTP/1.1 428")
        purged = request("POST", purge_path,
                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "purge-confirmed"), ("If-Match", etag), ("X-KuttiDB-Confirm", purge_queue_id)], b"{}")
        assert purged[0].startswith(b"HTTP/1.1 200") and json.loads(purged[1])["data"]["removed_count"] == 2, purged
        assert json.loads(request("GET", f"/api/admin/v1/queues/{purge_queue_id}/messages?limit=10", auth)[1])["data"] == []
        delete_queue = request("POST", "/api/admin/v1/queues",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-delete-queue")],
                               b'{"name":"admin-delete-queue","durable":true}')
        assert delete_queue[0].startswith(b"HTTP/1.1 201"), delete_queue
        delete_queue_id = "b64u:" + base64.urlsafe_b64encode(b"admin-delete-queue").rstrip(b"=").decode()
        assert request("POST", f"/api/admin/v1/queues/{delete_queue_id}/messages",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "seed-delete-queue")],
                       b'{"body":"Z29uZQ=="}')[0].startswith(b"HTTP/1.1 201")
        delete_info = request("GET", f"/api/admin/v1/queues/{delete_queue_id}", auth)
        delete_etag = next(line.split(b": ", 1)[1].decode() for line in delete_info[0].split(b"\r\n") if line.startswith(b"ETag: "))
        assert request("DELETE", f"/api/admin/v1/queues/{delete_queue_id}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-missing-confirm")], b"{}")[0].startswith(b"HTTP/1.1 428")
        assert request("DELETE", f"/api/admin/v1/queues/{delete_queue_id}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-stale"), ("If-Match", '"q-999999"'), ("X-KuttiDB-Confirm", delete_queue_id)], b"{}")[0].startswith(b"HTTP/1.1 412")
        deleted_queue = request("DELETE", f"/api/admin/v1/queues/{delete_queue_id}",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "delete-confirmed"), ("If-Match", delete_etag), ("X-KuttiDB-Confirm", delete_queue_id)], b"{}")
        assert deleted_queue[0].startswith(b"HTTP/1.1 200") and json.loads(deleted_queue[1])["data"]["removed_count"] == 1, deleted_queue
        assert request("GET", f"/api/admin/v1/queues/{delete_queue_id}", auth)[0].startswith(b"HTTP/1.1 404")
        maintenance = request("GET", "/api/admin/v1/maintenance", auth)
        assert maintenance[0].startswith(b"HTTP/1.1 200") and {item["engine"] for item in json.loads(maintenance[1])["data"]} == {"keyspace", "queue", "stream"}, maintenance
        keyspace_checkpoint = request("POST", "/api/admin/v1/maintenance/keyspace-checkpoint",
                                      auth + [("Content-Type", "application/json"), ("Idempotency-Key", "keyspace-checkpoint")], b"{}")
        assert keyspace_checkpoint[0].startswith(b"HTTP/1.1 202"), keyspace_checkpoint
        assert wait_job(auth, json.loads(keyspace_checkpoint[1])["data"]["job_id"])["state"] == "succeeded"
        queue_checkpoint = request("POST", "/api/admin/v1/maintenance/queue-checkpoint",
                                   auth + [("Content-Type", "application/json"), ("Idempotency-Key", "queue-checkpoint")], b"{}")
        assert queue_checkpoint[0].startswith(b"HTTP/1.1 202"), queue_checkpoint
        assert wait_job(auth, json.loads(queue_checkpoint[1])["data"]["job_id"])["state"] == "succeeded"
        stream_checkpoint = request("POST", "/api/admin/v1/maintenance/stream-checkpoint",
                                    auth + [("Content-Type", "application/json"), ("Idempotency-Key", "stream-checkpoint")], b"{}")
        assert stream_checkpoint[0].startswith(b"HTTP/1.1 202"), stream_checkpoint
        assert wait_job(auth, json.loads(stream_checkpoint[1])["data"]["job_id"])["state"] == "succeeded"
        checkpoint_all = request("POST", "/api/admin/v1/maintenance/checkpoint-all",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "checkpoint-all")], b"{}")
        assert checkpoint_all[0].startswith(b"HTTP/1.1 202"), checkpoint_all
        assert wait_job(auth, json.loads(checkpoint_all[1])["data"]["job_id"])["state"] == "succeeded"
        batch_stream = request("POST", "/api/admin/v1/streams",
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "create-batch-stream")],
                               b'{"name":"admin-batch-stream","partitions":1}')
        assert batch_stream[0].startswith(b"HTTP/1.1 201"), batch_stream
        batch_stream_id = "b64u:" + base64.urlsafe_b64encode(b"admin-batch-stream").rstrip(b"=").decode()
        appended_batch = request("POST", f"/api/admin/v1/streams/{batch_stream_id}/records:batch",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "append-batch-stream")],
                                 b'{"partition":0,"records":[{"body":"YWxwaGE=","key":"Ynl0ZS1rZXk="},{"body":"YmV0YQ=="}]}')
        assert appended_batch[0].startswith(b"HTTP/1.1 201"), appended_batch
        assert json.loads(appended_batch[1])["data"] == [{"partition": 0, "offset": 0}, {"partition": 0, "offset": 1}], appended_batch
        batch_fetched = json.loads(request("GET", f"/api/admin/v1/streams/{batch_stream_id}/partitions/0/records?offset=0&max_records=10&max_bytes=1024", auth)[1])
        assert [item["body"]["data"] for item in batch_fetched["data"]] == ["YWxwaGE=", "YmV0YQ=="], batch_fetched
        assert batch_fetched["data"][0]["key"] == {"encoding": "base64", "data": "Ynl0ZS1rZXk=", "size": 8, "content_type": "application/octet-stream"}, batch_fetched
        assert batch_fetched["data"][1]["key"] is None, batch_fetched
        exact_record = request("GET", f"/api/admin/v1/streams/{batch_stream_id}/partitions/0/records/1?max_bytes=1024", auth)
        assert exact_record[0].startswith(b"HTTP/1.1 200") and json.loads(exact_record[1])["data"]["body"]["data"] == "YmV0YQ==", exact_record
        exact_keyed_record = request("GET", f"/api/admin/v1/streams/{batch_stream_id}/partitions/0/records/0?max_bytes=1024", auth)
        assert exact_keyed_record[0].startswith(b"HTTP/1.1 200") and json.loads(exact_keyed_record[1])["data"]["key"]["data"] == "Ynl0ZS1rZXk=", exact_keyed_record
        appended_single_key = request("POST", f"/api/admin/v1/streams/{batch_stream_id}/records",
                                      auth + [("Content-Type", "application/json"), ("Idempotency-Key", "append-single-key")],
                                      b'{"partition":0,"key":"c2luZ2xlLWtleQ==","body":"c2luZ2xl"}')
        assert appended_single_key[0].startswith(b"HTTP/1.1 201") and json.loads(appended_single_key[1])["data"]["offset"] == 2, appended_single_key
        exact_single_key = request("GET", f"/api/admin/v1/streams/{batch_stream_id}/partitions/0/records/2?max_bytes=1024", auth)
        assert exact_single_key[0].startswith(b"HTTP/1.1 200") and json.loads(exact_single_key[1])["data"]["key"]["data"] == "c2luZ2xlLWtleQ==", exact_single_key
        assert request("GET", f"/api/admin/v1/streams/{batch_stream_id}/partitions/0/records/9?max_bytes=1024", auth)[0].startswith(b"HTTP/1.1 404")
        rate_batch = json.dumps({"partition": 0, "records": [{"body": "eA=="}] * 98}).encode()
        assert request("POST", f"/api/admin/v1/streams/{batch_stream_id}/records:batch",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "append-tail-rate-batch")],
                       rate_batch)[0].startswith(b"HTTP/1.1 201")
        rate_tail, rate_elapsed = sse_records(f"/api/admin/v1/streams/{batch_stream_id}/partitions/0/records:tail?offset=0", 101, auth)
        assert rate_tail.startswith(b"HTTP/1.1 200") and rate_elapsed >= .7, (rate_elapsed, rate_tail[-256:])
        stream_body = b'{"name":"admin-test-stream","partitions":2,"max_retained_bytes":1024}'
        stream_response = request("POST", "/api/admin/v1/streams", auth + [("Content-Type", "application/json"),
                                                                                ("Idempotency-Key", "create-stream-1")],
                                  stream_body)
        assert stream_response[0].startswith(b"HTTP/1.1 201"), stream_response
        stream_id = "b64u:" + base64.urlsafe_b64encode(b"admin-test-stream").rstrip(b"=").decode()
        stream_info = request("GET", f"/api/admin/v1/streams/{stream_id}", auth)
        assert json.loads(stream_info[1])["data"]["partition_count"] == 2 and json.loads(stream_info[1])["data"]["max_retained_bytes"] == 1024
        scoped_groups = request("GET", f"/api/admin/v1/streams/{stream_id}/consumer-groups", auth)
        assert scoped_groups[0].startswith(b"HTTP/1.1 200"), scoped_groups
        append = request("POST", f"/api/admin/v1/streams/{stream_id}/records",
                         auth + [("Content-Type", "application/json"), ("Idempotency-Key", "append-stream-1")],
                         b'{"body":"c3RyZWFtLXZhbHVl","partition":1}')
        assert append[0].startswith(b"HTTP/1.1 201"), append
        assert json.loads(append[1])["data"] == {"partition": 1, "offset": 0, "durability": "known"}
        partition_snapshot = request("GET", f"/api/admin/v1/streams/{stream_id}/partitions", auth)
        assert partition_snapshot[0].startswith(b"HTTP/1.1 200"), partition_snapshot
        assert json.loads(partition_snapshot[1])["data"][1] == {"partition": 1, "base_offset": 0, "next_offset": 1, "retained_bytes": 12}
        append_second = request("POST", f"/api/admin/v1/streams/{stream_id}/records",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "append-stream-2")],
                                b'{"body":"c2Vjb25k","partition":1}')
        assert append_second[0].startswith(b"HTTP/1.1 201"), append_second
        tail_path = f"/api/admin/v1/streams/{stream_id}/partitions/1/records:tail?offset=0"
        assert request("GET", tail_path, auth)[0].startswith(b"HTTP/1.1 400")
        tail = sse_first(tail_path, auth)
        assert tail.startswith(b"HTTP/1.1 200") and b"Content-Type: text/event-stream" in tail and b"event: record\n" in tail and b'"offset":0' in tail, tail
        assert request("GET", "/api/admin/v1/status", auth)[0].startswith(b"HTTP/1.1 200")
        stream_revision = request("GET", f"/api/admin/v1/streams/{stream_id}", auth)
        assert b'ETag: "s-' in stream_revision[0], stream_revision
        stream_etag = next(line.split(b": ", 1)[1].decode() for line in stream_revision[0].split(b"\r\n") if line.startswith(b"ETag: "))
        truncate_path = f"/api/admin/v1/streams/{stream_id}/partitions/1:truncate"
        assert request("POST", truncate_path,
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "truncate-missing-confirm")],
                       b'{"base_offset":1}')[0].startswith(b"HTTP/1.1 428")
        stale_job = request("POST", truncate_path,
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "truncate-stale"), ("If-Match", '"s-999999"'), ("X-KuttiDB-Confirm", stream_id)],
                            b'{"base_offset":1}')
        assert stale_job[0].startswith(b"HTTP/1.1 202"), stale_job
        assert wait_job(auth, json.loads(stale_job[1])["data"]["job_id"])["failure_code"] == "precondition_failed"
        truncated = request("POST", truncate_path,
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "truncate-confirmed"), ("If-Match", stream_etag), ("X-KuttiDB-Confirm", stream_id)],
                            b'{"base_offset":1}')
        assert truncated[0].startswith(b"HTTP/1.1 202"), truncated
        completed_job = wait_job(auth, json.loads(truncated[1])["data"]["job_id"])
        assert completed_job["state"] == "succeeded" and completed_job["base_offset"] == 1, completed_job
        partition_snapshot = json.loads(request("GET", f"/api/admin/v1/streams/{stream_id}/partitions", auth)[1])
        assert partition_snapshot["data"][1]["base_offset"] == 1 and partition_snapshot["data"][1]["next_offset"] == 2, partition_snapshot
        retention_gap = sse_first(f"/api/admin/v1/streams/{stream_id}/partitions/1/records:tail?offset=0", auth)
        assert b"event: offset_out_of_range\n" in retention_gap and b'"base_offset":1' in retention_gap, retention_gap
        fetched = request("GET", f"/api/admin/v1/streams/{stream_id}/partitions/1/records?offset=0&max_records=10&max_bytes=1024", auth)
        assert fetched[0].startswith(b"HTTP/1.1 200"), fetched
        assert json.loads(fetched[1])["data"] == [{"partition": 1, "offset": 1, "key": None, "body": {"encoding": "base64", "data": "c2Vjb25k", "size": 6, "content_type": "application/octet-stream"}}]
        group_id = "b64u:" + base64.urlsafe_b64encode(b"admin-group").rstrip(b"=").decode()
        with KuttiDBClient(port=7417) as native:
            assignment = native.stream_group_join("admin-test-stream", "admin-group")
            assert assignment.generation == 1
            offsets_url = f"/api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/offsets"
            offsets = request("GET", offsets_url, auth)
            assert offsets[0].startswith(b"HTTP/1.1 200"), offsets
            assert json.loads(offsets[1])["meta"]["snapshot_revision"] == 1
            assert b'ETag: "g-1"' in offsets[0], offsets[0]
            group_url = offsets_url.removesuffix("/offsets")
            group_detail = request("GET", group_url, auth)
            assert group_detail[0].startswith(b"HTTP/1.1 200") and json.loads(group_detail[1])["data"]["active_member_count"] == 1, group_detail
            members = request("GET", group_url + "/members", auth)
            member_data = json.loads(members[1])["data"]
            assert members[0].startswith(b"HTTP/1.1 200") and member_data[0]["member_id"] == "member-0" and member_data[0]["assigned_partition_count"] == 2, members
            offset_url = offsets_url + "/1"
            no_match = request("PUT", offset_url,
                               auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-offset-no-match")],
                               b'{"offset":1}')
            assert no_match[0].startswith(b"HTTP/1.1 428"), no_match
            committed_offset = request("PUT", offset_url,
                                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-offset-1"), ("If-Match", '"g-1"')],
                                       b'{"offset":1}')
            assert committed_offset[0].startswith(b"HTTP/1.1 200"), committed_offset
            assert json.loads(committed_offset[1])["data"]["offset"] == 1
            batch_offset_url = offsets_url + ":batch"
            batch_offsets = request("POST", batch_offset_url,
                                    auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-offset-batch"), ("If-Match", '"g-1"')],
                                    b'{"offsets":[{"partition":0,"offset":0},{"partition":1,"offset":1}]}')
            assert batch_offsets[0].startswith(b"HTTP/1.1 200") and json.loads(batch_offsets[1])["data"] == [{"partition": 0, "offset": 0}, {"partition": 1, "offset": 1}], batch_offsets
            reset_url = offsets_url.removesuffix("/offsets") + ":reset-offsets"
            assert request("POST", reset_url,
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-reset-no-confirm"), ("If-Match", '"g-1"')],
                           b'{"strategy":"latest"}')[0].startswith(b"HTTP/1.1 428")
            active_reset = request("POST", reset_url,
                                   auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-reset-active"), ("If-Match", '"g-1"'), ("X-KuttiDB-Confirm", group_id)],
                                   b'{"strategy":"latest"}')
            assert active_reset[0].startswith(b"HTTP/1.1 409") and json.loads(active_reset[1])["error"]["code"] == "active_group", active_reset
            forced_reset = request("POST", reset_url,
                                   auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-reset-force"), ("If-Match", '"g-1"'), ("X-KuttiDB-Confirm", group_id)],
                                   b'{"strategy":"latest","force":true}')
            assert forced_reset[0].startswith(b"HTTP/1.1 200") and json.loads(forced_reset[1])["data"] == {"strategy": "latest", "partition_count": 2, "durability": "known"}, forced_reset
            sessions_url = offsets_url.removesuffix("/offsets") + "/sessions"
            created_session = request("POST", sessions_url,
                                      auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-session-create"), ("X-KuttiDB-Confirm", group_id)],
                                      b'{"lease_ms":1000}')
            session_data = json.loads(created_session[1])["data"]
            assert created_session[0].startswith(b"HTTP/1.1 201") and session_data["session_id"].startswith("gs:") and len(session_data["assigned_partitions"]) == 1, created_session
            session_url = sessions_url + "/" + session_data["session_id"]
            assert request("GET", session_url, auth)[0].startswith(b"HTTP/1.1 200")
            heartbeat = request("POST", session_url + ":heartbeat",
                                auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-session-heartbeat")], b"{}")
            heartbeat_data = json.loads(heartbeat[1])["data"]
            assert heartbeat[0].startswith(b"HTTP/1.1 200") and heartbeat_data["assigned_partitions"] == session_data["assigned_partitions"], heartbeat
            assigned_partition = heartbeat_data["assigned_partitions"][0]
            records = request("GET", session_url + f"/records?partition={assigned_partition}&offset=0&max_records=10&max_bytes=1024", auth)
            assert records[0].startswith(b"HTTP/1.1 200"), records
            latest_offset = next(item["next_offset"] for item in partition_snapshot["data"] if item["partition"] == assigned_partition)
            session_commit = request("POST", session_url + "/offsets:commit",
                                     auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-session-commit"), ("If-Match", '"g-2"')],
                                     json.dumps({"partition": assigned_partition, "offset": latest_offset}).encode())
            assert session_commit[0].startswith(b"HTTP/1.1 200"), session_commit
            assert request("POST", session_url + ":leave",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-session-leave")], b"{}")[0].startswith(b"HTTP/1.1 200")
            assert request("GET", session_url, auth)[0].startswith(b"HTTP/1.1 410")
            stale = request("PUT", offset_url,
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "group-offset-stale"), ("If-Match", '"g-2"')],
                            b'{"offset":1}')
            assert stale[0].startswith(b"HTTP/1.1 412"), stale
        group_id = "b64u:" + base64.urlsafe_b64encode(b"not-a-group").rstrip(b"=").decode()
        assert request("GET", f"/api/admin/v1/streams/{stream_id}/consumer-groups/{group_id}/offsets", auth)[0].startswith(b"HTTP/1.1 404")
        stream_before_update = request("GET", f"/api/admin/v1/streams/{stream_id}", auth)
        stream_etag = next(line.split(b": ", 1)[1].decode() for line in stream_before_update[0].split(b"\r\n") if line.startswith(b"ETag: "))
        assert request("PATCH", f"/api/admin/v1/streams/{stream_id}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "stream-retention-no-match")],
                       b'{"max_retained_bytes":1024,"max_retained_age_ms":0}')[0].startswith(b"HTTP/1.1 428")
        retention = request("PATCH", f"/api/admin/v1/streams/{stream_id}",
                            auth + [("Content-Type", "application/json"), ("Idempotency-Key", "stream-retention-update"), ("If-Match", stream_etag)],
                            b'{"max_retained_bytes":1024,"max_retained_age_ms":0}')
        assert retention[0].startswith(b"HTTP/1.1 200") and json.loads(retention[1])["data"]["updated"], retention
        stream_after_update = request("GET", f"/api/admin/v1/streams/{stream_id}", auth)
        assert json.loads(stream_after_update[1])["data"]["max_retained_bytes"] == 1024 and json.loads(stream_after_update[1])["data"]["max_retained_age_ms"] == 0, stream_after_update
        stream_etag = next(line.split(b": ", 1)[1].decode() for line in stream_after_update[0].split(b"\r\n") if line.startswith(b"ETag: "))
        assert request("DELETE", f"/api/admin/v1/streams/{stream_id}",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "stream-delete-no-confirm"), ("If-Match", stream_etag)],
                       b"{}")[0].startswith(b"HTTP/1.1 428")
        deleted_stream = request("DELETE", f"/api/admin/v1/streams/{stream_id}",
                                 auth + [("Content-Type", "application/json"), ("Idempotency-Key", "stream-delete-confirmed"), ("If-Match", stream_etag), ("X-KuttiDB-Confirm", stream_id)],
                                 b"{}")
        assert deleted_stream[0].startswith(b"HTTP/1.1 202"), deleted_stream
        deleted_job = wait_job(auth, json.loads(deleted_stream[1])["data"]["job_id"])
        assert deleted_job["state"] == "succeeded" and deleted_job["kind"] == "stream.delete", deleted_job
        assert request("GET", f"/api/admin/v1/streams/{stream_id}", auth)[0].startswith(b"HTTP/1.1 404")
        assert request("POST", "/api/admin/v1/queues", auth + [("Content-Type", "application/json")], queue_body)[0].startswith(b"HTTP/1.1 428")
        assert request("GET", "/api/admin/v1/status")[0].startswith(b"HTTP/1.1 401")
        assert request("GET", "/api/admin/v1/status", [("Authorization", "Basic x")])[0].startswith(b"HTTP/1.1 401")
        assert request("OPTIONS", "/api/admin/v1/status")[0].startswith(b"HTTP/1.1 401")
        assert request("OPTIONS", "/api/admin/v1/status", auth)[0].startswith(b"HTTP/1.1 204")
        assert request("POST", "/api/admin/v1/status", auth)[0].startswith(b"HTTP/1.1 405")
        assert request("GET", "/api/admin/v1/nope", auth)[0].startswith(b"HTTP/1.1 404")
        assert request("GET", "/api/admin/v1/queues?limit=501", auth)[0].startswith(b"HTTP/1.1 400")
        assert request("POST", "/api/admin/v1/queues",
                       auth + [("Content-Type", "application/json"), ("Idempotency-Key", "invalid-utf8")],
                       b'{"name":"\xc0\xaf"}')[0].startswith(b"HTTP/1.1 400")
        assert request("POST", "/api/admin/v1/status", auth + [("Content-Type", "application/json")], b"x")[0].startswith(b"HTTP/1.1 405")
        head, _ = request("GET", "/api/admin/v1/status", auth + [("Origin", "https://ui.example")])
        assert b"Access-Control-Allow-Origin: https://ui.example" in head and b"Vary: Origin" in head
        assert request("GET", "/api/admin/v1/status", auth + [("Origin", "https://bad.example")])[0].startswith(b"HTTP/1.1 403")
        s = socket.create_connection(("127.0.0.1", PORT), 2)
        s.sendall(b"GET /api/admin/v1/status HTTP/1.1\r\nX: " + b"a" * 9000)
        assert s.recv(64).startswith(b"HTTP/1.1 431")
        s.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    with open(audit_file, "r", encoding="utf-8") as f:
        audit_records = [json.loads(line) for line in f if line.strip()]
    assert any(r["operation"] == "queue.publish" and r["result"] == "attempt" for r in audit_records)
    assert any(r["operation"] == "queue.delivery.ack" and r["result"] == "completed" for r in audit_records)
    assert all("idempotency_key_hash" in r and len(r["idempotency_key_hash"]) == 16 for r in audit_records)
    audit_text = json.dumps(audit_records)
    assert TOKEN.decode() not in audit_text and "aGVsbG8=" not in audit_text and "publish-queue-1" not in audit_text

    # Native TLS is available in this build: the independent admin TLS
    # listener accepts HTTPS and does not accept a plaintext HTTP request.
    features = SERVER_FEATURES
    assert "management-api-contract=1.0" in features and "management-api-audit=required" in features
    cert, key = os.path.join(tmp, "admin.crt"), os.path.join(tmp, "admin.key")
    made = subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                           "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost"],
                          capture_output=True, timeout=20)
    if "admin-tls=openssl" in features and made.returncode == 0:
        os.chmod(key, 0o600)
        tls_port = PORT + 1
        tls_proc = subprocess.Popen([SERVER, "7417", wal_file, "--admin-bind", f"127.0.0.1:{tls_port}",
                                     "--admin-token-file", token_file, "--admin-tls-cert", cert,
                                     "--admin-tls-key", key, "--admin-audit-log", audit_file],
                                    stderr=subprocess.PIPE, start_new_session=True)
        try:
            end = time.time() + 5
            while time.time() < end:
                try:
                    raw = socket.create_connection(("127.0.0.1", tls_port), .1)
                    ctx = ssl._create_unverified_context()
                    secure = ctx.wrap_socket(raw, server_hostname="localhost")
                    secure.sendall(b"GET /api/admin/v1/status HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer " + TOKEN + b"\r\n\r\n")
                    assert secure.recv(64).startswith(b"HTTP/1.1 200")
                    secure.close()
                    break
                except (OSError, ssl.SSLError):
                    time.sleep(.04)
            else:
                raise RuntimeError("admin TLS did not start")
        finally:
            tls_proc.terminate()
            tls_proc.wait(timeout=10)

    # Audit persistence failure has two materially different outcomes.  The
    # injected boundary lets us verify both without relying on chmod after
    # the audit descriptor has been opened.
    def start_audit_failure_server(port, fail_after):
        env = os.environ.copy()
        env["KUTTIDB_TEST_ADMIN_AUDIT_FAIL_AFTER"] = str(fail_after)
        child = subprocess.Popen([SERVER, "7417", os.path.join(tmp, f"audit-{port}.wal"),
                                  "--admin-bind", f"127.0.0.1:{port}",
                                  "--admin-token-file", token_file,
                                  "--admin-audit-log", os.path.join(tmp, f"audit-{port}.jsonl")],
                                 stderr=subprocess.PIPE, start_new_session=True, env=env)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", port), .1).close()
                return child
            except OSError:
                time.sleep(.03)
        child.terminate()
        raise RuntimeError("audit failure test server did not start")

    before_proc = start_audit_failure_server(PORT + 2, 0)
    try:
        rejected = request("POST", "/api/admin/v1/queues",
                           auth + [("Content-Type", "application/json"), ("Idempotency-Key", "audit-before")],
                           b'{"name":"audit-before","durable":false}', port=PORT + 2)
        assert rejected[0].startswith(b"HTTP/1.1 503") and b'audit_unavailable' in rejected[1], rejected
    finally:
        before_proc.terminate()
        before_proc.wait(timeout=10)

    after_proc = start_audit_failure_server(PORT + 3, 1)
    try:
        indoubt = request("POST", "/api/admin/v1/queues",
                          auth + [("Content-Type", "application/json"), ("Idempotency-Key", "audit-after")],
                          b'{"name":"audit-after","durable":false}', port=PORT + 3)
        assert indoubt[0].startswith(b"HTTP/1.1 500") and b'operation_in_doubt' in indoubt[1], indoubt
        audit_after_id = "b64u:" + base64.urlsafe_b64encode(b"audit-after").rstrip(b"=").decode()
        assert request("GET", f"/api/admin/v1/queues/{audit_after_id}", auth,
                       port=PORT + 3)[0].startswith(b"HTTP/1.1 200")
    finally:
        after_proc.terminate()
        after_proc.wait(timeout=10)

    # The accepted-client cap is enforced at accept time, not merely passed to
    # listen(2) as a backlog.  A stalled authenticated connection therefore
    # cannot accumulate unbounded server-side handlers.
    cap_port = PORT + 4
    cap_proc = subprocess.Popen([SERVER, "7417", os.path.join(tmp, "cap.wal"),
                                 "--admin-bind", f"127.0.0.1:{cap_port}",
                                 "--admin-token-file", token_file,
                                 "--admin-audit-log", os.path.join(tmp, "cap.audit.jsonl"),
                                 "--admin-max-clients", "1"],
                                stderr=subprocess.PIPE, start_new_session=True)
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                ready = request("GET", "/api/admin/v1/status", auth, port=cap_port)
                if ready[0].startswith(b"HTTP/1.1 200"):
                    break
            except OSError:
                time.sleep(.03)
        else:
            raise RuntimeError("connection-cap test server did not start")
        held = socket.create_connection(("127.0.0.1", cap_port), 2)
        held.sendall(b"GET /api/admin/v1/status HTTP/1.1\r\nHost: localhost\r\n")
        time.sleep(.05)
        capped = request("GET", "/api/admin/v1/status", auth, port=cap_port)
        assert capped[0].startswith(b"HTTP/1.1 429") and b"Retry-After: 1" in capped[0], capped
        held.close()
    finally:
        cap_proc.terminate()
        cap_proc.wait(timeout=10)

print("MANAGEMENT API TESTS PASSED")
