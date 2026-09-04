"""Anti-cache-stampede (singleflight) protocol tests.

Covers the Milestone 5 exit gate: crashed loaders cannot strand a key (leases
expire), waits are bounded and never block the event loop, and claims/waiters/
negative answers behave per the documented state machine.
"""

import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient, KuttiDBError

PORT = 7408


def wait_port():
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.1).close()
            return
        except OSError:
            time.sleep(0.03)
    raise RuntimeError("singleflight server did not start")


def start():
    server = os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb"))
    proc = subprocess.Popen([server, str(PORT), "-", "50"],
                            stderr=subprocess.DEVNULL, start_new_session=True)
    wait_port()
    return proc


def stop(proc):
    proc.kill()
    proc.wait()


tmp = tempfile.mkdtemp(prefix="kuttidb-sf-proto-")
proc = None
try:
    # ---- exactly one claim among concurrent racers -------------------------
    proc = start()
    states = []
    lock = threading.Lock()
    def racer():
        with KuttiDBClient(port=PORT) as c:
            r = c.get_or_claim("hot", lease=5.0)
            with lock:
                states.append(r["state"])
    ts = [threading.Thread(target=racer) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert states.count("claimed") == 1 and states.count("wait") == 7, states
    stats = KuttiDBClient(port=PORT).stats()
    assert stats["claims"] == 1 and stats["singleflight_waiters"] == 0

    # ---- crashed loader: the lease expires and the key is claimable again --
    time.sleep(5.2)  # lease was 5s; the claiming "loader" never returned
    with KuttiDBClient(port=PORT) as c:
        r = c.get_or_claim("hot", lease=5.0)
        assert r["state"] == "claimed", r
    # a waiter registered now is woken by the new loader's put_and_release
    waiter_result = {}
    def waiter():
        with KuttiDBClient(port=PORT) as c:
            r = c.get_or_claim("hot", lease=5.0)
            assert r["state"] == "wait", r
            waiter_result.update(c.wait_for_key("hot", timeout=5.0))
    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.2)
    with KuttiDBClient(port=PORT) as c:
        c.put_and_release("hot", b"loaded", ttl=60)
    t.join(5)
    assert waiter_result.get("state") == "value" and \
        waiter_result.get("value") == b"loaded", waiter_result
    with KuttiDBClient(port=PORT) as c:
        assert c.get("hot") == b"loaded"
        assert c.get_or_claim("hot", lease=5.0)["state"] == "value"

    # ---- get_or_load: the loader runs exactly once under concurrency -------
    loader_calls = []
    def slow_loader():
        loader_calls.append(1)
        time.sleep(0.25)
        return b"expensive"
    results = []
    def loader_racer():
        with KuttiDBClient(port=PORT) as c:
            results.append(c.get_or_load("price:1", slow_loader, ttl=60))
    ts = [threading.Thread(target=loader_racer) for _ in range(10)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert results == [b"expensive"] * 10, results
    assert len(loader_calls) == 1, loader_calls  # single-flight held

    # ---- negative caching: loader returning None is cached, GET stays a miss
    loader_calls.clear()
    neg_results = []
    def neg_racer():
        with KuttiDBClient(port=PORT) as c:
            neg_results.append(c.get_or_load("absent:1", lambda: None, ttl=1.0))
    ts = [threading.Thread(target=neg_racer) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert neg_results == [None] * 6, neg_results
    assert len(loader_calls) == 0
    with KuttiDBClient(port=PORT) as c:
        r = c.get_or_claim("absent:1", lease=5.0)
        assert r["state"] == "negative", r
        assert c.get("absent:1") is None, "negative must stay invisible to GET"
    stats = KuttiDBClient(port=PORT).stats()
    assert stats["negatives"] == 1, stats
    # negative answers expire like TTL data
    time.sleep(1.2)
    with KuttiDBClient(port=PORT) as c:
        r = c.get_or_claim("absent:1", lease=5.0)
        assert r["state"] == "claimed", r  # negative expired: claimable again

    # ---- wait timeout is enforced server-side ------------------------------
    with KuttiDBClient(port=PORT) as c:
        c.get_or_claim("lonely", lease=5.0)  # claim and never release
    timeout_result = {}
    def lonely_waiter():
        with KuttiDBClient(port=PORT) as c:
            r = c.get_or_claim("lonely", lease=5.0)
            assert r["state"] == "wait", r
            timeout_result.update(c.wait_for_key("lonely", timeout=0.5))
    t = threading.Thread(target=lonely_waiter)
    t.start()
    t.join(5)
    assert timeout_result.get("state") == "timeout", timeout_result

    # ---- release without a value lets a new loader claim -------------------
    released = {}
    def release_waiter():
        with KuttiDBClient(port=PORT) as c:
            r = c.get_or_claim("lonely", lease=5.0)
            assert r["state"] == "wait", r
            released.update(c.wait_for_key("lonely", timeout=5.0))
    t = threading.Thread(target=release_waiter)
    t.start()
    time.sleep(0.2)
    with KuttiDBClient(port=PORT) as c:
        c.release_claim("lonely")
    t.join(5)
    assert released.get("state") == "released", released
    with KuttiDBClient(port=PORT) as c:
        r = c.get_or_claim("lonely", lease=5.0)
        assert r["state"] == "claimed", r
        c.release_claim("lonely")

    # ---- protocol refusals and limits ---------------------------------------
    def raw(op, key, value):
        k = key.encode()
        with socket.create_connection(("127.0.0.1", PORT), 2) as s:
            s.settimeout(2)
            s.sendall(struct.pack("<BHI", op, len(k), len(value)) + k + value)
            return s.recv(5)[0]

    assert raw(0x50, "x", b"") == 2                # GET_OR_CLAIM needs lease
    assert raw(0x50, "x", struct.pack("<I", 61000)) == 2  # lease above cap
    assert raw(0x51, "x", struct.pack("<I", 0)) == 2      # zero timeout
    assert raw(0x51, "x", struct.pack("<I", 61000)) == 2  # timeout above cap
    assert raw(0x53, "x", b"junk") == 2            # RELEASE takes no payload
    assert raw(0x54, "x", b"") == 2                # GET_OR_REFRESH needs lease
    assert raw(0x54, "x", struct.pack("<I", 61000)) == 2
    # duplicate WAIT on one connection is refused while the first is pending
    k = b"dup"
    with KuttiDBClient(port=PORT) as c:
        assert c.get_or_claim("dup", lease=5.0)["state"] == "claimed"
    with socket.create_connection(("127.0.0.1", PORT), 2) as s:
        s.settimeout(2)
        frame = struct.pack("<BHI", 0x51, len(k), 4) + k + struct.pack("<I", 5000)
        s.sendall(frame)   # first WAIT: registered, response deferred
        s.sendall(frame)   # second WAIT on the same connection: refused
        assert s.recv(5)[0] == 2
    stop(proc)

    # ---- stale-while-revalidate and refresh-ahead ----------------------------
    proc = start()
    with KuttiDBClient(port=PORT) as c:
        # fresh value answers like a plain hit
        c.put_swr("swr", b"v1", ttl=1.0, stale_for=10.0)
        assert c.get("swr") == b"v1"
        r = c.get_or_refresh("swr")
        assert r["state"] == "value" and r["value"] == b"v1" and not r["holder"]
        assert c.stats()["stale_entries"] == 1
        # plain GET still respects the TTL (no behavior change)
        time.sleep(1.15)
        assert c.get("swr") is None
        # expired key: the retained copy answers stale and this caller
        # becomes the revalidation holder; a second reader also gets the
        # stale value immediately, holder=False, without waiting
        r = c.get_or_refresh("swr")
        assert r["state"] == "stale" and r["value"] == b"v1" and r["holder"], r
        r2 = c.get_or_refresh("swr")
        assert r2["state"] == "stale" and r2["value"] == b"v1" and not r2["holder"]
        stats = c.stats()
        assert stats["stale_serves"] == 2 and stats["claims"] == 1, stats
        # revalidation re-arms the window; the value is fresh again
        c.put_swr("swr", b"v2", ttl=30.0, stale_for=10.0)
        r = c.get_or_refresh("swr")
        assert r["state"] == "value" and r["value"] == b"v2", r
        # refresh-ahead: still fresh, but the refresh window is due and this
        # caller holds the lease
        c.put_swr("ahead", b"a", ttl=30.0, stale_for=10.0, refresh_after=0.4)
        time.sleep(0.5)
        r = c.get_or_refresh("ahead")
        assert r["state"] == "refresh" and r["value"] == b"a" and r["holder"], r
        assert c.stats()["refresh_serves"] == 1
        # a successful plain put supersedes the retained stale copy
        c.put_swr("gone", b"old", ttl=1.0, stale_for=10.0)
        time.sleep(1.15)
        assert c.get_or_refresh("gone")["state"] == "stale"
        c.put("gone", b"fresh")
        r = c.get_or_refresh("gone")
        assert r["state"] == "value" and r["value"] == b"fresh", r
    stop(proc)
    # the stale registry is in-memory coordination state: a restart clears it
    proc = start()
    with KuttiDBClient(port=PORT) as c:
        assert c.stats()["stale_entries"] == 0
        # an expired key with no retained copy falls back to claim/wait
        r = c.get_or_refresh("expired-nowhere")
        assert r["state"] == "claimed", r
    stop(proc)

    # ---- get_or_load_swr helper: loader runs once per window -----------------
    proc = start()
    with KuttiDBClient(port=PORT) as c:
        calls = []

        def loader():
            calls.append(1)
            return b"loaded"

        v = c.get_or_load_swr("helper", loader, ttl=30.0, stale_for=10.0)
        assert v == b"loaded" and len(calls) == 1
        assert c.get_or_load_swr("helper", loader,
                                 ttl=30.0, stale_for=10.0) == b"loaded"
        assert len(calls) == 1, "loader must not run while the value is fresh"
        # after expiry the stale copy answers while this caller revalidates
        c.put_swr("helper", b"stale-v", ttl=1.0, stale_for=10.0)
        time.sleep(1.15)
        v = c.get_or_load_swr("helper", loader, ttl=30.0, stale_for=10.0)
        assert v == b"loaded" and len(calls) == 2, (v, calls)
        assert c.get("helper") == b"loaded"
        # negative answers still work through the helper
        n = c.get_or_load_swr("missing", lambda: None,
                              ttl=30.0, stale_for=10.0)
        assert n is None
        assert c.get("missing") is None
    stop(proc)

    # ---- claims are ephemeral: a restart clears them all --------------------
    proc = start()
    with KuttiDBClient(port=PORT) as c:
        r = c.get_or_claim("restart", lease=30.0)
        assert r["state"] == "claimed", r
    stop(proc)
    proc = start()
    with KuttiDBClient(port=PORT) as c:
        r = c.get_or_claim("restart", lease=30.0)
        assert r["state"] == "claimed", r  # no stranded state after restart
        assert c.stats()["claims"] <= 1
    print("SINGLEFLIGHT PROTOCOL TESTS PASSED")
finally:
    if proc:
        try:
            stop(proc)
        except Exception:
            pass
    shutil.rmtree(tmp, ignore_errors=True)
