"""Protocol parser fuzzing against the live server.

The server may refuse or close any malformed connection, but it must stay
alive, keep serving well-formed clients, and enforce its resource limits.
Deterministic sequence seeded from a fixed value."""

import os
import random
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from kuttidb_client import KuttiDBClient

PORT = 7423
random.seed(0x5eed)


def wait_port():
    until = time.time() + 8
    while time.time() < until:
        try:
            socket.create_connection(("127.0.0.1", PORT), .1).close()
            return
        except OSError:
            time.sleep(.03)
    raise RuntimeError("server did not start")


def conn(timeout=2.0):
    s = socket.create_connection(("127.0.0.1", PORT), timeout)
    s.settimeout(timeout)
    return s


def frame(op, key, value=b""):
    return struct.pack("<BHI", op, len(key), len(value)) + key + value


def drain_and_close(s):
    try:
        s.settimeout(0.2)
        while s.recv(4096):
            pass
    except OSError:
        pass
    try:
        s.close()
    except OSError:
        pass


tmp = tempfile.mkdtemp(prefix="kuttidb-proto-fuzz-")
p = None
try:
    p = subprocess.Popen(
        [os.environ.get("KUTTIDB_SERVER", os.path.join(ROOT, "kuttidb")),
         str(PORT), os.path.join(tmp, "kuttidb.wal"), "20",
         "--queue-wal", os.path.join(tmp, "queue.wal"),
         "--stream-wal", os.path.join(tmp, "stream.wal")],
        stderr=subprocess.DEVNULL, start_new_session=True)
    wait_port()

    # 1. Pure random garbage on fresh connections.
    for _ in range(200):
        s = conn()
        s.sendall(os.urandom(random.randint(0, 4096)))
        drain_and_close(s)

    # 2. Every opcode with malformed headers/payloads (bad lengths, missing
    #    bodies, oversized claims).
    for op in range(256):
        for _ in range(3):
            s = conn()
            klen = random.choice([0, 1, 255, 256, 65534, 65535])
            vlen = random.choice([0, 1, 7, 100, 1 << 20, 0xFFFFFFFF])
            payload = os.urandom(min(vlen, 4096))
            s.sendall(struct.pack("<BHI", op, klen, vlen) +
                      os.urandom(min(klen, 512)) + payload)
            drain_and_close(s)

    # 3. Valid frames cut off mid-body, then abandoned.
    body = frame(0x01, b"fuzz", b"value")
    for cut in range(1, len(body), 3):
        s = conn()
        s.sendall(body[:cut])
        drain_and_close(s)

    # 4. Pipelined garbage mixed with valid frames on one connection.
    s = conn()
    try:
        s.sendall(os.urandom(13) + frame(0x02, b"x") +
                  os.urandom(31) + frame(0x11, b"", struct.pack("<I", 1)) +
                  os.urandom(5))
    except OSError:
        pass
    drain_and_close(s)

    # 5. Malformed stream, queue, and atomic commands with plausible envelopes.
    for op in (0x20, 0x21, 0x30, 0x33, 0x40, 0x41, 0x42, 0x43,
               0x50, 0x60, 0x61, 0x62, 0x63, 0x65, 0x67, 0x68):
        for _ in range(10):
            s = conn()
            topic = random.choice([b"", b"t", b"x" * 300])
            arg = os.urandom(random.randint(0, 64))
            s.sendall(frame(op, topic, arg))
            drain_and_close(s)

    # The server must still be a fully working, bounded store.
    with KuttiDBClient(port=PORT) as c:
        c.put("after-fuzz", b"alive")
        assert c.get("after-fuzz") == b"alive"
        stats = c.stats()
        assert stats["count"] >= 1
        assert stats["connections"] <= 8  # event loops, no leaked conns
    with KuttiDBClient(port=PORT) as c:
        assert c.health() is True

    print("PROTOCOL FUZZ TESTS PASSED")
finally:
    if p:
        p.kill()
        p.wait()
    shutil.rmtree(tmp, ignore_errors=True)
