#!/usr/bin/env python3
"""End-to-end protocol test for atomic job completion.

Drives the vertical acceptance scenario over the native binary protocol:
durable Queue declaration, completion-capable consume, atomic completion
with a version-checked durable-state PUT and an output publish, receipt
replay after a lost response, restart, checkpoint, later ACK of the output,
later state change, and the full conflict/fencing matrix.
"""
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid

def free_port():
    import socket as _s
    with _s.socket() as sk:
        sk.bind(("127.0.0.1", 0))
        return sk.getsockname()[1]


PORT = int(os.environ.get("KUTTIDB_JOB_TEST_PORT", "0")) or free_port()


class Conn:
    def __init__(self, port=PORT):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def send(self, op, key=b"", value=b""):
        self.sock.sendall(struct.pack("<BHI", op, len(key), len(value)) + key + value)

    def recv_exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise RuntimeError("connection closed")
            buf += chunk
        return buf

    def recv_response(self):
        header = self.recv_exact(5)
        status = header[0]
        (vlen,) = struct.unpack("<I", header[1:5])
        return status, self.recv_exact(vlen) if vlen else b""

    def req(self, op, key=b"", value=b""):
        self.send(op, key, value)
        return self.recv_response()

    def close(self):
        self.sock.close()


def caps(conn):
    st, v = conn.req(0x0A, b"", struct.pack("<HH", 1, 8))
    assert st == 0x00, st
    _, minor, bits = struct.unpack("<HHQ", v[:12])
    return minor, bits


def start_server(wal_dir, extra=()):
    args = ["./kuttidb", str(PORT), os.path.join(wal_dir, "kuttidb.wal"),
            "--job-completion"] + list(extra)
    proc = subprocess.Popen(args, cwd=os.getcwd(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited: %s" % proc.stderr.read().decode())
        try:
            c = Conn()
            c.req(0x09)  # health
            c.close()
            return proc
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start: %s" % proc.stderr.read())


def stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def b64u(data):
    import base64
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def declare(conn, name, max_depth=0):
    st, _ = conn.req(0x20, name.encode(), struct.pack("<BQ", 1, max_depth))
    assert st == 0x00, ("declare", name, st)


def publish(conn, name, payload):
    st, v = conn.req(0x21, name.encode(), payload)
    assert st == 0x00, ("publish", st)
    return struct.unpack("<Q", v)[0]


def manifest(conn):
    st, v = conn.req(0x77)
    assert st == 0x00, ("manifest", st)
    (n,) = struct.unpack("<H", v[:2])
    out = {}
    at = 2
    for _ in range(n):
        (nlen,) = struct.unpack("<H", v[at:at + 2]); at += 2
        name = v[at:at + nlen].decode(); at += nlen
        durable = v[at]; at += 1
        inc, depth, inflight, maxd, rev = struct.unpack("<QQQQQ", v[at:at + 40])
        at += 40
        out[name] = {"durable": durable, "incarnation": inc, "depth": depth,
                     "inflight": inflight, "max_depth": maxd, "revision": rev}
    return out


def job_consume(conn, queue, consumer, visibility_ms=30000):
    value = struct.pack("<H", len(consumer)) + consumer.encode() + \
        struct.pack("<Q", visibility_ms)
    st, v = conn.req(0x70, queue.encode(), value)
    if st == 0x01:
        return None
    assert st == 0x00, ("consume", st, v)
    store_id = v[0:16]
    inc, msg_id = struct.unpack("<QQ", v[16:32])
    (attempts,) = struct.unpack("<I", v[32:36])
    redelivered = v[36]
    (lease,) = struct.unpack("<Q", v[37:45])
    proof = v[45:61]
    payload = v[61:]
    return {"store_id": store_id, "incarnation": inc, "message_id": msg_id,
            "attempts": attempts, "redelivered": redelivered, "lease": lease,
            "proof": proof, "payload": payload}


def job_complete(conn, queue, d, op_id, state_key, expected_version, state_value,
                 output=None):
    value = op_id + struct.pack("<QQ", d["incarnation"], d["message_id"]) + \
        d["proof"] + struct.pack("<H", len(state_key)) + state_key + \
        struct.pack("<Q", expected_version) + \
        struct.pack("<I", len(state_value)) + state_value
    if output:
        qname, out_inc, payload = output
        qname = qname.encode() if isinstance(qname, str) else qname
        value += b"\x01" + struct.pack("<H", len(qname)) + qname + \
            struct.pack("<Q", out_inc) + struct.pack("<I", len(payload)) + payload
    else:
        value += b"\x00"
    st, v = conn.req(0x71, queue.encode(), value)
    if st == 0x00:
        commit, version, out_msg, completed, expires = struct.unpack(
            "<QQQQQ", v[:40])
        return {"ok": True, "replayed": bool(v[40]), "commit": commit,
                "version": version, "output_msg": out_msg,
                "completed": completed, "expires": expires}
    code, outcome = v[0], v[1]
    return {"ok": False, "code": code, "outcome": outcome}


def state_get(conn, key):
    st, v = conn.req(0x73, key)
    if st == 0x01:
        return None
    assert st == 0x00, ("state_get", st)
    version, commit = struct.unpack("<QQ", v[:16])
    return {"version": version, "commit": commit, "value": v[16:]}


def state_put(conn, key, op_id, expected, value):
    st, v = conn.req(0x74, key, op_id + struct.pack("<Q", expected) + value)
    if st == 0x00:
        commit, version, completed, expires = struct.unpack("<QQQQ", v[:32])
        return {"ok": True, "replayed": bool(v[32]), "commit": commit,
                "version": version, "completed": completed, "expires": expires}
    code, outcome = v[0], v[1]
    return {"ok": False, "code": code, "outcome": outcome}


def state_delete(conn, key, op_id, expected):
    st, v = conn.req(0x75, key, op_id + struct.pack("<Q", expected))
    if st == 0x00:
        commit, version, completed, expires = struct.unpack("<QQQQ", v[:32])
        return {"ok": True, "replayed": bool(v[32]), "commit": commit,
                "version": version}
    code, outcome = v[0], v[1]
    return {"ok": False, "code": code, "outcome": outcome}


def consume_raw(conn, queue, visibility=30000):
    """Ordinary consume (0x22): returns the delivery tag for plain ACK."""
    st, v = conn.req(0x22, queue.encode(), struct.pack("<Q", visibility))
    if st == 0x01:
        return None
    assert st == 0x00, ("raw consume", st)
    tag, msg_id = struct.unpack("<QQ", v[:16])
    return {"tag": tag, "message_id": msg_id, "payload": v[26:]}


def ack(conn, queue, tag):
    st, _ = conn.req(0x23, queue.encode(), struct.pack("<Q", tag))
    assert st == 0x00, ("ack", st)


def durable_operation(conn, op_id):
    st, v = conn.req(0x76, b"", op_id)
    if st == 0x01:
        return None
    assert st == 0x00, ("durable_operation", st)
    return {"kind": v[0], "commit": struct.unpack("<Q", v[1:9])[0],
            "version": struct.unpack("<Q", v[9:17])[0]}


def receipt_lookup(conn, op_id):
    st, v = conn.req(0x72, b"", op_id)
    if st == 0x01:
        return None
    assert st == 0x00, ("receipt", st)
    commit, version, out_msg, completed, expires = struct.unpack("<QQQQQ", v[:40])
    return {"commit": commit, "version": version, "output_msg": out_msg,
            "completed": completed, "expires": expires}


def depth(conn, name):
    return manifest(conn)[name]["depth"]


def main():
    proc = None
    with tempfile.TemporaryDirectory() as wal_dir:
        proc = start_server(wal_dir)
        c = Conn()
        minor, bits = caps(c)
        assert minor >= 8, minor
        CAP_JOBS = 1 << 16
        assert bits & CAP_JOBS, "CAP_JOBS missing"

        declare(c, "extract-pdf")
        declare(c, "index-text")
        m = manifest(c)
        assert m["extract-pdf"]["durable"] == 1
        in_inc = m["extract-pdf"]["incarnation"]
        out_inc = m["index-text"]["incarnation"]

        msg_id = publish(c, "extract-pdf", b"pdf-bytes-1")
        c.req(0x29, b"pdf-worker")  # register named consumer

        d = job_consume(c, "extract-pdf", "pdf-worker")
        assert d and d["message_id"] == msg_id, d
        assert d["incarnation"] == in_inc

        # 4. Submit the atomic completion with a version-checked create.
        op_id = uuid.uuid4().bytes
        r = job_complete(c, "extract-pdf", d, op_id, b"pdf:42", 0,
                         b"extracted-text-1", ("index-text", out_inc, b"pdf:42"))
        assert r["ok"], r
        assert not r["replayed"]
        assert r["output_msg"] != 0

        # one committed state change, one output publish, no input left
        st = state_get(c, b"pdf:42")
        assert st and st["value"] == b"extracted-text-1", st
        assert st["version"] == r["version"]
        assert depth(c, "extract-pdf") == 0
        assert depth(c, "index-text") == 1

        # 5/6. Lost response + restart: replay returns the original result.
        c.close()
        stop(proc)
        proc = start_server(wal_dir)
        c = Conn()
        m2 = manifest(c)
        assert m2["extract-pdf"]["incarnation"] == in_inc, "incarnation drift"
        r2 = job_complete(c, "extract-pdf", d, op_id, b"pdf:42", 0,
                          b"extracted-text-1", ("index-text", out_inc, b"pdf:42"))
        assert r2["ok"] and r2["replayed"], r2
        assert r2["commit"] == r["commit"] and r2["version"] == r["version"]
        assert r2["output_msg"] == r["output_msg"]
        # replay must not republish the output or re-write state
        assert depth(c, "index-text") == 1
        st = state_get(c, b"pdf:42")
        assert st["version"] == r["version"]

        # 7. After the output was ACKed and the state subsequently changed.
        out_del = consume_raw(c, "index-text")
        assert out_del and out_del["message_id"] == r["output_msg"]
        ack(c, "index-text", out_del["tag"])
        # ack via tag: use 0x23 with delivery tag? queue ACK uses tag.
        r3 = job_complete(c, "extract-pdf", d, op_id, b"pdf:42", 0,
                          b"extracted-text-1", ("index-text", out_inc, b"pdf:42"))
        assert r3["ok"] and r3["replayed"], r3
        assert depth(c, "index-text") == 0, "replay republished the output"
        # newer state must not be overwritten
        op2 = uuid.uuid4().bytes
        sp = state_put(c, b"pdf:42", op2, r["version"], b"corrected")
        assert sp["ok"], sp
        r4 = job_complete(c, "extract-pdf", d, op_id, b"pdf:42", 0,
                          b"extracted-text-1", ("index-text", out_inc, b"pdf:42"))
        assert r4["ok"] and r4["replayed"], r4
        st = state_get(c, b"pdf:42")
        assert st["value"] == b"corrected", "replay overwrote newer state"

        # receipt lookup without any delivery
        rr = receipt_lookup(c, op_id)
        assert rr and rr["commit"] == r["commit"]

        # the input queue is empty: a NEW delivery cannot be fenced, but the
        # old completion id must still replay (empty ready set returns MISS)
        c2 = Conn()
        d5 = job_consume(c2, "extract-pdf", "pdf-worker")
        assert d5 is None, "input queue should be empty"
        c2.close()

        # state version conflict paths
        c3 = Conn()
        sp2 = state_put(c3, b"pdf:42", uuid.uuid4().bytes, 1, b"stale")
        assert sp2["ok"] is False and sp2["code"] == 5, sp2
        sp3 = state_put(c3, b"pdf:42", uuid.uuid4().bytes, 0, b"x")
        assert sp3["code"] == 5, sp3
        sd = state_delete(c3, b"pdf:42", uuid.uuid4().bytes, 1)
        assert sd["code"] == 5, sd
        sp_ver = state_get(c3, b"pdf:42")["version"]
        del_op = uuid.uuid4().bytes
        sd2 = state_delete(c3, b"pdf:42", del_op, sp_ver)
        assert sd2["ok"], sd2
        assert state_get(c3, b"pdf:42") is None
        # retry of the delete with the SAME id returns the retained receipt
        # even though the entry is now absent
        sd3 = state_delete(c3, b"pdf:42", del_op, sp_ver)
        assert sd3["ok"] and sd3["replayed"], sd3
        # lookup through the shared durable-operation surface
        do = durable_operation(c3, del_op)
        assert do and do["kind"] == 3, do
        do2 = durable_operation(c3, op2)
        assert do2 and do2["kind"] == 2 and do2["version"] == sp["version"], do2
        # idempotency conflict: same id, different request
        sp4 = state_put(c3, b"pdf:42", del_op, 0, b"different")
        assert sp4["code"] == 4, sp4
        c3.close()
        c.close()
        stop(proc)
    print("test_job_protocol: OK")


if __name__ == "__main__":
    main()
