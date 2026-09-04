import concurrent.futures
import os
import socket as sockmod
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kuttidb_client import KuttiDBClient

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7391


def wait_for_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sockmod.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


results = {}


def run_tests():
    try:
        wait_for_port(PORT)
        with KuttiDBClient(port=PORT) as c:
            c.put("hello", b"world")
            assert c.get("hello") == b"world"
            c.put("hello", b"world2")
            assert c.get("hello") == b"world2"
            assert c.get("nope") is None
            assert c.delete("hello")
            assert not c.delete("hello")
            assert c.get("hello") is None
            blob = os.urandom(4096)
            c.put("bin", blob)
            assert c.get("bin") == blob

            # batched ops
            items = [(f"b{i}", f"v{i}".encode()) for i in range(1000)]
            c.put_many(items)
            vals = c.get_many([k for k, _ in items])
            assert vals == [v for _, v in items]
            vals = c.get_many([f"b{i}" for i in range(0, 1000, 7)])
            assert all(v is not None for v in vals)
            missing = c.get_many(["nope1", "nope2"])
            assert missing == [None, None]
            # overwrite via batch
            c.put_many([("b0", b"overwritten")])
            assert c.get("b0") == b"overwritten"

            def worker(i):
                with KuttiDBClient(port=PORT) as cc:
                    for j in range(200):
                        k = f"k{i}-{j}"
                        v = os.urandom(64)
                        cc.put(k, v)
                        assert cc.get(k) == v
                        if j % 3 == 0:
                            assert cc.delete(k)
                    return True

            with concurrent.futures.ThreadPoolExecutor(8) as ex:
                assert all(ex.map(worker, range(8)))

            start = time.perf_counter()
            with KuttiDBClient(port=PORT) as cc:
                for j in range(10000):
                    cc.put(f"bench{j}", b"x" * 100)
                for j in range(10000):
                    cc.get(f"bench{j}")
            dt = time.perf_counter() - start
            results["bench"] = 20000 / dt

            # batched throughput
            start = time.perf_counter()
            with KuttiDBClient(port=PORT) as cc:
                cc.put_many((f"bbench{j}", b"y" * 100) for j in range(100000))
                got = cc.get_many([f"bbench{j}" for j in range(100000)])
            assert all(v == b"y" * 100 for v in got)
            dt = time.perf_counter() - start
            results["bench_batch"] = 200000 / dt
        results["ok"] = True
    except Exception as e:  # noqa: BLE001
        results["err"] = e
        results["ok"] = False


import subprocess
server_bin = os.environ.get("KUTTIDB_SERVER", "./kuttidb")
proc = subprocess.Popen([server_bin, str(PORT), "-"],
                        stderr=subprocess.DEVNULL, start_new_session=True)
try:
    run_tests()
finally:
    proc.terminate()
    proc.wait(timeout=10)

if not results.get("ok"):
    print(f"TESTS FAILED: {results.get('err')}")
    sys.exit(1)
print(f"20k sequential ops ({results['bench']:.0f} ops/s via Python client)")
print(f"200k batched ops    ({results['bench_batch']:.0f} ops/s via Python client)")
print("ALL TESTS PASSED")
