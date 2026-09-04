import concurrent.futures as cf
import multiprocessing as mp
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def worker(args):
    i, port, n = args
    from kuttidb_client import KuttiDBClient
    c = KuttiDBClient(port=port, timeout=180)
    c.put_many((f"w{i}-{j}", b"x" * 100) for j in range(n))
    got = c.get_many([f"w{i}-{j}" for j in range(n)])
    assert all(v == b"x" * 100 for v in got), f"client {i} mismatch"
    return 2 * n


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7392
    nclients = int(sys.argv[2]) if len(sys.argv) > 2 else 16
    per = int(sys.argv[3]) if len(sys.argv) > 3 else 25000
    no_wal = len(sys.argv) > 4 and sys.argv[4] == "off"
    wal = "-" if no_wal else f"/tmp/bench{port}.wal"

    proc = subprocess.Popen(["./kuttidb", str(port), wal, "100"],
                            stderr=subprocess.DEVNULL, start_new_session=True)
    time.sleep(0.7)
    try:
        t = time.perf_counter()
        with cf.ProcessPoolExecutor(nclients) as ex:
            total = sum(ex.map(worker, [(i, port, per) for i in range(nclients)]))
        dt = time.perf_counter() - t
        print(f"{total} ops, {nclients} client processes, WAL on: "
              f"{total/dt:.0f} ops/s, data verified")
    finally:
        proc.terminate()
        proc.wait()
        for suffix in ("", ".snap"):
            try:
                os.remove(wal + suffix)
            except OSError:
                pass
