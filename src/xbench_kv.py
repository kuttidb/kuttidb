#!/usr/bin/env python3
"""Cache (key/value) comparison at a matched periodic-durability tier.

The audit of the earlier cache comparison found three defects, all of which
this harness fixes by construction:

  * KuttiDB's interleaved PUT/GET/DELETE mix was ranked against Redis's
    separate SET and GET runs. Here both systems run the same two phases in
    the same order: write every key, then read every key back.
  * The resource comparison gave KuttiDB 400k distinct live keys and Redis one
    repeatedly overwritten key. Here both get the same distinct keyspace, so
    the memory columns describe the same amount of stored data.
  * Redis AOF `everysec` was compared against KuttiDB's 100 ms periodic
    setting. Here both are configured to the same nominal one-second window.

Values are verified on read: a wrong or missing value fails the run.
"""
import argparse, os, shutil, statistics, subprocess, sys, tempfile, time
from pathlib import Path

sys.path.insert(0, "/root/perf")
sys.path.insert(0, "/root/perf/work/src")
from xbench_core import dir_bytes, phase, record, stop, usage, wait_for

DATA_ROOT = "/root/perf/data"


def keys(count):
    return [f"bench:key:{i:012d}" for i in range(count)]


class KuttiCache:
    name = "kuttidb"

    def __init__(self, args):
        self.args, self.dir = args, tempfile.mkdtemp(prefix="kv-kuttidb-", dir=DATA_ROOT)
        self.log = open(Path(self.dir) / "server.log", "w+")

    def start(self, port):
        from kuttidb_client import KuttiDBClient
        # Positional FSYNC_MS is the cache WAL's periodic interval.
        self.proc = subprocess.Popen(
            [self.args.kuttidb_binary, str(port), str(Path(self.dir) / "cache.wal"),
             str(self.args.fsync_ms), "--threads", "1"],
            stdout=self.log, stderr=self.log, cwd=self.dir)
        self.server_pid = self.proc.pid

        def ready():
            with KuttiDBClient(port=port, timeout=0.5) as db:
                db.health()
            return True
        wait_for(ready, 20, "kuttidb", self.proc)
        self.db = KuttiDBClient(port=port, timeout=120)
        self.db.__enter__()

    def write(self, ks, value, batch):
        done = 0
        for start in range(0, len(ks), batch):
            chunk = ks[start:start + batch]
            self.db.put_many([(k, value) for k in chunk])
            done += len(chunk)
        return done

    def read(self, ks, value, batch):
        done = 0
        for start in range(0, len(ks), batch):
            chunk = ks[start:start + batch]
            got = self.db.get_many(chunk)
            if len(got) != len(chunk) or any(g != value for g in got):
                raise RuntimeError(f"value mismatch at {start}")
            done += len(chunk)
        return done

    def close(self):
        try:
            self.db.__exit__(None, None, None)
        except Exception:
            pass
        stop(self.proc)
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class RedisCache:
    name = "redis"

    def __init__(self, args):
        self.args, self.dir = args, tempfile.mkdtemp(prefix="kv-redis-", dir=DATA_ROOT)
        self.log = open(Path(self.dir) / "server.log", "w+")

    def start(self, port):
        import redis
        conf = Path(self.dir) / "redis.conf"
        # everysec is Redis's one-second AOF window, matching --fsync-ms 1000.
        policy = "always" if self.args.fsync_ms <= 1 else "everysec"
        conf.write_text(f"port {port}\ndir {self.dir}\nappendonly yes\n"
                        f"appendfsync {policy}\nsave ''\nprotected-mode no\n")
        self.proc = subprocess.Popen(["redis-server", str(conf)],
                                     stdout=self.log, stderr=self.log, cwd=self.dir)
        self.server_pid = self.proc.pid
        self.r = redis.Redis(host="127.0.0.1", port=port, socket_timeout=120)
        wait_for(lambda: self.r.ping(), 20, "redis", self.proc)

    def write(self, ks, value, batch):
        # MSET, not a pipeline of SETs: it is the same shape of operation as
        # KuttiDB's put_many (one command carrying the whole batch), so the
        # comparison is not decided by how much work each client library does
        # assembling the request.
        done = 0
        for start in range(0, len(ks), batch):
            chunk = ks[start:start + batch]
            self.r.mset({k: value for k in chunk})
            done += len(chunk)
        return done

    def read(self, ks, value, batch):
        done = 0
        for start in range(0, len(ks), batch):
            chunk = ks[start:start + batch]
            got = self.r.mget(chunk)
            if len(got) != len(chunk) or any(g != value for g in got):
                raise RuntimeError(f"value mismatch at {start}")
            done += len(chunk)
        return done

    def close(self):
        stop(self.proc)
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


SYSTEMS = {c.name: c for c in (KuttiCache, RedisCache)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default="kuttidb,redis")
    p.add_argument("--count", type=int, default=200000)
    p.add_argument("--batch", type=int, default=256)
    p.add_argument("--value-size", type=int, default=100)
    p.add_argument("--fsync-ms", type=int, default=1000,
                   help="periodic durability window; 1000 pairs with Redis everysec")
    p.add_argument("--port", type=int, default=18400)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--kuttidb-binary", default="/root/perf/kuttidb-safe")
    p.add_argument("--jsonl", required=True)
    args = p.parse_args()
    ks, value = keys(args.count), b"v" * args.value_size
    port = args.port
    for repeat in range(1, args.repeats + 1):
        for name in [n for n in args.systems.split(",") if n]:
            port += 7
            system = SYSTEMS[name](args)
            try:
                t0 = time.perf_counter()
                system.start(port)
                row = {"system": name, "repeat": repeat, "count": args.count,
                       "batch": args.batch, "value_bytes": args.value_size,
                       "fsync_ms": args.fsync_ms,
                       "startup_s": time.perf_counter() - t0,
                       "idle": usage(system.server_pid),
                       "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                row["publish"] = phase(system.server_pid, args.count,
                                       lambda: system.write(ks, value, args.batch))
                row["consume_ack"] = phase(system.server_pid, args.count,
                                           lambda: system.read(ks, value, args.batch))
                row["data_dir_bytes"] = dir_bytes(system.dir)
                row["verified"] = True
                print(f"  {name}: write={row['publish']['messages_per_second']:9.0f}/s "
                      f"read={row['consume_ack']['messages_per_second']:9.0f}/s "
                      f"srvCPU/M={row['publish'].get('server_cpu_s_per_million', -1):6.2f}s "
                      f"idleRSS={row['idle']['rss_kib'] / 1024:6.1f}MiB "
                      f"loadedRSS={row['consume_ack'].get('server_rss_kib', 0) / 1024:7.1f}MiB",
                      flush=True)
            except Exception as error:
                import traceback
                row = {"system": name, "repeat": repeat, "verified": False,
                       "error": f"{type(error).__name__}: {error}",
                       "traceback": traceback.format_exc()}
                print(f"  {name}: FAILED {row['error']}", flush=True)
            finally:
                system.close()
            record(args.jsonl, row)
            time.sleep(2)
        print(f"repeat {repeat} complete", flush=True)


if __name__ == "__main__":
    main()
