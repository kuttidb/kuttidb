#!/usr/bin/env python3
"""Stream (append-only partitioned log) comparison at matched durability.

Workload, identical for every system: append COUNT 100-byte records to one
topic with PARTITIONS partitions in batches of BATCH, then read all COUNT
back from offset zero in batches of BATCH, committing the group offset after
each batch. Payloads and the total count are verified.

Durability tier — every append is on the device before it is acknowledged:

  kuttidb   stream append replies wait for an fsync covering their record
  kafka     acks=all with log.flush.interval.messages=1
  redis     Streams, appendonly yes with appendfsync always

Server CPU, RSS and write counters come from /proc for the server's whole
process tree, so the figures do not depend on the client library.
"""
import argparse, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

sys.path.insert(0, "/root/perf")
sys.path.insert(0, "/root/perf/work/src")
from xbench_core import dir_bytes, phase, record, stop, usage, wait_for

DATA_ROOT = "/root/perf/data"
VALUE = b"s" * 100


class Base:
    def __init__(self, args):
        self.args = args
        self.dir = tempfile.mkdtemp(prefix=f"st-{self.name}-", dir=DATA_ROOT)
        self.log = open(Path(self.dir) / "server.log", "w+")
        self.proc = None

    def data_bytes(self):
        return dir_bytes(self.dir)

    def close(self):
        stop(self.proc)
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class KuttiStream(Base):
    name = "kuttidb"

    def start(self, port):
        from kuttidb_client import KuttiDBClient
        self.proc = subprocess.Popen(
            [self.args.kuttidb_binary, str(port), str(Path(self.dir) / "cache.wal"),
             "100", "--threads", "1"], stdout=self.log, stderr=self.log, cwd=self.dir)
        self.server_pid = self.proc.pid

        def ready():
            with KuttiDBClient(port=port, timeout=0.5) as db:
                db.health()
            return True
        wait_for(ready, 20, "kuttidb", self.proc)
        self.db = KuttiDBClient(port=port, timeout=120)
        self.db.__enter__()
        self.db.stream_declare("bench", partitions=self.args.partitions)
        # A committing consumer joins the group first, exactly as an
        # application would; the join also fixes the assignment this client
        # is allowed to commit for.
        self.db.stream_group_join("bench", "g", lease=60.0)   # 60s is the protocol maximum

    def append(self, count, batch):
        done = 0
        for start in range(0, count, batch):
            n = min(batch, count - start)
            # Explicit partition so records spread evenly and the read phase
            # knows exactly how many to expect per partition.
            part = (start // batch) % self.args.partitions
            done += len(self.db.stream_append_many("bench", [VALUE] * n, partition=part))
        return done

    def read(self, count, batch):
        got = 0
        for part in range(self.args.partitions):
            offset = 0
            while True:
                items = self.db.stream_fetch("bench", partition=part, offset=offset,
                                             max_records=batch)
                if not items:
                    break
                for item in items:
                    if item["value"] != VALUE:
                        raise RuntimeError("payload mismatch")
                offset = items[-1]["offset"] + 1
                got += len(items)
                self.db.stream_commit_batch("bench", "g", [(part, offset)])
        return got

    def close(self):
        try:
            self.db.__exit__(None, None, None)
        except Exception:
            pass
        super().close()


class RedisStream(Base):
    name = "redis"

    def start(self, port):
        import redis
        conf = Path(self.dir) / "redis.conf"
        conf.write_text(f"port {port}\ndir {self.dir}\nappendonly yes\n"
                        "appendfsync always\nsave ''\nprotected-mode no\n")
        self.proc = subprocess.Popen(["redis-server", str(conf)],
                                     stdout=self.log, stderr=self.log, cwd=self.dir)
        self.server_pid = self.proc.pid
        self.r = redis.Redis(host="127.0.0.1", port=port, socket_timeout=120)
        wait_for(lambda: self.r.ping(), 20, "redis", self.proc)
        # One Redis stream key per partition, mirroring the partition count.
        self.keys = [f"bench:{p}" for p in range(self.args.partitions)]

    def append(self, count, batch):
        done = 0
        for start in range(0, count, batch):
            n = min(batch, count - start)
            key = self.keys[(start // batch) % len(self.keys)]
            pipe = self.r.pipeline(transaction=False)
            for _ in range(n):
                pipe.xadd(key, {b"v": VALUE})
            done += len(pipe.execute())
        return done

    def read(self, count, batch):
        got = 0
        for key in self.keys:
            last = "-"
            while True:
                items = self.r.xrange(key, min=last, count=batch)
                if last != "-" and items:
                    items = items[1:]
                if not items:
                    break
                for _, fields in items:
                    if fields[b"v"] != VALUE:
                        raise RuntimeError("payload mismatch")
                last = items[-1][0].decode()
                got += len(items)
                # Durable offset record, the analogue of a group commit.
                self.r.set(f"off:{key}", last)
        return got


class KafkaStream(Base):
    name = "kafka"
    HOME = "/root/kafka_2.13-4.1.2"

    def start(self, port):
        from confluent_kafka import Producer, Consumer, admin
        ctl = port + 1000
        props = Path(self.dir) / "server.properties"
        props.write_text(
            "process.roles=broker,controller\nnode.id=1\n"
            f"controller.quorum.bootstrap.servers=127.0.0.1:{ctl}\n"
            f"listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{ctl}\n"
            f"advertised.listeners=PLAINTEXT://127.0.0.1:{port}\n"
            "controller.listener.names=CONTROLLER\n"
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n"
            f"log.dirs={self.dir}/logs\n"
            "offsets.topic.replication.factor=1\n"
            "transaction.state.log.replication.factor=1\n"
            "transaction.state.log.min.isr=1\n"
            f"num.partitions={self.args.partitions}\n"
            "log.flush.interval.messages=1\nlog.flush.interval.ms=0\n"
            "log.preallocate=false\n")
        cluster = subprocess.check_output(
            [f"{self.HOME}/bin/kafka-storage.sh", "random-uuid"], text=True).strip()
        subprocess.run([f"{self.HOME}/bin/kafka-storage.sh", "format", "-t", cluster,
                        "-c", str(props), "--standalone"], check=True, capture_output=True)
        self.proc = subprocess.Popen(
            [f"{self.HOME}/bin/kafka-server-start.sh", str(props)],
            stdout=self.log, stderr=self.log, cwd=self.dir,
            env=dict(os.environ, KAFKA_HEAP_OPTS="-Xmx512M -Xms512M",
                     LOG_DIR=f"{self.dir}/klog"))
        self.server_pid = self.proc.pid
        self.bootstrap = f"127.0.0.1:{port}"

        def ready():
            admin.AdminClient({"bootstrap.servers": self.bootstrap}).list_topics(timeout=3)
            return True
        wait_for(ready, 180, "kafka", self.proc)
        a = admin.AdminClient({"bootstrap.servers": self.bootstrap})
        list(a.create_topics([admin.NewTopic("bench", self.args.partitions, 1)]).values())[0].result()
        self.producer = Producer({"bootstrap.servers": self.bootstrap, "acks": "all",
                                  "linger.ms": 5})
        self.consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "g",
                                  "auto.offset.reset": "earliest",
                                  "enable.auto.commit": False})
        self.consumer.subscribe(["bench"])

    def append(self, count, batch):
        self.delivered = 0

        def on_delivery(err, _msg):
            if err is None:
                self.delivered += 1
        for start in range(0, count, batch):
            part = (start // batch) % self.args.partitions
            for _ in range(min(batch, count - start)):
                self.producer.produce("bench", VALUE, partition=part, callback=on_delivery)
            self.producer.flush()
        return self.delivered

    def read(self, count, batch):
        got = 0
        while got < count:
            msgs = self.consumer.consume(min(batch, count - got), timeout=30)
            if not msgs:
                break
            for m in msgs:
                if m.error():
                    raise RuntimeError(m.error())
                if m.value() != VALUE:
                    raise RuntimeError("payload mismatch")
            got += len(msgs)
            self.consumer.commit(asynchronous=False)
        return got

    def close(self):
        try:
            self.consumer.close()
        except Exception:
            pass
        super().close()


SYSTEMS = {c.name: c for c in (KuttiStream, RedisStream, KafkaStream)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default="kuttidb,redis,kafka")
    p.add_argument("--count", type=int, default=100000)
    p.add_argument("--batch", type=int, default=256)
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--port", type=int, default=18600)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--kuttidb-binary", default="/root/perf/kuttidb-safe")
    p.add_argument("--jsonl", required=True)
    args = p.parse_args()
    port = args.port
    for repeat in range(1, args.repeats + 1):
        for name in [n for n in args.systems.split(",") if n]:
            port += 7
            system = SYSTEMS[name](args)
            try:
                t0 = time.perf_counter()
                system.start(port)
                row = {"system": name, "repeat": repeat, "count": args.count,
                       "batch": args.batch, "partitions": args.partitions,
                       "startup_s": time.perf_counter() - t0,
                       "idle": usage(system.server_pid),
                       "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                row["publish"] = phase(system.server_pid, args.count,
                                       lambda: system.append(args.count, args.batch))
                row["consume_ack"] = phase(system.server_pid, args.count,
                                           lambda: system.read(args.count, args.batch))
                row["data_dir_bytes"] = system.data_bytes()
                row["verified"] = True
                print(f"  {name}: append={row['publish']['messages_per_second']:9.0f}/s "
                      f"read={row['consume_ack']['messages_per_second']:9.0f}/s "
                      f"srvCPU/M={row['publish'].get('server_cpu_s_per_million', -1):7.2f}s "
                      f"idleRSS={row['idle']['rss_kib'] / 1024:6.1f}MiB", flush=True)
            except Exception as error:
                import traceback
                row = {"system": name, "repeat": repeat, "verified": False,
                       "error": f"{type(error).__name__}: {error}",
                       "traceback": traceback.format_exc()}
                print(f"  {name}: FAILED {row['error']}", flush=True)
            finally:
                system.close()
            record(args.jsonl, row)
            time.sleep(3)
        print(f"repeat {repeat} complete", flush=True)


if __name__ == "__main__":
    main()
