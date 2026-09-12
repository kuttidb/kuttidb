#!/usr/bin/env python3
"""Cross-server durable-queue comparison at a matched durability tier.

Workload, identical for every system: publish COUNT 100-byte messages into
one durable queue, then drain all COUNT with an explicit consumer
acknowledgement, verifying that exactly COUNT come back. Work is submitted in
batches of BATCH so that every system gets the same opportunity to amortise
round trips, and each system is configured so that a publish is not
acknowledged until its bytes are on the device:

  kuttidb   durable queue; publish, delivery and ACK replies each wait for an
            fsync that covers their record.
  redis     Streams with a consumer group; appendonly yes, appendfsync always.
  nats      JetStream file store with sync_interval: always; publish waits for
            the PubAck, consume acknowledges explicitly.
  rabbitmq  durable queue, persistent messages, publisher confirms; consumer
            acknowledges.
  kafka     single broker, acks=all with flush.messages=1 so each batch is
            flushed to disk; consumer commits offsets.

Every server is started as a child of this process so its CPU, RSS and write
counters can be read from /proc for its whole process tree.
"""
import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, "/root/perf")
sys.path.insert(0, "/root/perf/work/src")
from xbench_core import VALUE, dir_bytes, phase, record, stop, usage, wait_for

DATA_ROOT = "/root/perf/data"


class System:
    name = "?"

    def __init__(self, args):
        self.args = args
        self.dir = tempfile.mkdtemp(prefix=f"x-{self.name}-", dir=DATA_ROOT)
        self.proc = None
        self.log = open(Path(self.dir) / "server.log", "w+")

    server_pid = None      # the process whose tree is measured

    def data_bytes(self):
        return dir_bytes(self.dir)

    def spawn(self, command, env=None):
        self.proc = subprocess.Popen(command, stdout=self.log, stderr=self.log,
                                     env=env, cwd=self.dir)
        self.server_pid = self.proc.pid
        return self.proc

    def close(self):
        stop(self.proc)
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


# --------------------------------------------------------------------------- kuttidb
class KuttiDB(System):
    name = "kuttidb"

    def start(self, port):
        from kuttidb_client import KuttiDBClient
        self.spawn([self.args.kuttidb_binary, str(port),
                    str(Path(self.dir) / "cache.wal"), "100", "--threads", "1"])

        def ready():
            with KuttiDBClient(port=port, timeout=0.5) as db:
                db.health()
            return True
        wait_for(ready, 20, "kuttidb", self.proc)
        self.db = KuttiDBClient(port=port, timeout=120)
        self.db.__enter__()
        self.db.queue_declare("bench", durable=True)
        self.db.queue_prefetch(self.args.batch * 4)

    def publish(self, count, batch):
        done = 0
        for start in range(0, count, batch):
            n = min(batch, count - start)
            done += len(self.db.queue_publish_batch("bench", [VALUE] * n))
        return done

    def consume_ack(self, count, batch):
        got = 0
        while got < count:
            messages = self.db.queue_consume_batch("bench", batch)
            if not messages:
                break
            assert all(m["value"] == VALUE for m in messages), "payload mismatch"
            got += self.db.queue_ack_batch("bench", [m["id"] for m in messages])
        return got

    def close(self):
        try:
            self.db.__exit__(None, None, None)
        except Exception:
            pass
        super().close()


# --------------------------------------------------------------------------- redis
class Redis(System):
    name = "redis"

    def start(self, port):
        import redis
        conf = Path(self.dir) / "redis.conf"
        conf.write_text(
            f"port {port}\ndir {self.dir}\nappendonly yes\nappendfsync always\n"
            "save ''\nprotected-mode no\ndaemonize no\n")
        self.spawn(["redis-server", str(conf)])
        self.r = redis.Redis(host="127.0.0.1", port=port, socket_timeout=120)
        wait_for(lambda: self.r.ping(), 20, "redis", self.proc)
        self.r.xgroup_create("bench", "g", id="0", mkstream=True)

    def publish(self, count, batch):
        done = 0
        for start in range(0, count, batch):
            n = min(batch, count - start)
            pipe = self.r.pipeline(transaction=False)
            for _ in range(n):
                pipe.xadd("bench", {b"v": VALUE})
            done += len(pipe.execute())
        return done

    def consume_ack(self, count, batch):
        got = 0
        while got < count:
            entries = self.r.xreadgroup("g", "c", {"bench": ">"}, count=batch)
            if not entries:
                break
            ids = [i for _, items in entries for i, _ in items]
            assert all(f[b"v"] == VALUE for _, items in entries for _, f in items)
            self.r.xack("bench", "g", *ids)
            got += len(ids)
        return got


# --------------------------------------------------------------------------- nats
class Nats(System):
    name = "nats"

    def start(self, port):
        import asyncio
        conf = Path(self.dir) / "nats.conf"
        conf.write_text(
            f"port: {port}\n"
            f"jetstream {{ store_dir: \"{self.dir}/js\", sync_interval: always }}\n")
        self.spawn(["nats-server", "-c", str(conf)])
        wait_for(lambda: socket.create_connection(("127.0.0.1", port), 0.5).close() or True,
                 20, "nats", self.proc)
        self.loop = asyncio.new_event_loop()
        self.port = port
        self.loop.run_until_complete(self._connect())

    async def _connect(self):
        import nats
        self.nc = await nats.connect(f"nats://127.0.0.1:{self.port}",
                                     max_reconnect_attempts=1,
                                     pending_size=1 << 26, flush_timeout=600)
        self.js = self.nc.jetstream(timeout=600)
        await self.js.add_stream(name="bench", subjects=["bench"], storage="file",
                                 retention="workqueue")
        self.sub = await self.js.pull_subscribe("bench", durable="c", stream="bench")

    def publish(self, count, batch):
        return self.loop.run_until_complete(self._publish(count, batch))

    async def _publish(self, count, batch):
        import asyncio
        done = 0
        for start in range(0, count, batch):
            n = min(batch, count - start)
            acks = [await self.js.publish_async("bench", VALUE) for _ in range(n)]
            done += len(await asyncio.gather(*acks))
        return done

    def consume_ack(self, count, batch):
        return self.loop.run_until_complete(self._consume(count, batch))

    async def _consume(self, count, batch):
        import asyncio
        got = 0
        while got < count:
            msgs = await self.sub.fetch(min(batch, count - got), timeout=120)
            if not msgs:
                break
            assert all(m.data == VALUE for m in msgs), "payload mismatch"
            # ack_sync defaults to a 1s deadline; with sync_interval: always
            # a batch of acknowledgements queues behind that many fsyncs, so
            # the deadline has to allow for the device, not the library default.
            await asyncio.gather(*[m.ack_sync(timeout=120) for m in msgs])
            got += len(msgs)
        return got

    def close(self):
        try:
            self.loop.run_until_complete(self.nc.close())
            self.loop.close()
        except Exception:
            pass
        super().close()


# --------------------------------------------------------------------------- rabbitmq
class Rabbit(System):
    """Driven through its packaged systemd unit: the Debian build expects its
    own environment and boot sequence, so the unit is started here and the
    broker's process tree is measured by PID. The unit is left stopped
    afterwards, which is how this host had it."""
    name = "rabbitmq"
    DATA = "/var/lib/rabbitmq"

    def start(self, port):
        import pika
        subprocess.run(["systemctl", "start", "rabbitmq-server"], check=True,
                       capture_output=True)
        params = pika.ConnectionParameters(
            host="127.0.0.1", port=5672, heartbeat=0,
            blocked_connection_timeout=600, socket_timeout=600)

        def ready():
            pika.BlockingConnection(params).close()
            return True
        wait_for(ready, 180, "rabbitmq")
        out = subprocess.check_output(
            ["systemctl", "show", "-p", "MainPID", "--value", "rabbitmq-server"],
            text=True).strip()
        self.server_pid = int(out)
        if not self.server_pid:
            raise RuntimeError("rabbitmq-server has no MainPID")
        self.queue = f"xbench{port}"
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        self.ch.queue_delete(queue=self.queue)
        self.ch.queue_declare(queue=self.queue, durable=True)
        self.ch.confirm_delivery()                            # publisher confirms
        self.props = pika.BasicProperties(delivery_mode=2)    # persistent

    def publish(self, count, batch):
        # BlockingConnection confirms each publish before returning, so this is
        # one fsync-covered confirm per message: the strict tier for RabbitMQ.
        for _ in range(count):
            self.ch.basic_publish("", self.queue, VALUE, properties=self.props)
        return count

    def consume_ack(self, count, batch):
        got = 0
        for method, _, body in self.ch.consume(self.queue, inactivity_timeout=60):
            if method is None:
                break
            assert body == VALUE, "payload mismatch"
            got += 1
            if got % batch == 0 or got == count:
                self.ch.basic_ack(method.delivery_tag, multiple=True)
            if got >= count:
                break
        self.ch.cancel()
        return got

    def data_bytes(self):
        return dir_bytes(self.DATA)

    def close(self):
        try:
            self.ch.queue_delete(queue=self.queue)
            self.conn.close()
        except Exception:
            pass
        subprocess.run(["systemctl", "stop", "rabbitmq-server"], capture_output=True)
        self.log.close()
        shutil.rmtree(self.dir, ignore_errors=True)


# --------------------------------------------------------------------------- kafka
class Kafka(System):
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
            "num.partitions=1\n"
            # Durability tier: flush every record to disk before it counts.
            "log.flush.interval.messages=1\nlog.flush.interval.ms=0\n"
            "log.preallocate=false\nlog.segment.bytes=268435456\n")
        cluster_id = subprocess.check_output(
            [f"{self.HOME}/bin/kafka-storage.sh", "random-uuid"], text=True).strip()
        subprocess.run([f"{self.HOME}/bin/kafka-storage.sh", "format", "-t",
                        cluster_id, "-c", str(props), "--standalone"],
                       check=True, capture_output=True)
        self.spawn([f"{self.HOME}/bin/kafka-server-start.sh", str(props)],
                   env=dict(os.environ, KAFKA_HEAP_OPTS="-Xmx512M -Xms512M",
                            LOG_DIR=f"{self.dir}/klog"))
        self.bootstrap = f"127.0.0.1:{port}"

        def ready():
            admin.AdminClient({"bootstrap.servers": self.bootstrap}).list_topics(timeout=3)
            return True
        wait_for(ready, 180, "kafka", self.proc)
        a = admin.AdminClient({"bootstrap.servers": self.bootstrap})
        list(a.create_topics([admin.NewTopic("bench", 1, 1)]).values())[0].result()
        self.producer = Producer({"bootstrap.servers": self.bootstrap, "acks": "all",
                                  "linger.ms": 5, "enable.idempotence": False})
        self.consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "g",
                                  "auto.offset.reset": "earliest",
                                  "enable.auto.commit": False})
        self.consumer.subscribe(["bench"])

    def publish(self, count, batch):
        self.delivered = 0

        def on_delivery(err, _msg):
            if err is None:
                self.delivered += 1
        for start in range(0, count, batch):
            for _ in range(min(batch, count - start)):
                self.producer.produce("bench", VALUE, callback=on_delivery)
            self.producer.flush()
        return self.delivered

    def consume_ack(self, count, batch):
        got = 0
        while got < count:
            msgs = self.consumer.consume(min(batch, count - got), timeout=30)
            if not msgs:
                break
            for m in msgs:
                if m.error():
                    raise RuntimeError(m.error())
                assert m.value() == VALUE, "payload mismatch"
            got += len(msgs)
            self.consumer.commit(asynchronous=False)
        return got

    def close(self):
        try:
            self.consumer.close()
        except Exception:
            pass
        super().close()


SYSTEMS = {c.name: c for c in (KuttiDB, Redis, Nats, Rabbit, Kafka)}


def run_one(cls, args, port):
    system = cls(args)
    try:
        idle_start = time.perf_counter()
        system.start(port)
        row = {"system": cls.name, "count": args.count, "batch": args.batch,
               "value_bytes": len(VALUE), "startup_s": time.perf_counter() - idle_start,
               "idle": usage(system.server_pid),
               "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        row["publish"] = phase(system.server_pid, args.count,
                               lambda: system.publish(args.count, args.batch))
        row["consume_ack"] = phase(system.server_pid, args.count,
                                   lambda: system.consume_ack(args.count, args.batch))
        row["data_dir_bytes"] = system.data_bytes()
        row["verified"] = True
        return row
    finally:
        system.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--systems", default="kuttidb,redis,nats,rabbitmq,kafka")
    p.add_argument("--count", type=int, default=100000)
    p.add_argument("--batch", type=int, default=256)
    p.add_argument("--port", type=int, default=18100)
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--kuttidb-binary", default="/root/perf/kuttidb-ckpt")
    p.add_argument("--jsonl", required=True, help="appended; keeps every trial")
    args = p.parse_args()
    names = [n for n in args.systems.split(",") if n]
    port = args.port
    for repeat in range(1, args.repeats + 1):
        for name in names:
            port += 7
            try:
                row = run_one(SYSTEMS[name], args, port)
            except Exception as error:
                import traceback
                row = {"system": name, "repeat": repeat, "verified": False,
                       "error": f"{type(error).__name__}: {error}",
                       "traceback": traceback.format_exc()}
                print(f"  {name}: FAILED {row['error']}", flush=True)
            else:
                row["repeat"] = repeat
                print(f"  {name}: publish={row['publish']['messages_per_second']:8.0f}/s "
                      f"consume+ack={row['consume_ack']['messages_per_second']:8.0f}/s "
                      f"srvCPU/M={row['publish'].get('server_cpu_s_per_million', -1):6.2f}s "
                      f"idleRSS={row['idle']['rss_kib'] / 1024:7.1f}MiB", flush=True)
            record(args.jsonl, row)
            time.sleep(3)
        print(f"repeat {repeat} complete", flush=True)


if __name__ == "__main__":
    main()
