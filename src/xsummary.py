#!/usr/bin/env python3
"""Summarise the cross-server comparison: medians per system, with the
failures listed rather than dropped."""
import json, statistics, sys
from collections import defaultdict

rows = [json.loads(l) for l in open(sys.argv[1])]
ok = defaultdict(list)
bad = defaultdict(list)
for r in rows:
    (ok if r.get("verified") else bad)[r["system"]].append(r)

ORDER = ["kuttidb", "redis", "nats", "rabbitmq", "kafka"]
systems = [s for s in ORDER if s in ok] + [s for s in ok if s not in ORDER]


def med(system, phase, key):
    vals = [r[phase][key] for r in ok[system] if phase in r and key in r[phase]]
    return statistics.median(vals) if vals else None


def fmt(v, spec=",.0f"):
    return "n/a" if v is None else format(v, spec)


n = len(ok[systems[0]]) if systems else 0
print(f"Cross-server durable queue comparison — 100-byte messages, batches of 256")
print(f"trials per system: " + ", ".join(f"{s}={len(ok[s])}" for s in systems))
print()
head = f"{'metric':38s}" + "".join(f"{s:>13s}" for s in systems)
print(head)
print("-" * len(head))
metrics = [
    ("publish msgs/s (median)",            "publish", "messages_per_second", ",.0f"),
    ("consume+ack msgs/s (median)",        "consume_ack", "messages_per_second", ",.0f"),
    ("publish server CPU s / M msgs",      "publish", "server_cpu_s_per_million", ",.1f"),
    ("consume+ack server CPU s / M msgs",  "consume_ack", "server_cpu_s_per_million", ",.1f"),
    ("publish client CPU s / M msgs",      "publish", "client_cpu_s_per_million", ",.1f"),
    ("publish disk bytes / message",       "publish", "server_write_bytes_per_message", ",.0f"),
    ("consume+ack disk bytes / message",   "consume_ack", "server_write_bytes_per_message", ",.0f"),
    ("loaded RSS MiB (after drain)",       "consume_ack", "server_rss_kib", ",.1f"),
    ("peak RSS MiB",                       "consume_ack", "server_peak_rss_kib", ",.1f"),
]
for label, phase, key, spec in metrics:
    cells = []
    for s in systems:
        v = med(s, phase, key)
        if v is not None and "RSS MiB" in label:
            v = v / 1024
        cells.append(fmt(v, spec))
    print(f"{label:38s}" + "".join(f"{c:>13s}" for c in cells))

for label, key, spec, div in (("idle RSS MiB", "rss_kib", ",.1f", 1024),):
    cells = []
    for s in systems:
        vals = [r["idle"][key] for r in ok[s] if r.get("idle")]
        cells.append(fmt(statistics.median(vals) / div if vals else None, spec))
    print(f"{label:38s}" + "".join(f"{c:>13s}" for c in cells))

cells = []
for s in systems:
    vals = [r["startup_s"] for r in ok[s] if "startup_s" in r]
    cells.append(fmt(statistics.median(vals) if vals else None, ",.1f"))
print(f"{'startup to ready (s)':38s}" + "".join(f"{c:>13s}" for c in cells))

cells = []
for s in systems:
    vals = [r["data_dir_bytes"] for r in ok[s] if r.get("data_dir_bytes")]
    cells.append(fmt(statistics.median(vals) / 1e6 if vals else None, ",.1f"))
print(f"{'data dir after drain (MB)':38s}" + "".join(f"{c:>13s}" for c in cells))

if bad:
    print("\nfailed trials (kept, not dropped):")
    for s, items in bad.items():
        for r in items:
            print(f"  {s}: {r.get('error')}")
