#!/usr/bin/env python3
"""SDK-level test for the atomic job completion client surface.

Exercises the typed Python API end to end against a spawned server:
manifest discovery, completion-capable consumption, intent serialization
(persisted before submission), atomic completion, receipt lookup after a
restart, direct state mutations with receipts, and the typed error mapping
(unsupported feature, version conflict, idempotency conflict).
"""
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from kuttidb_client import (  # noqa: E402
    JobCompletionIntent,
    JobDeliveryExpiredError,
    JobIdempotencyConflictError,
    JobStateVersionConflictError,
    JobUnsupportedFeatureError,
    KuttiDBClient,
    KuttiDBError,
)

PORT = int(os.environ.get("KUTTIDB_JOB_SDK_PORT", "0"))


def free_port():
    import socket
    with socket.socket() as sk:
        sk.bind(("127.0.0.1", 0))
        return sk.getsockname()[1]


def start_server(wal_dir, jobs=True):
    args = ["./kuttidb", str(PORT), os.path.join(wal_dir, "kuttidb.wal")]
    if jobs:
        args.append("--job-completion")
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + 10
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited: %s" % proc.stderr.read().decode())
        try:
            with KuttiDBClient(port=PORT) as probe:
                probe.health()
            return proc
        except (KuttiDBError, OSError):
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def main():
    global PORT
    if not PORT:
        PORT = free_port()
    with tempfile.TemporaryDirectory() as wal_dir:
        proc = start_server(wal_dir)
        db = KuttiDBClient(port=PORT)

        caps = db.capabilities()
        assert caps["minor"] >= 8, caps

        # Queues + stable identity discovery
        db.queue_declare("extract-pdf", durable=True)
        db.queue_declare("index-text", durable=True)
        manifest = {q["name"]: q for q in db.queue_manifest()}
        assert manifest["extract-pdf"]["durable"] is True
        in_inc = manifest["extract-pdf"]["incarnation"]
        out_inc = manifest["index-text"]["incarnation"]
        assert in_inc and out_inc

        db.queue_consumer_register("pdf-worker")
        msg_id = db.queue_publish("extract-pdf", b"pdf-bytes")
        delivery = db.job_consume("extract-pdf", "pdf-worker")
        assert delivery is not None and delivery.message_id == msg_id
        assert delivery.queue_incarnation == in_inc
        assert len(delivery.proof) == 16

        # Compose + persist the intent BEFORE submitting (recovery path).
        intent = delivery.to_intent(
            state_key="pdf:42", expected_version=0,
            state_value=b"extracted",
            output_queue="index-text", output_incarnation=out_inc,
            output_value=b"pdf:42")
        persisted = intent.to_json()
        assert JobCompletionIntent.from_json(persisted) == intent

        result = db.job_complete(intent, proof=delivery.proof)
        assert not result.replayed and result.commit_id > 0
        assert result.output_message_id > 0 and result.state_version == 1

        state = db.state_get("pdf:42")
        assert state and state["value"] == b"extracted"
        assert state["version"] == result.state_version
        assert db.queue_stats("extract-pdf")["depth"] == 0
        assert db.queue_stats("index-text")["depth"] == 1
        stats = db.stats()
        assert stats["job_enabled"] == 1
        assert stats["job_state_entries"] == 1
        assert stats["job_completions"] == 1

        receipt = db.job_completion(intent.operation_id)
        assert receipt and receipt.commit_id == result.commit_id

        # Restart: the same persisted intent replays its original result
        # (the proof is stale; receipt lookup precedes lease validation).
        db.close()
        proc.terminate()
        proc.wait()
        proc = start_server(wal_dir)
        db = KuttiDBClient(port=PORT)

        revived = JobCompletionIntent.from_json(persisted)
        replay = db.job_complete(revived, proof=b"\x00" * 16)
        assert replay.replayed and replay.commit_id == result.commit_id
        assert replay.output_message_id == result.output_message_id
        assert db.queue_stats("index-text")["depth"] == 1

        # Direct state mutations with the shared receipt ledger.
        put = db.state_put("pdf:42", b"corrected", expected_version=1)
        assert not put.replayed and put.state_version == 2
        replay_put = db.state_put("pdf:42", b"corrected", expected_version=1,
                                  operation_id=put.operation_id)
        assert replay_put.replayed and replay_put.commit_id == put.commit_id
        conflict = None
        try:
            db.state_put("pdf:42", b"different", expected_version=2,
                         operation_id=put.operation_id)
        except JobIdempotencyConflictError as error:
            conflict = error
        assert conflict is not None and conflict.code == "idempotency_conflict"
        stale = None
        try:
            db.state_put("pdf:42", b"x", expected_version=99)
        except JobStateVersionConflictError as error:
            stale = error
        assert stale is not None and stale.code == "state_version_conflict"
        lookup = db.durable_operation(put.operation_id)
        assert lookup and lookup["kind"] == "state_put"
        assert lookup["state_version"] == 2

        deletion = db.state_delete("pdf:42", expected_version=2)
        assert not deletion.replayed
        assert db.state_get("pdf:42") is None
        retry = db.state_delete("pdf:42", expected_version=2,
                                operation_id=deletion.operation_id)
        assert retry.replayed and retry.commit_id == deletion.commit_id

        # Fencing: a fresh delivery with a tiny lease expires.
        db.queue_publish("extract-pdf", b"second")
        db.queue_consumer_register("pdf-worker")
        second = db.job_consume("extract-pdf", "pdf-worker", visibility=0.001)
        assert second is not None
        time.sleep(0.05)
        expired = None
        try:
            db.job_complete(second.to_intent(
                state_key="pdf:42", expected_version=0, state_value=b"late"),
                proof=second.proof)
        except JobDeliveryExpiredError as error:
            expired = error
        assert expired is not None and expired.code == "delivery_expired"
        db.close()
        proc.terminate()
        proc.wait()

        # Feature disabled: explicit typed error, never a silent fallback.
        # A fresh directory: the used WAL correctly refuses a disabled open.
        with tempfile.TemporaryDirectory() as plain_dir:
            proc = start_server(plain_dir, jobs=False)
            db = KuttiDBClient(port=PORT)
            unsupported = None
            try:
                db.job_consume("extract-pdf", "pdf-worker")
            except JobUnsupportedFeatureError as error:
                unsupported = error
            assert unsupported is not None
            assert unsupported.code == "unsupported_feature"
            db.close()
            proc.terminate()
            proc.wait()
        proc = None
    print("test_job_client: OK")


if __name__ == "__main__":
    main()
