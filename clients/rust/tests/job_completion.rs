//! Integration tests for the atomic job completion client surface
//! (opcodes 0x70–0x77, capability bit `CAP_JOBS`).
//!
//! Builds the server if needed (`make -j8` at the repository root, or the
//! path in `KUTTIDB_SERVER`), spawns `./kuttidb <port> <wal> --job-completion`
//! on an ephemeral port, and exercises manifest discovery, completion-capable
//! consumption, atomic completion, receipt lookup across a restart, direct
//! state mutations with receipts, the typed error envelope, the Pool path,
//! and the unsupported-feature error of a server without the feature.

use kuttidb::{
    Client, Error, JobCode, JobCompletionIntent, JobOutput, ManagedOptions, ManagedTransport, Pool,
    QueueOptions, StateOptions, FEATURE_JOBS, OperationKind,
};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::OnceLock;
use std::time::{Duration, UNIX_EPOCH};

fn server_path() -> PathBuf {
    static SERVER: OnceLock<PathBuf> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            if let Some(from_env) = std::env::var_os("KUTTIDB_SERVER") {
                return PathBuf::from(from_env);
            }
            let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let binary = repo.join("kuttidb");
            if !binary.exists() {
                let status = Command::new("make")
                    .arg("-j8")
                    .current_dir(&repo)
                    .status()
                    .expect("failed to run make at the repository root");
                assert!(status.success(), "make -j8 failed");
            }
            assert!(
                binary.exists(),
                "server binary missing at {} — run make at the repo root",
                binary.display()
            );
            binary
        })
        .clone()
}

/// macOS $TMPDIR pushes paths past the 104-byte sockaddr_un limit, so prefer
/// /tmp (mirrors tests/managed_integration.rs).
fn temp_dir(label: &str) -> PathBuf {
    let root = if std::path::Path::new("/tmp").is_dir() {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = root.join(format!(
        "kuttidb-rust-job-{label}-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn start_server(wal_dir: &std::path::Path, jobs: bool) -> (Child, u16) {
    let port = free_port();
    let wal = wal_dir.join("kuttidb.wal");
    let mut args = vec![port.to_string(), wal.display().to_string()];
    if jobs {
        args.push("--job-completion".to_owned());
    }
    let mut child = Command::new(server_path())
        .args(&args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn kuttidb");
    let addr = format!("127.0.0.1:{port}");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            let stderr = child
                .stderr
                .take()
                .map(|mut e| {
                    use std::io::Read;
                    let mut buf = String::new();
                    let _ = e.read_to_string(&mut buf);
                    buf
                })
                .unwrap_or_default();
            panic!("server exited early ({status}): {stderr}");
        }
        if let Ok(mut client) = Client::connect(&addr) {
            if client.health().unwrap_or(false) {
                return (child, port);
            }
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("server did not become healthy on {addr}");
        }
        std::thread::sleep(Duration::from_millis(30));
    }
}

/// SIGTERM so the server checkpoints and exits gracefully before a respawn
/// on the same WAL.
fn stop_server(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();
    let _ = child.wait();
}

fn expect_job_code(error: Error, code: JobCode) -> String {
    let job = error
        .as_job()
        .unwrap_or_else(|| panic!("expected a typed job error, got {error:?}"));
    assert_eq!(job.code, code, "wrong job error code in {job:?}");
    job.detail.clone().unwrap_or_default()
}

#[test]
fn job_completion_end_to_end() {
    let dir = temp_dir("e2e");
    let (mut child, port) = start_server(&dir, true);
    let addr = format!("127.0.0.1:{port}");

    let mut db = Client::connect(&addr).expect("connect");
    let caps = db.capabilities().expect("capabilities");
    assert!(caps.minor >= 8, "protocol minor too old: {}", caps.minor);
    assert_ne!(caps.features & FEATURE_JOBS, 0, "CAP_JOBS missing");

    // Manifest: stable identity (incarnation) for completion intents.
    db.queue_declare("extract-pdf", QueueOptions::default())
        .unwrap();
    db.queue_declare("index-text", QueueOptions::default())
        .unwrap();
    db.queue_declare("pool-jobs", QueueOptions::default()).unwrap();
    let manifest = db.queue_manifest().unwrap();
    let in_inc = manifest
        .iter()
        .find(|q| q.name == "extract-pdf")
        .expect("input queue in manifest")
        .clone();
    let out_inc = manifest
        .iter()
        .find(|q| q.name == "index-text")
        .expect("output queue in manifest")
        .clone();
    assert!(in_inc.durable && out_inc.durable);
    assert!(in_inc.incarnation > 0 && out_inc.incarnation > 0);

    // Consume: delivery identity + one-use proof.
    db.queue_consumer_register("pdf-worker").unwrap();
    let message_id = db.queue_publish("extract-pdf", b"pdf-bytes", None).unwrap();
    let delivery = db
        .job_consume("extract-pdf", "pdf-worker", Duration::from_secs(30))
        .expect("job consume")
        .expect("delivery");
    assert_eq!(delivery.message_id, message_id);
    assert_eq!(delivery.queue_incarnation, in_inc.incarnation);
    assert_eq!(delivery.value, b"pdf-bytes");

    // Compose + persist the intent BEFORE submitting (recovery path).
    let intent = delivery.to_intent(
        b"pdf:42".to_vec(),
        0,
        b"extracted".to_vec(),
        Some(JobOutput {
            queue: "index-text".to_owned(),
            queue_incarnation: out_inc.incarnation,
            value: b"pdf:42".to_vec(),
        }),
    );
    let persisted = intent.to_json_string();
    let revived = JobCompletionIntent::from_json_string(&persisted).expect("parse intent");
    assert_eq!(revived, intent);

    // Lossless JSON: 64-bit fields stay decimal strings beyond 2^53.
    let huge = JobCompletionIntent::new(
        "extract-pdf",
        u64::MAX,
        9_007_199_254_740_993, // 2^53 + 1
        b"pdf:42".to_vec(),
        u64::MAX,
        b"payload".to_vec(),
        None,
    );
    let round = JobCompletionIntent::from_json_string(&huge.to_json_string()).expect("parse");
    assert_eq!(round, huge);
    let text = huge.to_json_string();
    assert!(
        text.contains("\"queue_incarnation\":\"18446744073709551615\"")
            && text.contains("\"message_id\":\"9007199254740993\"")
            && text.contains("\"expected_version\":\"18446744073709551615\""),
        "64-bit fields must be decimal strings: {text}"
    );

    // Atomic completion: state PUT + ACK + output publish + receipt.
    let result = db.job_complete(&intent, &delivery.proof).expect("complete");
    assert!(!result.replayed);
    assert!(result.commit_id > 0);
    assert!(result.output_message_id > 0);
    assert_eq!(result.state_version, 1);
    assert!(result.receipt_expires_at_ms > result.completed_at_ms);

    // State read-back and queue effects.
    let state = db.state_get(b"pdf:42").expect("state get").expect("hit");
    assert_eq!(state.value, b"extracted");
    assert_eq!(state.version, result.state_version);
    assert_eq!(state.commit_id, result.commit_id);
    assert_eq!(db.queue_stats("extract-pdf").unwrap().unwrap().depth, 0);
    assert_eq!(db.queue_stats("index-text").unwrap().unwrap().depth, 1);

    let receipt = db
        .job_completion(intent.operation_id)
        .expect("receipt lookup")
        .expect("retained receipt");
    assert_eq!(receipt.commit_id, result.commit_id);
    assert_eq!(receipt.state_version, result.state_version);
    assert_eq!(receipt.output_message_id, result.output_message_id);
    assert_eq!(receipt.operation_id, intent.operation_id);

    // RESTART on the same WAL: the persisted intent replays its original
    // result (the proof is stale; receipt lookup precedes lease validation).
    stop_server(&mut child);
    let (mut child, port) = start_server(&dir, true);
    let addr = format!("127.0.0.1:{port}");
    let mut db = Client::connect(&addr).expect("reconnect");

    let revived = JobCompletionIntent::from_json_string(&persisted).expect("parse intent");
    let replay = db
        .job_complete(&revived, &[0u8; 16])
        .expect("replay after restart");
    assert!(replay.replayed);
    assert_eq!(replay.commit_id, result.commit_id);
    assert_eq!(replay.state_version, result.state_version);
    assert_eq!(replay.output_message_id, result.output_message_id);
    assert_eq!(db.queue_stats("index-text").unwrap().unwrap().depth, 1);
    let manifest = db.queue_manifest().unwrap();
    let re_inc = manifest
        .iter()
        .find(|q| q.name == "extract-pdf")
        .expect("input queue after restart")
        .clone();
    assert_eq!(re_inc.incarnation, in_inc.incarnation);

    // Direct state mutations with the shared receipt ledger.
    let put_options = StateOptions::new(1);
    let put = db
        .state_put(b"pdf:42", b"corrected", put_options)
        .expect("state put");
    assert!(!put.replayed);
    assert_eq!(put.state_version, 2);
    assert_eq!(put.kind, OperationKind::StatePut);
    let replay_put = db
        .state_put(b"pdf:42", b"corrected", StateOptions::with_operation_id(1, put_options.operation_id))
        .expect("state put replay");
    assert!(replay_put.replayed);
    assert_eq!(replay_put.commit_id, put.commit_id);
    assert_eq!(replay_put.state_version, put.state_version);

    // Same id, different content: a stop-and-reconcile conflict.
    let conflict = db
        .state_put(b"pdf:42", b"different", StateOptions::with_operation_id(2, put_options.operation_id))
        .unwrap_err();
    expect_job_code(conflict, JobCode::IdempotencyConflict);
    // Stale expected version: a typed version conflict, delivery untouched.
    let stale = db
        .state_put(b"pdf:42", b"x", StateOptions::new(99))
        .unwrap_err();
    expect_job_code(stale, JobCode::StateVersionConflict);

    let lookup = db
        .durable_operation(put_options.operation_id)
        .expect("durable operation lookup")
        .expect("retained mutation receipt");
    assert_eq!(lookup.kind, OperationKind::StatePut);
    assert_eq!(lookup.state_version, 2);
    assert_eq!(lookup.commit_id, put.commit_id);

    let delete_options = StateOptions::new(2);
    let deletion = db.state_delete(b"pdf:42", delete_options).expect("delete");
    assert!(!deletion.replayed);
    assert!(db.state_get(b"pdf:42").expect("state get").is_none());
    let retry = db
        .state_delete(b"pdf:42", StateOptions::with_operation_id(2, delete_options.operation_id))
        .expect("delete replay");
    assert!(retry.replayed);
    assert_eq!(retry.commit_id, deletion.commit_id);

    // Fencing: a fresh delivery with a tiny lease cannot commit. The exact
    // typed code depends on whether the background sweeper already reaped the
    // expired in-flight delivery (then the queue fence answers not_owned);
    // both outcomes are fail-closed, pre-commit, and never silent.
    db.queue_consumer_register("pdf-worker").unwrap();
    db.queue_publish("extract-pdf", b"second", None).unwrap();
    let second = db
        .job_consume("extract-pdf", "pdf-worker", Duration::from_millis(1))
        .expect("job consume")
        .expect("delivery");
    std::thread::sleep(Duration::from_millis(50));
    let fenced = db
        .job_complete(
            &second.to_intent(b"pdf:42".to_vec(), 0, b"late".to_vec(), None),
            &second.proof,
        )
        .unwrap_err();
    let job = fenced
        .as_job()
        .unwrap_or_else(|| panic!("expected a typed job error, got {fenced:?}"));
    assert!(
        matches!(job.code, JobCode::DeliveryExpired | JobCode::DeliveryNotOwned),
        "expected a fencing rejection, got {job:?}"
    );
    assert!(job.is_not_committed());

    // Pool path (size > 1): consume and complete on two distinct pooled
    // connections — the consumer's owner token, not the socket, owns it.
    // A dedicated queue keeps the reaped fencing message out of the way.
    db.queue_consumer_register("pool-worker").unwrap();
    db.queue_publish("pool-jobs", b"pooled", None).unwrap();
    let pool_inc = db
        .queue_manifest()
        .unwrap()
        .iter()
        .find(|q| q.name == "pool-jobs")
        .expect("pool queue in manifest")
        .incarnation;
    let pool = Pool::new(&addr, 2).expect("pool");
    let pool_result = pool
        .with(|c1| {
            let pooled = c1
                .job_consume("pool-jobs", "pool-worker", Duration::from_secs(30))
                .expect("pooled consume")
                .expect("pooled delivery");
            assert_eq!(pooled.value, b"pooled");
            let pooled_intent = JobCompletionIntent::new(
                "pool-jobs",
                pool_inc,
                pooled.message_id,
                b"pdf:42".to_vec(),
                0,
                b"pool-done".to_vec(),
                None,
            );
            // Second checkout: a different connection from the same pool.
            pool.with(|c2| c2.job_complete(&pooled_intent, &pooled.proof))
        })
        .expect("pooled completion");
    assert!(!pool_result.replayed);
    // The version keeps the key's full history (create, put, delete, create).
    assert!(pool_result.state_version > 1);
    assert!(pool_result.commit_id > 0);
    let pooled_state = db
        .state_get(b"pdf:42")
        .expect("pooled state get")
        .expect("pooled state hit");
    assert_eq!(pooled_state.value, b"pool-done");
    assert_eq!(pooled_state.version, pool_result.state_version);
    assert_eq!(pooled_state.commit_id, pool_result.commit_id);

    drop(db);
    drop(pool);
    stop_server(&mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn job_completion_unsupported_feature() {
    // Fresh directory: a used WAL refuses a disabled open after records.
    let dir = temp_dir("plain");
    let (mut child, port) = start_server(&dir, false);
    let addr = format!("127.0.0.1:{port}");
    let mut db = Client::connect(&addr).expect("connect");
    let caps = db.capabilities().expect("capabilities");
    assert_eq!(caps.features & FEATURE_JOBS, 0, "CAP_JOBS must be off");

    let error = db
        .job_consume("extract-pdf", "pdf-worker", Duration::from_secs(30))
        .unwrap_err();
    let detail = expect_job_code(error, JobCode::UnsupportedFeature);
    assert!(detail.contains("job-completion"), "detail: {detail}");
    let job = db
        .job_completion([7u8; 16])
        .unwrap_err();
    expect_job_code(job, JobCode::UnsupportedFeature);
    drop(db);
    stop_server(&mut child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn managed_job_options_propagate() {
    let nonce = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = temp_dir("managed").join(format!("managed-{nonce}"));
    let mut db = Client::connect_managed(ManagedOptions {
        data_dir: data_dir.clone(),
        executable: Some(server_path()),
        transport: ManagedTransport::Unix,
        idle_timeout: Duration::from_millis(400),
        startup_timeout: Duration::from_secs(10),
        job_completion: true,
        job_state_max_memory_mb: Some(8),
        job_receipts_max_memory_mb: Some(8),
        job_receipts_max_count: Some(1000),
        job_receipt_retention_ms: Some(60_000),
        job_completion_max_bytes: Some(1 << 20),
        ..Default::default()
    })
    .expect("managed connect");
    let caps = db.capabilities().expect("capabilities");
    assert_ne!(caps.features & FEATURE_JOBS, 0, "managed CAP_JOBS missing");
    db.queue_declare("managed-jobs", QueueOptions::default())
        .unwrap();
    assert!(db
        .queue_manifest()
        .expect("managed manifest")
        .iter()
        .any(|q| q.name == "managed-jobs"));
    let options = StateOptions::new(0);
    let receipt = db
        .state_put(b"managed:key", b"value", options)
        .expect("managed state put");
    assert_eq!(receipt.state_version, 1);
    assert_eq!(
        db.state_get(b"managed:key").expect("managed get").unwrap().value,
        b"value"
    );
    drop(db);
    std::thread::sleep(Duration::from_millis(700));
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn intent_json_tolerates_whitespace_and_number_tokens() {
    let intent = JobCompletionIntent::new(
        "q", 1, 2, b"k".to_vec(), 3, b"v".to_vec(), None,
    );
    let text = intent.to_json_string();
    // Pretty-printed with whitespace and raw number tokens parses the same.
    let spaced = text.replace('{', "{ ").replace('}', " }").replace(',', ", ");
    assert_eq!(
        JobCompletionIntent::from_json_string(&spaced).expect("parse"),
        intent
    );
    let numeric = text
        .replace("\"queue_incarnation\":\"1\"", "\"queue_incarnation\":1")
        .replace("\"message_id\":\"2\"", "\"message_id\":2")
        .replace("\"expected_version\":\"3\"", "\"expected_version\":3");
    assert_eq!(
        JobCompletionIntent::from_json_string(&numeric).expect("parse"),
        intent
    );
    // Invalid input is a typed validation failure, not a crash.
    assert!(JobCompletionIntent::from_json_string("{not json").is_err());
}

#[test]
fn intent_json_interops_with_python_reference() {
    // Byte-for-byte output of JobCompletionIntent.to_json() from the Python
    // reference SDK (src/kuttidb_client.py) for the same logical intent.
    let python_json = r#"{"operation_id":"01234567-89ab-cdef-0123-456789abcdef","input":{"queue":"extract-pdf","queue_incarnation":"9007199254740993","message_id":"18446744073709551615"},"state":{"key":"cGRmOjQy","expected_version":"99","value":"ZXh0cmFjdGVk"},"output":{"queue":"index-text","queue_incarnation":"42","value":"b3V0"}}"#;
    let parsed = JobCompletionIntent::from_json_string(python_json).expect("parse python json");
    assert_eq!(
        parsed.operation_id,
        [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef
        ]
    );
    assert_eq!(parsed.queue, "extract-pdf");
    assert_eq!(parsed.queue_incarnation, 9_007_199_254_740_993);
    assert_eq!(parsed.message_id, u64::MAX);
    assert_eq!(parsed.state_key, b"pdf:42");
    assert_eq!(parsed.expected_version, 99);
    assert_eq!(parsed.state_value, b"extracted");
    let output = parsed.output.clone().expect("output");
    assert_eq!(output.queue, "index-text");
    assert_eq!(output.queue_incarnation, 42);
    assert_eq!(output.value, b"out");
    // And the Rust encoding of the same intent is byte-identical.
    assert_eq!(parsed.to_json_string(), python_json);
}
