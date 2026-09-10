//! Atomic job completion (durable state + completion), opcodes 0x70–0x77.
//!
//! Wire contract: `docs/plans/ATOMIC_JOB_COMPLETION_DESIGN.md` §9 and
//! `docs/design/PROTOCOL.md`. The feature is capability-gated server-side
//! (`CAP_JOBS`, `--job-completion`); this module never emulates an operation
//! with separate writes — without the feature the server answers the typed
//! error envelope and the client maps it onto [`Error::Job`].
//!
//! The typed error envelope `[status 0x02][len:4][code:1][outcome:1][detail]`
//! separates "definitely not committed" from "unknown" (a possibly committed
//! append whose durability could not be resolved). On an unknown outcome the
//! original intent (operation id and semantic request) must be preserved for
//! reconciliation; [`Client::job_completion`] is the safe next step.

use super::features::Dec;
use super::{Client, Error, ST_MISS, ST_OK};
use std::borrow::Cow;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ST_ERR: u8 = 0x02;

const OP_JOB_CONSUME: u8 = 0x70;
const OP_JOB_COMPLETE: u8 = 0x71;
const OP_JOB_RECEIPT: u8 = 0x72;
const OP_STATE_GET: u8 = 0x73;
const OP_STATE_PUT: u8 = 0x74;
const OP_STATE_DELETE: u8 = 0x75;
const OP_DURABLE_OPERATION: u8 = 0x76;
const OP_QUEUE_MANIFEST: u8 = 0x77;

const QUEUE_NAME_MAX: usize = 255;
const RECEIPT_BODY: usize = 41; // 5 x u64 + replayed byte
const MUTATION_BODY: usize = 33; // 4 x u64 + replayed byte

// -- typed errors -----------------------------------------------------------

/// Stable wire code of an atomic-job-completion failure. The numeric values
/// are part of the wire contract (`src/job_status.h`); do not reorder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobCode {
    UnsupportedFeature,
    ValidationFailed,
    RequestTooLarge,
    IdempotencyConflict,
    StateVersionConflict,
    DeliveryExpired,
    DeliveryNotOwned,
    ResourceExhausted,
    /// Reserved; not emitted by the v1 single-threaded-per-commit design.
    OperationInProgress,
    /// The outcome could not be resolved; reconcile via the receipt lookup.
    OperationInDoubt,
    PersistenceUnavailable,
    NotFound,
    Other(u8),
}

impl JobCode {
    pub fn from_u8(code: u8) -> JobCode {
        match code {
            1 => JobCode::UnsupportedFeature,
            2 => JobCode::ValidationFailed,
            3 => JobCode::RequestTooLarge,
            4 => JobCode::IdempotencyConflict,
            5 => JobCode::StateVersionConflict,
            6 => JobCode::DeliveryExpired,
            7 => JobCode::DeliveryNotOwned,
            8 => JobCode::ResourceExhausted,
            9 => JobCode::OperationInProgress,
            10 => JobCode::OperationInDoubt,
            11 => JobCode::PersistenceUnavailable,
            12 => JobCode::NotFound,
            other => JobCode::Other(other),
        }
    }

    /// Stable wire code name (e.g. `"idempotency_conflict"`).
    pub fn name(&self) -> Cow<'static, str> {
        match self {
            JobCode::UnsupportedFeature => Cow::Borrowed("unsupported_feature"),
            JobCode::ValidationFailed => Cow::Borrowed("validation_failed"),
            JobCode::RequestTooLarge => Cow::Borrowed("request_too_large"),
            JobCode::IdempotencyConflict => Cow::Borrowed("idempotency_conflict"),
            JobCode::StateVersionConflict => Cow::Borrowed("state_version_conflict"),
            JobCode::DeliveryExpired => Cow::Borrowed("delivery_expired"),
            JobCode::DeliveryNotOwned => Cow::Borrowed("delivery_not_owned"),
            JobCode::ResourceExhausted => Cow::Borrowed("resource_exhausted"),
            JobCode::OperationInProgress => Cow::Borrowed("operation_in_progress"),
            JobCode::OperationInDoubt => Cow::Borrowed("operation_in_doubt"),
            JobCode::PersistenceUnavailable => Cow::Borrowed("persistence_unavailable"),
            JobCode::NotFound => Cow::Borrowed("not_found"),
            JobCode::Other(n) => Cow::Owned(format!("code_{n}")),
        }
    }
}

/// Whether the durable effect of a failed operation definitely did not
/// happen, or could not be resolved. Never conflate a conflict, an absence,
/// and an unknown outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobOutcome {
    NotCommitted,
    Unknown,
}

/// Typed atomic-job-completion failure mirroring the wire error envelope
/// `[status 0x02][len:4][code:1][outcome:1][detail]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobError {
    pub code: JobCode,
    pub outcome: JobOutcome,
    pub detail: Option<String>,
}

impl JobError {
    pub fn new(code: JobCode, outcome: JobOutcome, detail: Option<String>) -> JobError {
        JobError {
            code,
            outcome,
            detail,
        }
    }

    pub(crate) fn from_envelope(body: &[u8]) -> JobError {
        let code = body.first().map_or(0, |c| *c);
        let outcome = if body.get(1).copied().unwrap_or(0) == 0 {
            JobOutcome::NotCommitted
        } else {
            JobOutcome::Unknown
        };
        let detail = match body.get(2..) {
            Some(d) if !d.is_empty() => Some(String::from_utf8_lossy(d).into_owned()),
            _ => None,
        };
        JobError::new(JobCode::from_u8(code), outcome, detail)
    }

    /// Stable wire code name (e.g. `"idempotency_conflict"`).
    pub fn code_name(&self) -> Cow<'static, str> {
        self.code.name()
    }

    /// True when the durable effect definitely did not happen.
    pub fn is_not_committed(&self) -> bool {
        self.outcome == JobOutcome::NotCommitted
    }

    /// True when the outcome is unresolved: the same-id receipt lookup or an
    /// exact retry of the preserved intent is the only safe continuation.
    pub fn is_unknown(&self) -> bool {
        self.outcome == JobOutcome::Unknown
    }
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "job operation failed: {} ({}",
            self.code.name(),
            match self.outcome {
                JobOutcome::NotCommitted => "not_committed",
                JobOutcome::Unknown => "unknown",
            }
        )?;
        if let Some(detail) = &self.detail {
            write!(f, ": {detail}")?;
        }
        f.write_str(")")
    }
}

// -- data types --------------------------------------------------------------

/// One live Queue from [`Client::queue_manifest`]: stable identity
/// (incarnation), durability, capacity, and revision. Incarnation ids are
/// required to compose completion intents for output queues and are stable
/// across restarts, changing only when a Queue is deleted and recreated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueManifestEntry {
    pub name: String,
    pub durable: bool,
    pub incarnation: u64,
    pub depth: u64,
    pub inflight: u64,
    pub max_depth: u64,
    pub revision: u64,
}

/// Completion-capable delivery from [`Client::job_consume`]. The opaque
/// `proof` is the only credential; native owner tokens and delivery tags
/// stay private to the server. The lease deadline is a wall-clock mirror for
/// display and logging only — fencing uses the server's monotonic lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobDelivery {
    pub store_id: [u8; 16],
    pub queue: String,
    pub queue_incarnation: u64,
    pub message_id: u64,
    pub attempts: u32,
    pub redelivered: bool,
    pub lease_deadline_ms: u64,
    pub proof: [u8; 16],
    pub value: Vec<u8>,
}

/// Optional routed output of one atomic completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobOutput {
    pub queue: String,
    pub queue_incarnation: u64,
    pub value: Vec<u8>,
}

/// One stable logical completion: caller-owned operation id plus the full
/// semantic request. Serialize this object before submission — that is the
/// supported recovery path for lost responses; retries must reuse the same
/// id and the same fields. [`JobCompletionIntent::to_json_string`] is
/// lossless: 64-bit identity and version fields are decimal strings, byte
/// spans are base64. The ephemeral delivery proof is deliberately not
/// serialized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobCompletionIntent {
    pub operation_id: [u8; 16],
    pub queue: String,
    pub queue_incarnation: u64,
    pub message_id: u64,
    pub state_key: Vec<u8>,
    pub expected_version: u64,
    pub state_value: Vec<u8>,
    pub output: Option<JobOutput>,
}

/// Immutable original result of one committed completion. `replayed` may
/// differ between the first success and a matched retry; every other field is
/// identical across retries while the receipt is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JobCompletionResult {
    pub commit_id: u64,
    pub state_version: u64,
    pub output_message_id: u64,
    pub completed_at_ms: u64,
    pub receipt_expires_at_ms: u64,
    pub replayed: bool,
}

/// Retained receipt of a committed completion, returned by
/// [`Client::job_completion`]. A miss means "no retained receipt" — absence
/// is never proof that the operation never executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JobReceipt {
    pub operation_id: [u8; 16],
    pub commit_id: u64,
    pub state_version: u64,
    pub output_message_id: u64,
    pub completed_at_ms: u64,
    pub receipt_expires_at_ms: u64,
}

/// One durable-state entry: exact value bytes, its version, and the commit
/// id that last wrote it. The `durable` keyspace is fixed, non-evictable,
/// and never expires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateValue {
    pub value: Vec<u8>,
    pub version: u64,
    pub commit_id: u64,
}

/// Options for [`Client::state_put`] / [`Client::state_delete`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateOptions {
    /// `0` creates only on PUT; a positive value must match the current
    /// version exactly. DELETE requires the entry's current positive version.
    pub expected_version: u64,
    /// Stable operation id for the shared receipt ledger; retries reuse it.
    pub operation_id: [u8; 16],
}

impl StateOptions {
    /// Fresh options with a randomly generated operation id. Persist the id
    /// before submitting so a lost response can be reconciled.
    pub fn new(expected_version: u64) -> StateOptions {
        StateOptions {
            expected_version,
            operation_id: random_uuid(),
        }
    }

    /// Explicit operation id, for an exact retry of a preserved mutation.
    pub fn with_operation_id(expected_version: u64, operation_id: [u8; 16]) -> StateOptions {
        StateOptions {
            expected_version,
            operation_id,
        }
    }
}

impl Default for StateOptions {
    fn default() -> Self {
        StateOptions::new(0)
    }
}

/// Receipt kind in the shared operation-id ledger. A completion and a state
/// mutation never share one operation id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Completion,
    StatePut,
    StateDelete,
}

impl OperationKind {
    fn from_u8(kind: u8) -> Result<OperationKind, Error> {
        match kind {
            1 => Ok(OperationKind::Completion),
            2 => Ok(OperationKind::StatePut),
            3 => Ok(OperationKind::StateDelete),
            _ => Err(Error::Server),
        }
    }
}

/// Receipt of one direct durable-state mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JobMutationReceipt {
    pub operation_id: [u8; 16],
    pub kind: OperationKind,
    pub commit_id: u64,
    pub state_version: u64,
    pub completed_at_ms: u64,
    pub receipt_expires_at_ms: u64,
    pub replayed: bool,
}

/// Retained receipt of one direct-state mutation, returned by
/// [`Client::durable_operation`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableOperationReceipt {
    pub kind: OperationKind,
    pub commit_id: u64,
    pub state_version: u64,
    pub completed_at_ms: u64,
    pub receipt_expires_at_ms: u64,
}

// -- id / encoding helpers ---------------------------------------------------

/// Random v4 operation id. Reads `/dev/urandom` (KuttiDB runs on macOS and
/// Linux only); the fallback mixes monotonic-time, process, and address
/// entropy so callers always receive a process-unique id.
pub fn random_uuid() -> [u8; 16] {
    let mut id = [0u8; 16];
    if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
        if file.read_exact(&mut id).is_ok() {
            id[6] = (id[6] & 0x0f) | 0x40;
            id[8] = (id[8] & 0x3f) | 0x80;
            return id;
        }
    }
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = nanos ^ ((std::process::id() as u64) << 32) ^ (&id as *const _ as u64);
    x = x.wrapping_add(
        COUNTER
            .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
            .wrapping_add(1),
    );
    let mut y = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    y = (y ^ (y >> 30)).wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    let bytes: [u8; 8] = (y ^ (y >> 33)).to_le_bytes();
    id[..8].copy_from_slice(&bytes);
    id[8..].copy_from_slice(&x.to_le_bytes());
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    id
}

/// Hyphenated lowercase UUID form (e.g. for logs and cross-SDK exchange).
pub fn uuid_string(id: &[u8; 16]) -> String {
    let h: Vec<String> = id.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}{}{}{}-{}{}-{}{}-{}{}-{}{}{}{}{}{}",
        h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11], h[12], h[13],
        h[14], h[15]
    )
}

/// Parse a hyphenated (or bare) lowercase/uppercase hex UUID into 16 bytes.
pub fn parse_uuid(text: &str) -> Result<[u8; 16], Error> {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 16 * 2 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Server);
    }
    let mut id = [0u8; 16];
    for (i, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| Error::Server)?;
    }
    Ok(id)
}

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = ((chunk[0] as u32) << 16)
            | ((chunk.get(1).copied().unwrap_or(0) as u32) << 8)
            | (chunk.get(2).copied().unwrap_or(0) as u32);
        out.push(B64_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(B64_ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64_ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64_ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn b64_value(c: u8) -> Result<u32, Error> {
    match c {
        b'A'..=b'Z' => Ok((c - b'A') as u32),
        b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
        b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(Error::Server),
    }
}

fn b64_decode(text: &str) -> Result<Vec<u8>, Error> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(Error::Server);
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0u32;
    let mut padding = 0usize;
    for &c in bytes {
        if c == b'=' {
            padding += 1;
            continue;
        }
        if padding > 0 {
            return Err(Error::Server);
        }
        acc = (acc << 6) | b64_value(c)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}

fn json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

// -- lossless intent JSON ----------------------------------------------------
//
// 64-bit identity and version fields are decimal strings (lossless beyond
// 2^53), byte spans are standard Base64. The shape mirrors the Python and
// Management API encodings so a persisted intent is interchangeable between
// SDKs.

impl JobCompletionIntent {
    /// One fresh logical completion with a randomly generated operation id.
    /// Persist the intent (or at least the id) before submitting.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        queue: &str,
        queue_incarnation: u64,
        message_id: u64,
        state_key: Vec<u8>,
        expected_version: u64,
        state_value: Vec<u8>,
        output: Option<JobOutput>,
    ) -> JobCompletionIntent {
        JobCompletionIntent {
            operation_id: random_uuid(),
            queue: queue.to_owned(),
            queue_incarnation,
            message_id,
            state_key,
            expected_version,
            state_value,
            output,
        }
    }

    /// Hyphenated lowercase form of the operation id.
    pub fn operation_uuid(&self) -> String {
        uuid_string(&self.operation_id)
    }

    /// Lossless JSON encoding for persisting an intent before submission
    /// (the recovery path for lost responses). 64-bit fields are decimal
    /// strings, byte spans Base64; the delivery proof is not serialized.
    pub fn to_json_string(&self) -> String {
        let mut out = String::with_capacity(160 + self.state_value.len() / 3 * 4);
        out.push_str("{\"operation_id\":\"");
        out.push_str(&self.operation_uuid());
        out.push_str("\",\"input\":{\"queue\":\"");
        out.push_str(&json_escape(&self.queue));
        out.push_str("\",\"queue_incarnation\":\"");
        out.push_str(&self.queue_incarnation.to_string());
        out.push_str("\",\"message_id\":\"");
        out.push_str(&self.message_id.to_string());
        out.push_str("\"},\"state\":{\"key\":\"");
        out.push_str(&b64_encode(&self.state_key));
        out.push_str("\",\"expected_version\":\"");
        out.push_str(&self.expected_version.to_string());
        out.push_str("\",\"value\":\"");
        out.push_str(&b64_encode(&self.state_value));
        out.push_str("\"},\"output\":");
        match &self.output {
            None => out.push_str("null"),
            Some(o) => {
                out.push_str("{\"queue\":\"");
                out.push_str(&json_escape(&o.queue));
                out.push_str("\",\"queue_incarnation\":\"");
                out.push_str(&o.queue_incarnation.to_string());
                out.push_str("\",\"value\":\"");
                out.push_str(&b64_encode(&o.value));
                out.push_str("\"}");
            }
        }
        out.push('}');
        out
    }

    /// Parse a persisted intent (see [`JobCompletionIntent::to_json_string`]).
    /// Retries must reuse the parsed id and fields unchanged.
    pub fn from_json_string(text: &str) -> Result<JobCompletionIntent, Error> {
        let root = match JsonParser::parse(text)? {
            Json::Obj(fields) => fields,
            _ => return Err(invalid_intent()),
        };
        let uuid = match json_get(&root, "operation_id")? {
            Json::Str(s) => parse_uuid(s)?,
            _ => return Err(invalid_intent()),
        };
        let input = json_object(json_get(&root, "input")?)?;
        let state = json_object(json_get(&root, "state")?)?;
        let output = match json_get(&root, "output")? {
            Json::Null => None,
            Json::Obj(fields) => Some(JobOutput {
                queue: json_string(json_get(fields, "queue")?)?.to_owned(),
                queue_incarnation: json_u64(json_get(fields, "queue_incarnation")?)?,
                value: b64_decode(json_string(json_get(fields, "value")?)?)?,
            }),
            _ => return Err(invalid_intent()),
        };
        Ok(JobCompletionIntent {
            operation_id: uuid,
            queue: json_string(json_get(input, "queue")?)?.to_owned(),
            queue_incarnation: json_u64(json_get(input, "queue_incarnation")?)?,
            message_id: json_u64(json_get(input, "message_id")?)?,
            state_key: b64_decode(json_string(json_get(state, "key")?)?)?,
            expected_version: json_u64(json_get(state, "expected_version")?)?,
            state_value: b64_decode(json_string(json_get(state, "value")?)?)?,
            output,
        })
    }
}

fn invalid_intent() -> Error {
    Error::Job(JobError::new(
        JobCode::ValidationFailed,
        JobOutcome::NotCommitted,
        Some("invalid intent json".into()),
    ))
}

fn json_get<'a>(fields: &'a [(String, Json)], key: &str) -> Result<&'a Json, Error> {
    fields
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v)
        .ok_or_else(invalid_intent)
}

fn json_object(value: &Json) -> Result<&[(String, Json)], Error> {
    match value {
        Json::Obj(fields) => Ok(fields),
        _ => Err(invalid_intent()),
    }
}

fn json_string(value: &Json) -> Result<&str, Error> {
    match value {
        Json::Str(s) => Ok(s),
        _ => Err(invalid_intent()),
    }
}

fn json_u64(value: &Json) -> Result<u64, Error> {
    // Decimal strings are the lossless on-disk form; a bare number token is
    // accepted only when it is an exact non-fractional u64.
    let text = match value {
        Json::Str(s) => s.as_str(),
        Json::Num(n) => n.as_str(),
        _ => return Err(invalid_intent()),
    };
    text.parse::<u64>().map_err(|_| invalid_intent())
}

impl Json {
    #[allow(dead_code)]
    fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

impl JobDelivery {
    /// Compose one completion intent from this delivery. The operation id is
    /// generated here, once; persist it before submitting.
    pub fn to_intent(
        &self,
        state_key: Vec<u8>,
        expected_version: u64,
        state_value: Vec<u8>,
        output: Option<JobOutput>,
    ) -> JobCompletionIntent {
        JobCompletionIntent {
            operation_id: random_uuid(),
            queue: self.queue.clone(),
            queue_incarnation: self.queue_incarnation,
            message_id: self.message_id,
            state_key,
            expected_version,
            state_value,
            output,
        }
    }
}

// -- minimal JSON parser (objects, arrays, strings, numbers, literals) -------

// Tolerant generic JSON tree: array/bool payloads are parsed for validity but
// never read by the intent schema.
#[allow(dead_code)]
enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

struct JsonParser<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> JsonParser<'a> {
    fn parse(text: &'a str) -> Result<Json, Error> {
        let mut p = JsonParser {
            b: text.as_bytes(),
            i: 0,
        };
        let value = p.value()?;
        p.skip_ws();
        if p.i != p.b.len() {
            return Err(Error::Server);
        }
        Ok(value)
    }

    fn skip_ws(&mut self) {
        while matches!(self.b.get(self.i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    fn peek(&mut self) -> Result<u8, Error> {
        self.skip_ws();
        self.b.get(self.i).copied().ok_or(Error::Server)
    }

    fn eat(&mut self, c: u8) -> Result<(), Error> {
        if self.peek()? == c {
            self.i += 1;
            Ok(())
        } else {
            Err(Error::Server)
        }
    }

    fn literal(&mut self, lit: &str) -> Result<(), Error> {
        self.skip_ws();
        if self.b[self.i..].starts_with(lit.as_bytes()) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(Error::Server)
        }
    }

    fn value(&mut self) -> Result<Json, Error> {
        match self.peek()? {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(Json::Str(self.string()?)),
            b't' => {
                self.literal("true")?;
                Ok(Json::Bool(true))
            }
            b'f' => {
                self.literal("false")?;
                Ok(Json::Bool(false))
            }
            b'n' => {
                self.literal("null")?;
                Ok(Json::Null)
            }
            _ => self.number(),
        }
    }

    fn object(&mut self) -> Result<Json, Error> {
        self.eat(b'{')?;
        let mut fields = Vec::new();
        if self.peek()? == b'}' {
            self.i += 1;
            return Ok(Json::Obj(fields));
        }
        loop {
            let key = self.string()?;
            self.eat(b':')?;
            let value = self.value()?;
            fields.push((key, value));
            match self.peek()? {
                b',' => self.i += 1,
                b'}' => {
                    self.i += 1;
                    return Ok(Json::Obj(fields));
                }
                _ => return Err(Error::Server),
            }
        }
    }

    fn array(&mut self) -> Result<Json, Error> {
        self.eat(b'[')?;
        let mut items = Vec::new();
        if self.peek()? == b']' {
            self.i += 1;
            return Ok(Json::Arr(items));
        }
        loop {
            items.push(self.value()?);
            match self.peek()? {
                b',' => self.i += 1,
                b']' => {
                    self.i += 1;
                    return Ok(Json::Arr(items));
                }
                _ => return Err(Error::Server),
            }
        }
    }

    fn number(&mut self) -> Result<Json, Error> {
        self.skip_ws();
        let start = self.i;
        while matches!(
            self.b.get(self.i),
            Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
        ) {
            self.i += 1;
        }
        if start == self.i {
            return Err(Error::Server);
        }
        Ok(Json::Num(
            String::from_utf8_lossy(&self.b[start..self.i]).into_owned(),
        ))
    }

    fn string(&mut self) -> Result<String, Error> {
        self.eat(b'"')?;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let c = *self.b.get(self.i).ok_or(Error::Server)?;
            self.i += 1;
            match c {
                b'"' => return String::from_utf8(out).map_err(|_| Error::Server),
                b'\\' => {
                    let e = *self.b.get(self.i).ok_or(Error::Server)?;
                    self.i += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0c),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            let ch = if (0xd800..0xdc00).contains(&hi) {
                                // surrogate pair
                                if self.b.get(self.i) == Some(&b'\\')
                                    && self.b.get(self.i + 1) == Some(&b'u')
                                {
                                    self.i += 2;
                                    let lo = self.hex4()?;
                                    if !(0xdc00..0xe000).contains(&lo) {
                                        return Err(Error::Server);
                                    }
                                    let cp = 0x1_0000
                                        + (((hi - 0xd800) as u32) << 10)
                                        + (lo - 0xdc00) as u32;
                                    char::from_u32(cp).ok_or(Error::Server)?
                                } else {
                                    return Err(Error::Server);
                                }
                            } else if (0xdc00..0xe000).contains(&hi) {
                                return Err(Error::Server);
                            } else {
                                char::from_u32(hi as u32).ok_or(Error::Server)?
                            };
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return Err(Error::Server),
                    }
                }
                _ => out.push(c),
            }
        }
    }

    fn hex4(&mut self) -> Result<u16, Error> {
        let mut v = 0u16;
        for _ in 0..4 {
            let c = *self.b.get(self.i).ok_or(Error::Server)?;
            self.i += 1;
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                b'A'..=b'F' => c - b'A' + 10,
                _ => return Err(Error::Server),
            };
            v = (v << 4) | d as u16;
        }
        Ok(v)
    }
}

// -- client surface ----------------------------------------------------------

/// Request/response for the atomic-job-completion family: the typed error
/// envelope ([code:1][outcome:1][detail]) must stay reachable on 0x02 so
/// failures map onto [`Error::Job`] instead of a generic error.
fn job_request(
    client: &mut Client,
    op: u8,
    key: &[u8],
    value: &[u8],
) -> Result<(u8, Vec<u8>), Error> {
    let (status, body) = client.request(op, key, value)?;
    if status == ST_ERR {
        return Err(Error::Job(JobError::from_envelope(&body)));
    }
    Ok((status, body))
}

fn u64_le(v: &[u8]) -> u64 {
    u64::from_le_bytes(v.try_into().unwrap())
}

impl Client {
    /// Additive Queue discovery: bounded manifest of every live Queue with
    /// stable identity (incarnation), durability, capacity, and revision.
    /// Incarnation ids are required to compose completion intents for output
    /// queues and are stable across restarts.
    pub fn queue_manifest(&mut self) -> Result<Vec<QueueManifestEntry>, Error> {
        let (status, body) = job_request(self, OP_QUEUE_MANIFEST, b"", b"")?;
        ok(status)?;
        let mut d = Dec { b: &body, i: 0 };
        let n = d.u16()?;
        if n > 256 {
            return Err(Error::Server);
        }
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let l = d.u16()? as usize;
            let name = String::from_utf8(d.take(l)?.to_vec()).map_err(|_| Error::Server)?;
            let durable = d.take(1)?[0] != 0;
            out.push(QueueManifestEntry {
                name,
                durable,
                incarnation: d.u64()?,
                depth: d.u64()?,
                inflight: d.u64()?,
                max_depth: d.u64()?,
                revision: d.u64()?,
            });
        }
        d.done()?;
        Ok(out)
    }

    /// Deliver one message with a completion proof. Requires a durable Queue
    /// and a registered named consumer ([`Client::queue_consumer_register`]);
    /// the consumer's stable owner token owns the delivery, so pooled
    /// connections stay interchangeable and a disconnected worker's
    /// deliveries follow their visibility deadlines. The returned proof is
    /// one-use: a committed completion retires it. Closing the connection
    /// does not unregister the consumer.
    pub fn job_consume(
        &mut self,
        queue: &str,
        consumer: &str,
        visibility: Duration,
    ) -> Result<Option<JobDelivery>, Error> {
        if queue.is_empty() || consumer.is_empty() || consumer.len() > QUEUE_NAME_MAX {
            return Err(Error::KeyTooLarge);
        }
        let mut value = (consumer.len() as u16).to_le_bytes().to_vec();
        value.extend_from_slice(consumer.as_bytes());
        value.extend_from_slice(&ms(visibility).to_le_bytes());
        let (status, body) = job_request(self, OP_JOB_CONSUME, queue.as_bytes(), &value)?;
        if status == ST_MISS {
            return Ok(None);
        }
        ok(status)?;
        if body.len() < 61 {
            return Err(Error::Server);
        }
        Ok(Some(JobDelivery {
            store_id: body[0..16].try_into().unwrap(),
            queue: queue.to_owned(),
            queue_incarnation: u64_le(&body[16..24]),
            message_id: u64_le(&body[24..32]),
            attempts: u32::from_le_bytes(body[32..36].try_into().unwrap()),
            redelivered: body[36] != 0,
            lease_deadline_ms: u64_le(&body[37..45]),
            proof: body[45..61].try_into().unwrap(),
            value: body[61..].to_vec(),
        }))
    }

    /// Submit one atomic completion: durable-state PUT + input ACK + optional
    /// output publish + receipt, committed together. On a timeout or
    /// disconnect keep the exact intent and id, then retry the same call or
    /// use [`Client::job_completion`] to query the receipt — never
    /// regenerate the id and never issue a separate ACK after a success.
    pub fn job_complete(
        &mut self,
        intent: &JobCompletionIntent,
        proof: &[u8; 16],
    ) -> Result<JobCompletionResult, Error> {
        if intent.queue.is_empty() || intent.queue.len() > QUEUE_NAME_MAX {
            return Err(Error::KeyTooLarge);
        }
        if intent.state_key.is_empty() || intent.state_key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        if let Some(output) = &intent.output {
            if output.queue_incarnation == 0 {
                return Err(Error::Job(JobError::new(
                    JobCode::ValidationFailed,
                    JobOutcome::NotCommitted,
                    Some("output intent requires its queue incarnation".into()),
                )));
            }
        }
        let mut value = Vec::with_capacity(64 + intent.state_key.len() + intent.state_value.len());
        value.extend_from_slice(&intent.operation_id);
        value.extend_from_slice(&intent.queue_incarnation.to_le_bytes());
        value.extend_from_slice(&intent.message_id.to_le_bytes());
        value.extend_from_slice(proof);
        value.extend_from_slice(&(intent.state_key.len() as u16).to_le_bytes());
        value.extend_from_slice(&intent.state_key);
        value.extend_from_slice(&intent.expected_version.to_le_bytes());
        value.extend_from_slice(&(intent.state_value.len() as u32).to_le_bytes());
        value.extend_from_slice(&intent.state_value);
        match &intent.output {
            None => value.push(0),
            Some(output) => {
                if output.queue.is_empty() || output.queue.len() > QUEUE_NAME_MAX {
                    return Err(Error::KeyTooLarge);
                }
                value.push(1);
                value.extend_from_slice(&(output.queue.len() as u16).to_le_bytes());
                value.extend_from_slice(output.queue.as_bytes());
                value.extend_from_slice(&output.queue_incarnation.to_le_bytes());
                value.extend_from_slice(&(output.value.len() as u32).to_le_bytes());
                value.extend_from_slice(&output.value);
            }
        }
        let (status, body) = job_request(self, OP_JOB_COMPLETE, intent.queue.as_bytes(), &value)?;
        ok(status)?;
        decode_result(&body)
    }

    /// Look up a retained completion receipt by operation id. Authenticated
    /// lookup never requires the (now stale) delivery proof and works after a
    /// restart. A miss means "no retained receipt" — absence is never proof
    /// that the operation never executed.
    pub fn job_completion(&mut self, operation_id: [u8; 16]) -> Result<Option<JobReceipt>, Error> {
        let (status, body) =
            job_request(self, OP_JOB_RECEIPT, b"", &operation_id)?;
        if status == ST_MISS {
            return Ok(None);
        }
        ok(status)?;
        if body.len() != RECEIPT_BODY {
            return Err(Error::Server);
        }
        Ok(Some(JobReceipt {
            operation_id,
            commit_id: u64_le(&body[0..8]),
            state_version: u64_le(&body[8..16]),
            output_message_id: u64_le(&body[16..24]),
            completed_at_ms: u64_le(&body[24..32]),
            receipt_expires_at_ms: u64_le(&body[32..40]),
        }))
    }

    /// Read one durable-state entry: exact value bytes, its version, and the
    /// commit id that last wrote it.
    pub fn state_get(&mut self, key: &[u8]) -> Result<Option<StateValue>, Error> {
        if key.is_empty() || key.len() > super::MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        let (status, body) = job_request(self, OP_STATE_GET, key, b"")?;
        if status == ST_MISS {
            return Ok(None);
        }
        ok(status)?;
        if body.len() < 16 {
            return Err(Error::Server);
        }
        Ok(Some(StateValue {
            version: u64_le(&body[0..8]),
            commit_id: u64_le(&body[8..16]),
            value: body[16..].to_vec(),
        }))
    }

    /// Version-checked direct durable-state PUT with its own receipt.
    /// `expected_version == 0` creates only; a positive value must match the
    /// current version exactly (no unchecked overwrite path exists). The same
    /// operation id may be retried unchanged to reconcile a lost response; a
    /// reused id with different content raises `idempotency_conflict`.
    pub fn state_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        options: StateOptions,
    ) -> Result<JobMutationReceipt, Error> {
        if key.is_empty() || key.len() > super::MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        let mut payload = options.operation_id.to_vec();
        payload.extend_from_slice(&options.expected_version.to_le_bytes());
        payload.extend_from_slice(value);
        let (status, body) = job_request(self, OP_STATE_PUT, key, &payload)?;
        ok(status)?;
        let fields = decode_mutation(&body)?;
        Ok(JobMutationReceipt {
            operation_id: options.operation_id,
            kind: OperationKind::StatePut,
            commit_id: fields.commit_id,
            state_version: fields.state_version,
            completed_at_ms: fields.completed_at_ms,
            receipt_expires_at_ms: fields.receipt_expires_at_ms,
            replayed: fields.replayed,
        })
    }

    /// Version-checked direct durable-state DELETE with its own receipt.
    /// Requires the entry's current positive version. Retrying a committed
    /// delete with the same id returns its retained receipt even though the
    /// entry is already absent; deleting an absent key without a retained
    /// receipt is a definite `not_found`.
    pub fn state_delete(&mut self, key: &[u8], options: StateOptions) -> Result<JobMutationReceipt, Error> {
        if key.is_empty() || key.len() > super::MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        if options.expected_version == 0 {
            return Err(Error::Job(JobError::new(
                JobCode::ValidationFailed,
                JobOutcome::NotCommitted,
                Some("state delete requires the entry's current positive version".into()),
            )));
        }
        let mut payload = options.operation_id.to_vec();
        payload.extend_from_slice(&options.expected_version.to_le_bytes());
        let (status, body) = job_request(self, OP_STATE_DELETE, key, &payload)?;
        ok(status)?;
        let fields = decode_mutation(&body)?;
        Ok(JobMutationReceipt {
            operation_id: options.operation_id,
            kind: OperationKind::StateDelete,
            commit_id: fields.commit_id,
            state_version: fields.state_version,
            completed_at_ms: fields.completed_at_ms,
            receipt_expires_at_ms: fields.receipt_expires_at_ms,
            replayed: fields.replayed,
        })
    }

    /// Look up a retained direct-state mutation receipt (shared
    /// operation-id ledger).
    pub fn durable_operation(
        &mut self,
        operation_id: [u8; 16],
    ) -> Result<Option<DurableOperationReceipt>, Error> {
        let (status, body) = job_request(self, OP_DURABLE_OPERATION, b"", &operation_id)?;
        if status == ST_MISS {
            return Ok(None);
        }
        ok(status)?;
        if body.len() != MUTATION_BODY {
            return Err(Error::Server);
        }
        Ok(Some(DurableOperationReceipt {
            kind: OperationKind::from_u8(body[0])?,
            commit_id: u64_le(&body[1..9]),
            state_version: u64_le(&body[9..17]),
            completed_at_ms: u64_le(&body[17..25]),
            receipt_expires_at_ms: u64_le(&body[25..33]),
        }))
    }
}

fn decode_result(body: &[u8]) -> Result<JobCompletionResult, Error> {
    if body.len() != RECEIPT_BODY {
        return Err(Error::Server);
    }
    Ok(JobCompletionResult {
        commit_id: u64_le(&body[0..8]),
        state_version: u64_le(&body[8..16]),
        output_message_id: u64_le(&body[16..24]),
        completed_at_ms: u64_le(&body[24..32]),
        receipt_expires_at_ms: u64_le(&body[32..40]),
        replayed: body[40] != 0,
    })
}

/// Shared 33-byte mutation receipt body
/// `[commit:8][version:8][completed_at:8][receipt_expires_at:8][replayed:1]`.
struct MutationFields {
    commit_id: u64,
    state_version: u64,
    completed_at_ms: u64,
    receipt_expires_at_ms: u64,
    replayed: bool,
}

fn decode_mutation(body: &[u8]) -> Result<MutationFields, Error> {
    if body.len() != MUTATION_BODY {
        return Err(Error::Server);
    }
    Ok(MutationFields {
        commit_id: u64_le(&body[0..8]),
        state_version: u64_le(&body[8..16]),
        completed_at_ms: u64_le(&body[16..24]),
        receipt_expires_at_ms: u64_le(&body[24..32]),
        replayed: body[32] != 0,
    })
}

fn ok(status: u8) -> Result<(), Error> {
    if status == ST_OK {
        Ok(())
    } else {
        Err(Error::Server)
    }
}

fn ms(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}
