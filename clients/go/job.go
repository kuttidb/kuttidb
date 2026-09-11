package kuttidb

// Atomic job completion (opcodes 0x70-0x77; capability bit 1<<16, protocol
// minor 8, docs/design/PROTOCOL.md). One durable commit binds a versioned
// durable-state write, the input queue ACK, an optional output-queue
// publish, and a retained receipt keyed by a caller-owned operation id.
//
// Every opcode in this family answers failures with a typed envelope —
// [status 0x02][len:4][code:1][outcome:1][detail] — where the outcome byte
// separates "definitely not committed" ("not_committed") from "unknown"
// (a possibly committed append whose durability could not be resolved).
// Never conflate a conflict, an absence, and an unknown outcome: on
// "unknown", reconcile via JobCompletion receipt lookup or an exact retry
// of the preserved intent; never regenerate the operation id. Legacy
// opcodes keep their existing framing untouched.

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// OperationID is the 16-byte identity of one logical operation (a
// completion, a state PUT, or a state DELETE). It is caller-owned: generate
// it once with NewOperationID (or ParseOperationID for a UUID string),
// persist it before submitting, and reuse the same value on retries.
type OperationID [16]byte

// NewOperationID returns a random UUIDv4 operation id from crypto/rand.
func NewOperationID() OperationID {
	var id OperationID
	if _, err := crand.Read(id[:]); err != nil {
		panic(fmt.Sprintf("kuttidb: crypto/rand unavailable: %v", err))
	}
	id[6] = (id[6] & 0x0f) | 0x40 // version 4
	id[8] = (id[8] & 0x3f) | 0x80 // RFC 4122 variant
	return id
}

// ParseOperationID parses a UUID string (canonical dashed form or 32 hex
// digits) into an OperationID.
func ParseOperationID(s string) (OperationID, error) {
	hex := strings.ReplaceAll(strings.TrimSpace(s), "-", "")
	if len(hex) != 32 {
		return OperationID{}, fmt.Errorf("kuttidb: invalid operation id %q", s)
	}
	var id OperationID
	for i := 0; i < 32; i += 2 {
		v, err := strconv.ParseUint(hex[i:i+2], 16, 8)
		if err != nil {
			return OperationID{}, fmt.Errorf("kuttidb: invalid operation id %q", s)
		}
		id[i/2] = byte(v)
	}
	return id, nil
}

// String returns the canonical dashed UUID form.
func (op OperationID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", op[0:4], op[4:6], op[6:8], op[8:10], op[10:16])
}

// IsZero reports whether the id is the zero value (unset).
func (op OperationID) IsZero() bool { return op == OperationID{} }

// MarshalJSON encodes the id as a canonical UUID string.
func (op OperationID) MarshalJSON() ([]byte, error) {
	return json.Marshal(op.String())
}

// UnmarshalJSON accepts a UUID string (dashed or plain hex).
func (op *OperationID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := ParseOperationID(s)
	if err != nil {
		return err
	}
	*op = parsed
	return nil
}

// Stable wire code names of the atomic-job-completion error envelope.
const (
	JobCodeUnsupportedFeature     = "unsupported_feature"
	JobCodeValidationFailed       = "validation_failed"
	JobCodeRequestTooLarge        = "request_too_large"
	JobCodeIdempotencyConflict    = "idempotency_conflict"
	JobCodeStateVersionConflict   = "state_version_conflict"
	JobCodeDeliveryExpired        = "delivery_expired"
	JobCodeDeliveryNotOwned       = "delivery_not_owned"
	JobCodeResourceExhausted      = "resource_exhausted"
	JobCodeOperationInProgress    = "operation_in_progress" // reserved
	JobCodeOperationInDoubt       = "operation_in_doubt"
	JobCodePersistenceUnavailable = "persistence_unavailable"
	JobCodeNotFound               = "not_found"
)

// Outcomes of the typed error envelope. "not_committed" means the durable
// effect did not happen; "unknown" means a possibly committed append whose
// durability could not be resolved — the receipt lookup (JobCompletion) or
// an exact retry of the preserved intent is the safe next step.
const (
	JobOutcomeNotCommitted = "not_committed"
	JobOutcomeUnknown      = "unknown"
)

// Sentinel errors for every wire code. A *JobError returned by any job
// method matches its sentinel, so callers can branch with errors.Is:
//
//	errors.Is(err, ErrJobIdempotencyConflict)
var (
	ErrJobUnsupportedFeature     = errors.New("kuttidb: unsupported_feature")
	ErrJobValidationFailed       = errors.New("kuttidb: validation_failed")
	ErrJobRequestTooLarge        = errors.New("kuttidb: request_too_large")
	ErrJobIdempotencyConflict    = errors.New("kuttidb: idempotency_conflict")
	ErrJobStateVersionConflict   = errors.New("kuttidb: state_version_conflict")
	ErrJobDeliveryExpired        = errors.New("kuttidb: delivery_expired")
	ErrJobDeliveryNotOwned       = errors.New("kuttidb: delivery_not_owned")
	ErrJobResourceExhausted      = errors.New("kuttidb: resource_exhausted")
	ErrJobOperationInProgress    = errors.New("kuttidb: operation_in_progress") // reserved
	ErrJobOperationInDoubt       = errors.New("kuttidb: operation_in_doubt")
	ErrJobPersistenceUnavailable = errors.New("kuttidb: persistence_unavailable")
	ErrJobNotFound               = errors.New("kuttidb: not_found")
)

var jobWireCodes = map[byte]string{
	1:  JobCodeUnsupportedFeature,
	2:  JobCodeValidationFailed,
	3:  JobCodeRequestTooLarge,
	4:  JobCodeIdempotencyConflict,
	5:  JobCodeStateVersionConflict,
	6:  JobCodeDeliveryExpired,
	7:  JobCodeDeliveryNotOwned,
	8:  JobCodeResourceExhausted,
	9:  JobCodeOperationInProgress,
	10: JobCodeOperationInDoubt,
	11: JobCodePersistenceUnavailable,
	12: JobCodeNotFound,
}

var jobCodeSentinels = map[string]error{
	JobCodeUnsupportedFeature:     ErrJobUnsupportedFeature,
	JobCodeValidationFailed:       ErrJobValidationFailed,
	JobCodeRequestTooLarge:        ErrJobRequestTooLarge,
	JobCodeIdempotencyConflict:    ErrJobIdempotencyConflict,
	JobCodeStateVersionConflict:   ErrJobStateVersionConflict,
	JobCodeDeliveryExpired:        ErrJobDeliveryExpired,
	JobCodeDeliveryNotOwned:       ErrJobDeliveryNotOwned,
	JobCodeResourceExhausted:      ErrJobResourceExhausted,
	JobCodeOperationInProgress:    ErrJobOperationInProgress,
	JobCodeOperationInDoubt:       ErrJobOperationInDoubt,
	JobCodePersistenceUnavailable: ErrJobPersistenceUnavailable,
	JobCodeNotFound:               ErrJobNotFound,
}

// JobError is a typed atomic-job-completion failure decoded from the wire
// envelope [code:1][outcome:1][detail]. Code is the stable wire name,
// Outcome is JobOutcomeNotCommitted or JobOutcomeUnknown, and Detail
// carries the server's optional text. errors.Is matches the per-code
// sentinel (see ErrJobUnsupportedFeature and friends); the Code field
// remains the authoritative discriminator for unnamed wire codes
// ("code_N" for values this SDK does not know).
type JobError struct {
	Code    string
	Outcome string
	Detail  string
}

func (e *JobError) Error() string {
	text := fmt.Sprintf("kuttidb: job operation failed: %s (%s)", e.Code, e.Outcome)
	if e.Detail != "" {
		text += ": " + e.Detail
	}
	return text
}

// Is reports whether target is the sentinel for this error's code, so
// errors.Is works across the job API without unwrapping.
func (e *JobError) Is(target error) bool {
	sentinel, ok := jobCodeSentinels[e.Code]
	return ok && sentinel == target
}

// jobErrorFromBody decodes the typed envelope body carried by a 0x02
// status on the job opcodes.
func jobErrorFromBody(body []byte) *JobError {
	var code, outcome byte
	var detail []byte
	if len(body) >= 1 {
		code = body[0]
	}
	if len(body) >= 2 {
		outcome = body[1]
	}
	if len(body) > 2 {
		detail = body[2:]
	}
	name, ok := jobWireCodes[code]
	if !ok {
		name = fmt.Sprintf("code_%d", code)
	}
	e := &JobError{Code: name, Outcome: JobOutcomeNotCommitted}
	if outcome != 0 {
		e.Outcome = JobOutcomeUnknown
	}
	if len(detail) > 0 {
		e.Detail = string(detail)
	}
	return e
}

// jobStatusError maps a job-family response to nil (OK), a typed JobError
// (status 0x02), or the generic server error.
func jobStatusError(status byte, body []byte, what string) error {
	switch status {
	case statusOK:
		return nil
	case statusError:
		return jobErrorFromBody(body)
	default:
		return fmt.Errorf("kuttidb: %s failed: %w", what, ErrServer)
	}
}

// jobRequest frames and runs one atomic-job-completion request. The typed
// error envelope must stay reachable here, so responses are never
// collapsed into a generic error before decoding.
func (c *Client) jobRequest(ctx context.Context, op byte, key string, value []byte) (byte, []byte, error) {
	return c.requestCtx(ctx, op, key, value)
}

// QueueManifestEntry is one live Queue in the bounded (256 entries) 0x77
// manifest: stable identity (Incarnation), durability, capacity, and
// revision. Incarnation ids are required to compose completion intents for
// output queues and are stable across restarts, changing only when a Queue
// is deleted and recreated.
type QueueManifestEntry struct {
	Name        string
	Durable     bool
	Incarnation uint64
	Depth       uint64
	Inflight    uint64
	MaxDepth    uint64
	Revision    uint64
}

// QueueManifest lists every live Queue with stable identity, durability,
// capacity, and revision (opcode 0x77, bounded at 256 entries).
func (c *Client) QueueManifest(ctx context.Context) ([]QueueManifestEntry, error) {
	status, value, err := c.jobRequest(ctx, opQueueManifest, "", nil)
	if err != nil {
		return nil, err
	}
	if err = jobStatusError(status, value, "queue manifest"); err != nil {
		return nil, err
	}
	d := decoder{b: value}
	n, err := d.u16()
	if err != nil {
		return nil, err
	}
	out := make([]QueueManifestEntry, 0, n)
	for range n {
		l, e := d.u16()
		if e != nil {
			return nil, e
		}
		name, e := d.bytes(int(l))
		if e != nil {
			return nil, e
		}
		flags, e := d.bytes(1)
		if e != nil {
			return nil, e
		}
		incarnation, e := d.u64()
		if e != nil {
			return nil, e
		}
		depth, e := d.u64()
		if e != nil {
			return nil, e
		}
		inflight, e := d.u64()
		if e != nil {
			return nil, e
		}
		maxDepth, e := d.u64()
		if e != nil {
			return nil, e
		}
		revision, e := d.u64()
		if e != nil {
			return nil, e
		}
		out = append(out, QueueManifestEntry{
			Name:        string(name),
			Durable:     flags[0] != 0,
			Incarnation: incarnation,
			Depth:       depth,
			Inflight:    inflight,
			MaxDepth:    maxDepth,
			Revision:    revision,
		})
	}
	return out, d.done()
}

// JobDelivery is a completion-capable delivery (opcode 0x70): the opaque
// 16-byte Proof is the only credential for JobComplete — native owner
// tokens and delivery tags stay private to the server, so pooled
// connections stay interchangeable. The proof is one use: a committed
// completion retires it. LeaseDeadlineMS is a wall-clock mirror for
// display and logging only; fencing uses the server's monotonic lease.
type JobDelivery struct {
	StoreID          []byte // 16 bytes: durable store identity
	Queue            string
	QueueIncarnation uint64
	MessageID        uint64
	Attempts         uint32
	Redelivered      bool
	LeaseDeadlineMS  uint64
	Proof            []byte // 16 bytes, one use
	Value            []byte
}

// IntentOptions composes a JobCompletionIntent from a JobDelivery.
// OperationID may be left zero to generate one (once, here); persist the
// returned intent before submitting it.
type IntentOptions struct {
	StateKey          string
	ExpectedVersion   uint64
	StateValue        []byte
	OutputQueue       string // empty = no output publish
	OutputIncarnation uint64 // required when OutputQueue is set
	OutputValue       []byte
	OperationID       OperationID // zero = generate one here, once
}

// ToIntent composes one atomic completion intent from this delivery: a
// durable-state write plus the input ACK plus an optional output publish.
// The operation id is generated here, once; persist it before submitting.
func (d *JobDelivery) ToIntent(options IntentOptions) *JobCompletionIntent {
	id := options.OperationID
	if id.IsZero() {
		id = NewOperationID()
	}
	return &JobCompletionIntent{
		OperationID:       id,
		Queue:             d.Queue,
		QueueIncarnation:  d.QueueIncarnation,
		MessageID:         d.MessageID,
		StateKey:          append([]byte(nil), options.StateKey...),
		ExpectedVersion:   options.ExpectedVersion,
		StateValue:        append([]byte(nil), options.StateValue...),
		OutputQueue:       options.OutputQueue,
		OutputIncarnation: options.OutputIncarnation,
		OutputValue:       append([]byte(nil), options.OutputValue...),
	}
}

// JobConsume delivers one message with a completion proof from a durable
// Queue (opcode 0x70). Requires a registered named consumer
// (QueueConsumerRegister); the consumer's stable owner token owns the
// delivery, so a disconnected worker's deliveries follow their visibility
// deadlines. Returns nil, nil when the queue is empty. Closing the
// connection does not unregister the consumer.
func (c *Client) JobConsume(ctx context.Context, queue, consumer string, visibility time.Duration) (*JobDelivery, error) {
	if queue == "" || len(queue) > 255 || consumer == "" || len(consumer) > 255 || visibility < 0 {
		return nil, fmt.Errorf("kuttidb: invalid job consume request")
	}
	ms, err := milliseconds(visibility, true)
	if err != nil {
		return nil, err
	}
	p := appendU16(nil, uint16(len(consumer)))
	p = append(p, consumer...)
	p = appendU64(p, ms)
	status, value, err := c.jobRequest(ctx, opJobConsume, queue, p)
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = jobStatusError(status, value, "job consume"); err != nil {
		return nil, err
	}
	if len(value) < 61 {
		return nil, fmt.Errorf("kuttidb: malformed job consume response")
	}
	return &JobDelivery{
		StoreID:          append([]byte(nil), value[0:16]...),
		Queue:            queue,
		QueueIncarnation: binary.LittleEndian.Uint64(value[16:24]),
		MessageID:        binary.LittleEndian.Uint64(value[24:32]),
		Attempts:         binary.LittleEndian.Uint32(value[32:36]),
		Redelivered:      value[36] != 0,
		LeaseDeadlineMS:  binary.LittleEndian.Uint64(value[37:45]),
		Proof:            append([]byte(nil), value[45:61]...),
		Value:            append([]byte(nil), value[61:]...),
	}, nil
}

// JobCompletionIntent is one stable logical completion: the caller-owned
// operation id plus the full semantic request (input identity, versioned
// durable-state write, optional output publish). Serialize the intent
// before submission — MarshalJSON is the supported recovery path for lost
// responses; retries must reuse the same id and the same fields.
//
// JSON encoding is lossless and matches the reference SDK: 64-bit identity
// and version fields serialize as DECIMAL STRINGS (uint64 does not fit a
// JSON number losslessly), byte spans as standard Base64, and the operation
// id as a canonical UUID string. The ephemeral delivery proof is
// deliberately not serialized.
type JobCompletionIntent struct {
	OperationID       OperationID
	Queue             string
	QueueIncarnation  uint64
	MessageID         uint64
	StateKey          []byte
	ExpectedVersion   uint64
	StateValue        []byte
	OutputQueue       string // empty = no output publish
	OutputIncarnation uint64
	OutputValue       []byte
}

// decimalU64 is a JSON string/number that decodes losslessly into a
// uint64: decimal strings are canonical (64-bit values never fit a JSON
// number), bare numbers are tolerated for hand-written documents.
type decimalU64 string

func (u *decimalU64) UnmarshalJSON(data []byte) error {
	s := string(data)
	if len(s) >= 2 && s[0] == '"' {
		return json.Unmarshal(data, (*string)(u))
	}
	*u = decimalU64(strings.TrimSpace(s))
	return nil
}

func (u decimalU64) uint64() (uint64, error) {
	v, err := strconv.ParseUint(string(u), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kuttidb: invalid decimal uint64 %q", string(u))
	}
	return v, nil
}

func formatDecimal(v uint64) string { return strconv.FormatUint(v, 10) }

// base64JSON normalizes nil byte spans to empty strings so JSON stays
// stable across values that differ only in nil-ness.
func base64JSON(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

type intentInputWire struct {
	Queue            string     `json:"queue"`
	QueueIncarnation decimalU64 `json:"queue_incarnation"`
	MessageID        decimalU64 `json:"message_id"`
}

type intentStateWire struct {
	Key             []byte     `json:"key"`
	ExpectedVersion decimalU64 `json:"expected_version"`
	Value           []byte     `json:"value"`
}

type intentOutputWire struct {
	Queue            string     `json:"queue"`
	QueueIncarnation decimalU64 `json:"queue_incarnation"`
	Value            []byte     `json:"value"`
}

type intentWire struct {
	OperationID string            `json:"operation_id"`
	Input       *intentInputWire  `json:"input"`
	State       *intentStateWire  `json:"state"`
	Output      *intentOutputWire `json:"output"`
}

// MarshalJSON encodes the intent for persistence before submission. 64-bit
// fields are decimal strings; byte spans are standard Base64; an absent
// output is null.
func (j JobCompletionIntent) MarshalJSON() ([]byte, error) {
	wire := intentWire{
		OperationID: j.OperationID.String(),
		Input: &intentInputWire{
			Queue:            j.Queue,
			QueueIncarnation: decimalU64(formatDecimal(j.QueueIncarnation)),
			MessageID:        decimalU64(formatDecimal(j.MessageID)),
		},
		State: &intentStateWire{
			Key:             base64JSON(j.StateKey),
			ExpectedVersion: decimalU64(formatDecimal(j.ExpectedVersion)),
			Value:           base64JSON(j.StateValue),
		},
	}
	if j.OutputQueue != "" {
		wire.Output = &intentOutputWire{
			Queue:            j.OutputQueue,
			QueueIncarnation: decimalU64(formatDecimal(j.OutputIncarnation)),
			Value:            base64JSON(j.OutputValue),
		}
	}
	return json.Marshal(wire)
}

// UnmarshalJSON revives a persisted intent. The output may be null or
// absent (no output publish); 64-bit fields accept decimal strings or bare
// numbers.
func (j *JobCompletionIntent) UnmarshalJSON(data []byte) error {
	var wire intentWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Input == nil || wire.State == nil {
		return errors.New("kuttidb: invalid job completion intent JSON")
	}
	id, err := ParseOperationID(wire.OperationID)
	if err != nil {
		return err
	}
	incarnation, err := wire.Input.QueueIncarnation.uint64()
	if err != nil {
		return err
	}
	messageID, err := wire.Input.MessageID.uint64()
	if err != nil {
		return err
	}
	expected, err := wire.State.ExpectedVersion.uint64()
	if err != nil {
		return err
	}
	*j = JobCompletionIntent{
		OperationID:      id,
		Queue:            wire.Input.Queue,
		QueueIncarnation: incarnation,
		MessageID:        messageID,
		StateKey:         append([]byte(nil), wire.State.Key...),
		ExpectedVersion:  expected,
		StateValue:       append([]byte(nil), wire.State.Value...),
	}
	if wire.Output != nil {
		outputIncarnation, err := wire.Output.QueueIncarnation.uint64()
		if err != nil {
			return err
		}
		j.OutputQueue = wire.Output.Queue
		j.OutputIncarnation = outputIncarnation
		j.OutputValue = append([]byte(nil), wire.Output.Value...)
	}
	return nil
}

// JobCompletionResult is the immutable original result of one committed
// completion. Replayed may differ between the first success and a matched
// retry; every other field is identical across retries while the receipt
// is retained.
type JobCompletionResult struct {
	CommitID         uint64
	StateVersion     uint64
	OutputMessageID  uint64 // 0 = no output publish
	CompletedAtMS    uint64
	ReceiptExpiresMS uint64
	Replayed         bool
}

// JobReceipt is a retained receipt of a committed completion, returned by
// JobCompletion lookup. The lookup is authenticated by the operation id
// alone and works after a restart; a miss means "no retained receipt" —
// absence is never proof that the operation never executed.
type JobReceipt struct {
	OperationID      OperationID
	CommitID         uint64
	StateVersion     uint64
	OutputMessageID  uint64
	CompletedAtMS    uint64
	ReceiptExpiresMS uint64
}

// decodeJobBody decodes the shared 41-byte completion/receipt body:
// [commit:8][state_version:8][output_message_id:8][completed_at:8]
// [receipt_expires_at:8][replayed:1].
func decodeJobBody(value []byte) (uint64, uint64, uint64, uint64, uint64, bool, error) {
	if len(value) != 41 {
		return 0, 0, 0, 0, 0, false, fmt.Errorf("kuttidb: malformed job completion response")
	}
	return binary.LittleEndian.Uint64(value),
		binary.LittleEndian.Uint64(value[8:]),
		binary.LittleEndian.Uint64(value[16:]),
		binary.LittleEndian.Uint64(value[24:]),
		binary.LittleEndian.Uint64(value[32:]),
		value[40] != 0, nil
}

// JobComplete submits one atomic completion: durable-state PUT + input ACK
// + optional output publish + receipt, committed together (opcode 0x71).
// The proof is the opaque credential from the current JobConsume delivery.
// On a timeout or disconnect keep the exact intent and id, then retry the
// same call or use JobCompletion to query the receipt — never regenerate
// the id and never issue a separate QueueAck after a success.
func (c *Client) JobComplete(ctx context.Context, intent JobCompletionIntent, proof []byte) (*JobCompletionResult, error) {
	if len(proof) != 16 {
		return nil, fmt.Errorf("kuttidb: delivery proof must be 16 bytes")
	}
	if intent.Queue == "" || len(intent.Queue) > 255 {
		return nil, fmt.Errorf("kuttidb: invalid input queue")
	}
	if len(intent.StateKey) == 0 || len(intent.StateKey) > maxKey {
		return nil, fmt.Errorf("kuttidb: invalid durable state key")
	}
	if len(intent.StateValue) > maxValue {
		return nil, ErrValueTooLarge
	}
	if intent.OutputQueue != "" {
		if intent.OutputIncarnation == 0 {
			return nil, fmt.Errorf("kuttidb: output intent requires its queue incarnation")
		}
		if len(intent.OutputQueue) > 255 {
			return nil, fmt.Errorf("kuttidb: invalid output queue")
		}
		if len(intent.OutputValue) > maxValue {
			return nil, ErrValueTooLarge
		}
	}
	p := make([]byte, 0, 56+len(intent.StateKey)+len(intent.StateValue)+len(intent.OutputQueue)+len(intent.OutputValue))
	p = append(p, intent.OperationID[:]...)
	p = appendU64(p, intent.QueueIncarnation)
	p = appendU64(p, intent.MessageID)
	p = append(p, proof...)
	p = appendU16(p, uint16(len(intent.StateKey)))
	p = append(p, intent.StateKey...)
	p = appendU64(p, intent.ExpectedVersion)
	p = appendU32(p, uint32(len(intent.StateValue)))
	p = append(p, intent.StateValue...)
	if intent.OutputQueue == "" {
		p = append(p, 0)
	} else {
		p = append(p, 1)
		p = appendU16(p, uint16(len(intent.OutputQueue)))
		p = append(p, intent.OutputQueue...)
		p = appendU64(p, intent.OutputIncarnation)
		p = appendU32(p, uint32(len(intent.OutputValue)))
		p = append(p, intent.OutputValue...)
	}
	status, value, err := c.jobRequest(ctx, opJobComplete, intent.Queue, p)
	if err != nil {
		return nil, err
	}
	if err = jobStatusError(status, value, "job completion"); err != nil {
		return nil, err
	}
	commitID, stateVersion, outputMessageID, completedAt, receiptExpires, replayed, err := decodeJobBody(value)
	if err != nil {
		return nil, err
	}
	return &JobCompletionResult{
		CommitID:         commitID,
		StateVersion:     stateVersion,
		OutputMessageID:  outputMessageID,
		CompletedAtMS:    completedAt,
		ReceiptExpiresMS: receiptExpires,
		Replayed:         replayed,
	}, nil
}

// JobCompletion looks up a retained completion receipt by operation id
// (opcode 0x72). The lookup never requires the (now stale) delivery proof
// and works after a restart. Returns nil, nil on a miss.
func (c *Client) JobCompletion(ctx context.Context, operationID OperationID) (*JobReceipt, error) {
	status, value, err := c.jobRequest(ctx, opJobReceipt, "", operationID[:])
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = jobStatusError(status, value, "job receipt lookup"); err != nil {
		return nil, err
	}
	commitID, stateVersion, outputMessageID, completedAt, receiptExpires, _, err := decodeJobBody(value)
	if err != nil {
		return nil, err
	}
	return &JobReceipt{
		OperationID:      operationID,
		CommitID:         commitID,
		StateVersion:     stateVersion,
		OutputMessageID:  outputMessageID,
		CompletedAtMS:    completedAt,
		ReceiptExpiresMS: receiptExpires,
	}, nil
}

// JobStateValue is one durable-state entry: the exact value bytes, its
// version, and the commit id that last wrote it. The "durable" keyspace is
// fixed, non-evictable, and never expires. (Named JobStateValue because
// StateValue is the single-flight result state.)
type JobStateValue struct {
	Value    []byte
	Version  uint64
	CommitID uint64
}

// StateGet reads one durable-state entry (opcode 0x73). Returns nil, nil
// when the key is absent.
func (c *Client) StateGet(ctx context.Context, key string) (*JobStateValue, error) {
	if key == "" || len(key) > maxKey {
		return nil, fmt.Errorf("kuttidb: invalid durable state key")
	}
	status, value, err := c.jobRequest(ctx, opStateGet, key, nil)
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = jobStatusError(status, value, "durable state read"); err != nil {
		return nil, err
	}
	if len(value) < 16 {
		return nil, fmt.Errorf("kuttidb: malformed durable state response")
	}
	return &JobStateValue{
		Version:  binary.LittleEndian.Uint64(value),
		CommitID: binary.LittleEndian.Uint64(value[8:]),
		Value:    append([]byte(nil), value[16:]...),
	}, nil
}

// StateOptions carries the version precondition and the caller-owned
// operation id of a direct durable-state mutation. A zero OperationID
// generates one; retries must reuse the returned receipt's id unchanged.
type StateOptions struct {
	ExpectedVersion uint64
	OperationID     OperationID
}

// JobMutationReceipt is the receipt of one direct durable-state mutation
// from the shared operation-id ledger. Kind is "state_put" or
// "state_delete".
type JobMutationReceipt struct {
	OperationID      OperationID
	Kind             string
	CommitID         uint64
	StateVersion     uint64
	CompletedAtMS    uint64
	ReceiptExpiresMS uint64
	Replayed         bool
}

// decodeMutationBody decodes the 33-byte direct-mutation receipt body:
// [commit:8][version:8][completed_at:8][receipt_expires_at:8][replayed:1].
func decodeMutationBody(value []byte) (uint64, uint64, uint64, uint64, bool, error) {
	if len(value) != 33 {
		return 0, 0, 0, 0, false, fmt.Errorf("kuttidb: malformed durable state response")
	}
	return binary.LittleEndian.Uint64(value),
		binary.LittleEndian.Uint64(value[8:]),
		binary.LittleEndian.Uint64(value[16:]),
		binary.LittleEndian.Uint64(value[24:]),
		value[32] != 0, nil
}

// StatePut performs a version-checked direct durable-state PUT with its
// own receipt (opcode 0x74). ExpectedVersion 0 creates only; a positive
// value must match the current version exactly (no unchecked overwrite
// path exists). The same operation id may be retried unchanged to
// reconcile a lost response; a reused id with different content is an
// idempotency_conflict error.
func (c *Client) StatePut(ctx context.Context, key string, value []byte, options StateOptions) (*JobMutationReceipt, error) {
	if key == "" || len(key) > maxKey {
		return nil, fmt.Errorf("kuttidb: invalid durable state key")
	}
	if len(value) > maxValue {
		return nil, ErrValueTooLarge
	}
	id := options.OperationID
	if id.IsZero() {
		id = NewOperationID()
	}
	p := make([]byte, 0, 24+len(value))
	p = append(p, id[:]...)
	p = appendU64(p, options.ExpectedVersion)
	p = append(p, value...)
	status, resp, err := c.jobRequest(ctx, opStatePut, key, p)
	if err != nil {
		return nil, err
	}
	if err = jobStatusError(status, resp, "durable state put"); err != nil {
		return nil, err
	}
	commitID, version, completedAt, receiptExpires, replayed, err := decodeMutationBody(resp)
	if err != nil {
		return nil, err
	}
	return &JobMutationReceipt{
		OperationID:      id,
		Kind:             "state_put",
		CommitID:         commitID,
		StateVersion:     version,
		CompletedAtMS:    completedAt,
		ReceiptExpiresMS: receiptExpires,
		Replayed:         replayed,
	}, nil
}

// StateDelete performs a version-checked direct durable-state DELETE with
// its own receipt (opcode 0x75). Requires the entry's current positive
// version. Retrying a committed delete with the same id returns its
// retained receipt even though the entry is already absent; deleting an
// absent key without a retained receipt is a definite not_found error.
func (c *Client) StateDelete(ctx context.Context, key string, options StateOptions) (*JobMutationReceipt, error) {
	if key == "" || len(key) > maxKey {
		return nil, fmt.Errorf("kuttidb: invalid durable state key")
	}
	if options.ExpectedVersion == 0 {
		return nil, fmt.Errorf("kuttidb: durable state delete requires the entry's current version")
	}
	id := options.OperationID
	if id.IsZero() {
		id = NewOperationID()
	}
	p := make([]byte, 0, 24)
	p = append(p, id[:]...)
	p = appendU64(p, options.ExpectedVersion)
	status, resp, err := c.jobRequest(ctx, opStateDelete, key, p)
	if err != nil {
		return nil, err
	}
	if err = jobStatusError(status, resp, "durable state delete"); err != nil {
		return nil, err
	}
	commitID, version, completedAt, receiptExpires, replayed, err := decodeMutationBody(resp)
	if err != nil {
		return nil, err
	}
	return &JobMutationReceipt{
		OperationID:      id,
		Kind:             "state_delete",
		CommitID:         commitID,
		StateVersion:     version,
		CompletedAtMS:    completedAt,
		ReceiptExpiresMS: receiptExpires,
		Replayed:         replayed,
	}, nil
}

// DurableOperationReceipt is a retained direct-state mutation receipt from
// the shared operation-id ledger. Kind is "state_put" or "state_delete".
type DurableOperationReceipt struct {
	Kind             string
	CommitID         uint64
	StateVersion     uint64
	CompletedAtMS    uint64
	ReceiptExpiresMS uint64
}

// DurableOperation looks up a retained direct-state mutation receipt
// (opcode 0x76, shared operation-id ledger). Returns nil, nil on a miss.
func (c *Client) DurableOperation(ctx context.Context, operationID OperationID) (*DurableOperationReceipt, error) {
	status, value, err := c.jobRequest(ctx, opDurableOperation, "", operationID[:])
	if err != nil {
		return nil, err
	}
	if status == statusMiss {
		return nil, nil
	}
	if err = jobStatusError(status, value, "durable operation lookup"); err != nil {
		return nil, err
	}
	if len(value) != 33 {
		return nil, fmt.Errorf("kuttidb: malformed durable operation response")
	}
	kind := fmt.Sprintf("kind_%d", value[0])
	switch value[0] {
	case 2:
		kind = "state_put"
	case 3:
		kind = "state_delete"
	}
	return &DurableOperationReceipt{
		Kind:             kind,
		CommitID:         binary.LittleEndian.Uint64(value[1:]),
		StateVersion:     binary.LittleEndian.Uint64(value[9:]),
		CompletedAtMS:    binary.LittleEndian.Uint64(value[17:]),
		ReceiptExpiresMS: binary.LittleEndian.Uint64(value[25:]),
	}, nil
}
