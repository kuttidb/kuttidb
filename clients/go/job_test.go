package kuttidb

// Integration coverage for the atomic job completion surface (opcodes
// 0x70-0x77). The suite builds the C server at the repository root with
// `make -j8`, spawns ./kuttidb on an ephemeral loopback port, and exercises
// manifest discovery, completion-capable consumption, atomic completion,
// receipt lookup across a SIGTERM restart, direct state mutations with
// receipts, the typed error mapping (idempotency conflict, version
// conflict, delivery expired, unsupported feature), and the context
// plumbing.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// buildServer runs `make -j8` at the repository root and returns the
// freshly ensured ./kuttidb executable path.
func buildServer(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("repository root: %v", err)
	}
	cmd := exec.Command("make", "-j8")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make -j8: %v\n%s", err, out)
	}
	bin := root + "/kuttidb"
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("server binary missing after make: %v", err)
	}
	return bin
}

// freeJobPort reserves an ephemeral loopback port and releases it for the
// server (the same reserve-then-bind window the managed test accepts).
func freeJobPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

type jobServerProc struct {
	cmd     *exec.Cmd
	port    int
	stderr  *bytes.Buffer
	exited  chan error
	mu      sync.Mutex
	stopped bool
}

// startJobServer spawns ./kuttidb on port with an optional --job-completion
// flag and waits until the TCP listener answers.
func startJobServer(t *testing.T, bin string, port int, wal string, jobs bool) *jobServerProc {
	t.Helper()
	args := []string{strconv.Itoa(port), wal}
	if jobs {
		args = append(args, "--job-completion")
	}
	cmd := exec.Command(bin, args...)
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	proc := &jobServerProc{cmd: cmd, port: port, stderr: stderr, exited: make(chan error, 1)}
	go func() { proc.exited <- cmd.Wait() }()
	t.Cleanup(proc.kill) // failure-safe: no leaked servers on t.Fatal
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for start := time.Now(); time.Since(start) < 10*time.Second; {
		select {
		case err := <-proc.exited:
			t.Fatalf("server exited during startup: %v\n%s", err, stderr.String())
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return proc
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatalf("server did not become ready:\n%s", stderr.String())
	return nil
}

// kill force-stops the server (t.Cleanup safety net; no-op after stop).
func (s *jobServerProc) kill() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()
	_ = s.cmd.Process.Kill()
	select {
	case <-s.exited:
	case <-time.After(5 * time.Second):
	}
}

// stop terminates the server with SIGTERM and waits for the exit.
func (s *jobServerProc) stop(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		t.Fatal("server did not exit on SIGTERM")
	}
}

func connectJobClient(t *testing.T, port int) *Client {
	t.Helper()
	client, err := New(fmt.Sprintf("127.0.0.1:%d", port), 2)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return client
}

func wantJobError(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s error, got nil", code)
	}
	var jobErr *JobError
	if !errors.As(err, &jobErr) {
		t.Fatalf("expected *JobError, got %T: %v", err, err)
	}
	if jobErr.Code != code {
		t.Fatalf("expected code %s, got %s (%s)", code, jobErr.Code, jobErr)
	}
	if jobErr.Outcome != JobOutcomeNotCommitted && jobErr.Outcome != JobOutcomeUnknown {
		t.Fatalf("invalid outcome %q", jobErr.Outcome)
	}
}

func TestJobCompletionIntentJSON(t *testing.T) {
	id := NewOperationID()
	if len(id) != 16 {
		t.Fatalf("operation id length: %d", len(id))
	}
	if id[6]>>4 != 4 || id[8]>>6 != 2 {
		t.Fatalf("not UUIDv4: %s", id)
	}
	parsed, err := ParseOperationID(id.String())
	if err != nil || parsed != id {
		t.Fatalf("ParseOperationID roundtrip: %v %s", err, parsed.String())
	}
	intent := JobCompletionIntent{
		OperationID:       id,
		Queue:             "extract-pdf",
		QueueIncarnation:  1 << 63, // beyond any JSON number guarantee
		MessageID:         (1 << 63) + 42,
		StateKey:          []byte("pdf:42"),
		ExpectedVersion:   7,
		StateValue:        []byte("extracted"),
		OutputQueue:       "index-text",
		OutputIncarnation: 9,
		OutputValue:       []byte("pdf:42"),
	}
	data, err := json.Marshal(intent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// 64-bit identity fields must be lossless DECIMAL STRINGS on the wire.
	if !strings.Contains(string(data), `"queue_incarnation":"`+formatDecimal(1<<63)+`"`) {
		t.Fatalf("queue incarnation is not a decimal string: %s", data)
	}
	if !strings.Contains(string(data), `"message_id":"`+formatDecimal((1<<63)+42)+`"`) {
		t.Fatalf("message id is not a decimal string: %s", data)
	}
	var revived JobCompletionIntent
	if err := json.Unmarshal(data, &revived); err != nil {
		t.Fatalf("unmarshal: %v", data)
	}
	if revived.OperationID != intent.OperationID || revived.Queue != intent.Queue ||
		revived.QueueIncarnation != intent.QueueIncarnation || revived.MessageID != intent.MessageID ||
		string(revived.StateKey) != "pdf:42" || revived.ExpectedVersion != 7 ||
		string(revived.StateValue) != "extracted" || revived.OutputQueue != "index-text" ||
		revived.OutputIncarnation != 9 || string(revived.OutputValue) != "pdf:42" {
		t.Fatalf("intent roundtrip mismatch: %+v", revived)
	}
	// No output serializes as null and roundtrips to an empty output queue.
	intent.OutputQueue, intent.OutputValue, intent.OutputIncarnation = "", nil, 0
	data, err = json.Marshal(intent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"output":null`) {
		t.Fatalf("absent output must serialize as null: %s", data)
	}
	var plain JobCompletionIntent
	if err := json.Unmarshal(data, &plain); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plain.OutputQueue != "" || plain.OutputIncarnation != 0 || len(plain.OutputValue) != 0 {
		t.Fatalf("absent output must revive empty: %+v", plain)
	}
}

func TestJobCompletionLifecycle(t *testing.T) {
	bin := buildServer(t)
	dir := t.TempDir()
	wal := dir + "/kuttidb.wal"
	port := freeJobPort(t)
	proc := startJobServer(t, bin, port, wal, true)

	db := connectJobClient(t, port)
	defer db.Close()
	ctx := context.Background()

	caps, err := db.Capabilities()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.Major != 1 || caps.Minor < 8 {
		t.Fatalf("protocol version: %d.%d", caps.Major, caps.Minor)
	}
	if caps.Features&FeatureJobs == 0 {
		t.Fatalf("CAP_JOBS not advertised: %x", caps.Features)
	}

	if err := db.QueueDeclare("extract-pdf", QueueOptions{Durable: true, MaxDepth: 100}); err != nil {
		t.Fatalf("declare extract-pdf: %v", err)
	}
	if err := db.QueueDeclare("index-text", QueueOptions{Durable: true}); err != nil {
		t.Fatalf("declare index-text: %v", err)
	}

	// Manifest: stable identity, durability, capacity, revision.
	manifest, err := db.QueueManifest(ctx)
	if err != nil {
		t.Fatalf("queue manifest: %v", err)
	}
	byName := map[string]QueueManifestEntry{}
	for _, entry := range manifest {
		byName[entry.Name] = entry
	}
	input, ok := byName["extract-pdf"]
	if !ok || !input.Durable || input.Incarnation == 0 {
		t.Fatalf("manifest missing extract-pdf: %+v", manifest)
	}
	output, ok := byName["index-text"]
	if !ok || output.Incarnation == 0 {
		t.Fatalf("manifest missing index-text: %+v", manifest)
	}
	if input.Depth != 0 || input.Inflight != 0 {
		t.Fatalf("unexpected manifest counters: %+v", input)
	}

	// Empty durable queue with a registered consumer: a clean miss.
	if _, err := db.QueueConsumerRegister("pdf-worker"); err != nil {
		t.Fatalf("consumer register: %v", err)
	}
	if delivery, err := db.JobConsume(ctx, "extract-pdf", "pdf-worker", 30*time.Second); err != nil || delivery != nil {
		t.Fatalf("consume on empty queue: %+v %v", delivery, err)
	}

	// Consume with a completion proof.
	msgID, err := db.QueuePublish("extract-pdf", []byte("pdf-bytes"), 0)
	if err != nil {
		t.Fatalf("queue publish: %v", err)
	}
	delivery, err := db.JobConsume(ctx, "extract-pdf", "pdf-worker", 30*time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("job consume: %+v %v", delivery, err)
	}
	if delivery.MessageID != msgID || delivery.QueueIncarnation != input.Incarnation {
		t.Fatalf("delivery identity: %+v (want msg %d, inc %d)", delivery, msgID, input.Incarnation)
	}
	if len(delivery.Proof) != 16 || len(delivery.StoreID) != 16 {
		t.Fatalf("delivery credentials: %d %d", len(delivery.Proof), len(delivery.StoreID))
	}
	if string(delivery.Value) != "pdf-bytes" {
		t.Fatalf("delivery payload: %q", delivery.Value)
	}

	// Compose the intent, persist its JSON BEFORE submitting (recovery path).
	intent := delivery.ToIntent(IntentOptions{
		StateKey:          "pdf:42",
		ExpectedVersion:   0,
		StateValue:        []byte("extracted"),
		OutputQueue:       "index-text",
		OutputIncarnation: output.Incarnation,
		OutputValue:       []byte("pdf:42"),
	})
	persisted, err := json.Marshal(intent)
	if err != nil {
		t.Fatalf("persist intent: %v", err)
	}
	var revivedIntent JobCompletionIntent
	if err := json.Unmarshal(persisted, &revivedIntent); err != nil {
		t.Fatalf("revive intent: %v", err)
	}

	result, err := db.JobComplete(ctx, *intent, delivery.Proof)
	if err != nil {
		t.Fatalf("job complete: %v", err)
	}
	if result.Replayed || result.CommitID == 0 || result.OutputMessageID == 0 || result.StateVersion != 1 {
		t.Fatalf("completion result: %+v", result)
	}

	// State read-back with the committing receipt identity.
	state, err := db.StateGet(ctx, "pdf:42")
	if err != nil || state == nil {
		t.Fatalf("state get: %+v %v", state, err)
	}
	if string(state.Value) != "extracted" || state.Version != result.StateVersion || state.CommitID != result.CommitID {
		t.Fatalf("state read-back: %+v vs result %+v", state, result)
	}
	if stats, err := db.QueueStats("extract-pdf"); err != nil || stats.Depth != 0 {
		t.Fatalf("input depth after ACK: %+v %v", stats, err)
	}
	if stats, err := db.QueueStats("index-text"); err != nil || stats.Depth != 1 {
		t.Fatalf("output depth: %+v %v", stats, err)
	}

	receipt, err := db.JobCompletion(ctx, intent.OperationID)
	if err != nil || receipt == nil {
		t.Fatalf("receipt lookup: %+v %v", receipt, err)
	}
	if receipt.CommitID != result.CommitID || receipt.StateVersion != result.StateVersion || receipt.OutputMessageID != result.OutputMessageID {
		t.Fatalf("receipt mismatch: %+v vs %+v", receipt, result)
	}

	// RESTART: SIGTERM, respawn on the same WAL, replay the same intent.
	db.Close()
	proc.stop(t)
	proc = startJobServer(t, bin, port, wal, true)
	db = connectJobClient(t, port)
	defer db.Close()

	manifest2, err := db.QueueManifest(ctx)
	if err != nil {
		t.Fatalf("manifest after restart: %v", err)
	}
	incarnations := map[string]uint64{}
	for _, entry := range manifest2 {
		incarnations[entry.Name] = entry.Incarnation
	}
	if incarnations["extract-pdf"] != input.Incarnation || incarnations["index-text"] != output.Incarnation {
		t.Fatalf("incarnations not stable across restart: %+v", incarnations)
	}

	replay, err := db.JobComplete(ctx, revivedIntent, make([]byte, 16))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.CommitID != result.CommitID || replay.StateVersion != result.StateVersion ||
		replay.OutputMessageID != result.OutputMessageID {
		t.Fatalf("replay mismatch: %+v vs %+v", replay, result)
	}
	if stats, err := db.QueueStats("index-text"); err != nil || stats.Depth != 1 {
		t.Fatalf("replay must not duplicate the output publish: %+v %v", stats, err)
	}

	// Direct state mutations with the shared receipt ledger.
	put, err := db.StatePut(ctx, "pdf:42", []byte("corrected"), StateOptions{ExpectedVersion: 1})
	if err != nil {
		t.Fatalf("state put: %v", err)
	}
	if put.Replayed || put.StateVersion != 2 || put.Kind != "state_put" {
		t.Fatalf("state put receipt: %+v", put)
	}
	replayPut, err := db.StatePut(ctx, "pdf:42", []byte("corrected"), StateOptions{ExpectedVersion: 1, OperationID: put.OperationID})
	if err != nil {
		t.Fatalf("state put replay: %v", err)
	}
	if !replayPut.Replayed || replayPut.CommitID != put.CommitID || replayPut.StateVersion != put.StateVersion {
		t.Fatalf("state put replay mismatch: %+v vs %+v", replayPut, put)
	}
	_, err = db.StatePut(ctx, "pdf:42", []byte("different"), StateOptions{ExpectedVersion: 2, OperationID: put.OperationID})
	wantJobError(t, err, JobCodeIdempotencyConflict)
	if !errors.Is(err, ErrJobIdempotencyConflict) {
		t.Fatalf("idempotency conflict sentinel: %v", err)
	}
	_, err = db.StatePut(ctx, "pdf:42", []byte("x"), StateOptions{ExpectedVersion: 99})
	wantJobError(t, err, JobCodeStateVersionConflict)
	if !errors.Is(err, ErrJobStateVersionConflict) {
		t.Fatalf("version conflict sentinel: %v", err)
	}
	lookup, err := db.DurableOperation(ctx, put.OperationID)
	if err != nil || lookup == nil {
		t.Fatalf("durable operation lookup: %+v %v", lookup, err)
	}
	if lookup.Kind != "state_put" || lookup.StateVersion != 2 || lookup.CommitID != put.CommitID {
		t.Fatalf("durable operation receipt: %+v", lookup)
	}

	deletion, err := db.StateDelete(ctx, "pdf:42", StateOptions{ExpectedVersion: 2})
	if err != nil {
		t.Fatalf("state delete: %v", err)
	}
	if deletion.Replayed || deletion.Kind != "state_delete" || deletion.StateVersion != 3 {
		// The tombstone takes the next version after the v2 state put.
		t.Fatalf("state delete receipt: %+v", deletion)
	}
	if state, err := db.StateGet(ctx, "pdf:42"); err != nil || state != nil {
		t.Fatalf("state get after delete: %+v %v", state, err)
	}
	retry, err := db.StateDelete(ctx, "pdf:42", StateOptions{ExpectedVersion: 2, OperationID: deletion.OperationID})
	if err != nil {
		t.Fatalf("state delete retry: %v", err)
	}
	if !retry.Replayed || retry.CommitID != deletion.CommitID {
		t.Fatalf("state delete replay: %+v vs %+v", retry, deletion)
	}

	// Fencing: a fresh delivery with a tiny lease expires before commit.
	if _, err := db.QueuePublish("extract-pdf", []byte("second"), 0); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	second, err := db.JobConsume(ctx, "extract-pdf", "pdf-worker", time.Millisecond)
	if err != nil || second == nil {
		t.Fatalf("second consume: %+v %v", second, err)
	}
	time.Sleep(60 * time.Millisecond)
	_, err = db.JobComplete(ctx, *second.ToIntent(IntentOptions{StateKey: "pdf:42", StateValue: []byte("late")}), second.Proof)
	wantJobError(t, err, JobCodeDeliveryExpired)

	db.Close()
	proc.stop(t)
}

// TestJobCompletionUnsupportedFeature asserts the explicit typed error
// against a server running WITHOUT --job-completion.
func TestJobCompletionUnsupportedFeature(t *testing.T) {
	bin := buildServer(t)
	dir := t.TempDir()
	port := freeJobPort(t)
	proc := startJobServer(t, bin, port, dir+"/kuttidb.wal", false)
	db := connectJobClient(t, port)
	defer db.Close()

	caps, err := db.Capabilities()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.Features&FeatureJobs != 0 {
		t.Fatalf("CAP_JOBS advertised without --job-completion: %x", caps.Features)
	}
	_, err = db.JobConsume(context.Background(), "extract-pdf", "pdf-worker", time.Second)
	wantJobError(t, err, JobCodeUnsupportedFeature)
	if !errors.Is(err, ErrJobUnsupportedFeature) {
		t.Fatalf("unsupported feature sentinel: %v", err)
	}
	jobErr := &JobError{}
	if errors.As(err, &jobErr) && jobErr.Outcome != JobOutcomeNotCommitted {
		t.Fatalf("outcome: %s", jobErr.Outcome)
	}
	if _, err := db.StateGet(context.Background(), "pdf:42"); !errors.Is(err, ErrJobUnsupportedFeature) {
		t.Fatalf("state get without feature: %v", err)
	}
	proc.stop(t)
}

// TestJobCompletionContextCancel verifies the context plumbing: a canceled
// context fails fast without touching the server.
func TestJobCompletionContextCancel(t *testing.T) {
	bin := buildServer(t)
	dir := t.TempDir()
	port := freeJobPort(t)
	proc := startJobServer(t, bin, port, dir+"/kuttidb.wal", true)
	db := connectJobClient(t, port)
	defer db.Close()
	defer proc.stop(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.JobConsume(ctx, "extract-pdf", "pdf-worker", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context: %v", err)
	}
	if _, err := db.StateGet(ctx, "pdf:42"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled state get: %v", err)
	}
	// The pool survives: a healthy call still works afterwards.
	if err := db.QueueDeclare("extract-pdf", QueueOptions{Durable: true}); err != nil {
		t.Fatalf("declare after canceled context: %v", err)
	}
}
