/* C companion client smoke: exercises the full atomic job completion
 * surface over TCP against a spawned server, including the restart-replay
 * recovery path. The C++ variant (test_job_client_cpp.cpp) proves the
 * header compiles and links under C++. */
#include "kuttidb_client.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures;

#define CHECK(cond, name)                                     \
    do {                                                      \
        if (!(cond)) {                                        \
            fprintf(stderr, "FAIL: %s (%s)\n", name,          \
                    kuttidb_last_error(client));              \
            failures++;                                       \
        } else {                                              \
            printf("ok: %s\n", name);                         \
        }                                                     \
    } while (0)

static void spawn_existing_server(const char *wal_path, char *port_text,
                                  pid_t *pid_out);

static void spawn_server(char *wal_path, char *port_text, pid_t *pid_out) {
    char tmpl[] = "/tmp/kuttidb-cclient-XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd < 0) { perror("mkstemp"); exit(1); }
    close(fd);
    unlink(tmpl);
    strcpy(wal_path, tmpl);
    spawn_existing_server(wal_path, port_text, pid_out);
}

/* Starts a server on an EXISTING WAL (restart/recovery path). */
static void spawn_existing_server(const char *wal_path, char *port_text,
                                  pid_t *pid_out) {
    int port = 17000 + (getpid() % 20000);
    snprintf(port_text, 8, "%d", port);
    pid_t pid = fork();
    if (pid == 0) {
        execl("./kuttidb", "./kuttidb", port_text, wal_path,
              "--job-completion", (char *)NULL);
        _exit(127);
    }
    *pid_out = pid;
    /* wait for readiness */
    for (int i = 0; i < 100; i++) {
        KuttiDBClientOptions opts = {0};
        opts.port = port;
        opts.timeout_seconds = 1.0;
        KuttiDBClient *probe = kuttidb_client_create(&opts);
        int supported = 0;
        KuttidbJobStatus st = kuttidb_job_check_supported(probe, &supported);
        kuttidb_client_destroy(probe);
        if (st == KUTTIDB_JOB_OK) return;
        usleep(50000);
    }
    fprintf(stderr, "server did not start\n");
    exit(1);
}

static void stop_server(pid_t pid) {
    kill(pid, SIGTERM);
    waitpid(pid, NULL, 0);
}

int main(void) {
    char wal_path[128], port_text[8];
    pid_t server = 0;
    spawn_server(wal_path, port_text, &server);
    int port = atoi(port_text);

    KuttiDBClientOptions opts = {0};
    opts.port = port;
    KuttiDBClient *client = kuttidb_client_create(&opts);
    int supported = 0;
    CHECK(kuttidb_job_check_supported(client, &supported) == KUTTIDB_JOB_OK &&
              supported == 1,
          "capability negotiation");

    /* Queue setup through the manifest flow: declare via the wire using
     * the same client (declare is not part of the job surface; the C
     * companion exposes the job surface only, so setup uses a raw frame). */
    {
        /* Queue declarations and publishing use a helper script: the
         * companion covers the job opcodes, while declare/publish stay on
         * the general wire (the manifest/consume/complete flow below is
         * what the companion API owns). */
        char script[512], cmd[600];
        snprintf(script, sizeof script, "/tmp/kuttidb-cclient-setup.%d.py",
                 (int)getpid());
        FILE *f = fopen(script, "w");
        if (!f) { perror("setup script"); return 1; }
        fprintf(f,
                "import socket, struct\n"
                "def req(s, o, k=b'', v=b''):\n"
                "    s.sendall(struct.pack('<BHI', o, len(k), len(v)) + k + v)\n"
                "    s.recv(5)\n"
                "s = socket.create_connection(('127.0.0.1', %s), timeout=5)\n"
                "req(s, 0x20, b'extract-pdf', struct.pack('<BQ', 1, 0))\n"
                "req(s, 0x20, b'index-text', struct.pack('<BQ', 1, 0))\n"
                "req(s, 0x21, b'extract-pdf', b'pdf-bytes')\n",
                port_text);
        fclose(f);
        snprintf(cmd, sizeof cmd,
                 "python3 kuttidb-cli -p %s consumer-register pdf-worker "
                 ">/dev/null 2>&1 && python3 %s 2>&1",
                 port_text, script);
        int rc = system(cmd);
        unlink(script);
        CHECK(rc == 0, "queue setup");
    }

    KuttiDBQueueManifest manifest;
    memset(&manifest, 0, sizeof manifest);
    CHECK(kuttidb_queue_manifest(client, &manifest) == KUTTIDB_JOB_OK &&
              manifest.count == 2,
          "manifest");
    uint64_t in_inc = 0, out_inc = 0;
    for (uint32_t i = 0; i < manifest.count; i++) {
        if (strcmp(manifest.entries[i].name, "extract-pdf") == 0)
            in_inc = manifest.entries[i].incarnation;
        if (strcmp(manifest.entries[i].name, "index-text") == 0)
            out_inc = manifest.entries[i].incarnation;
    }
    CHECK(in_inc && out_inc, "incarnations discovered");
    kuttidb_queue_manifest_free(&manifest);

    KuttiDBJobDelivery delivery;
    memset(&delivery, 0, sizeof delivery);
    CHECK(kuttidb_job_consume(client, "extract-pdf", 11, "pdf-worker", 10,
                              30.0, &delivery) == KUTTIDB_JOB_OK &&
              delivery.message_id != 0,
          "job consume");
    CHECK(delivery.queue_incarnation == in_inc && delivery.value_len == 9,
          "delivery identity");

    unsigned char op_id[16];
    CHECK(kuttidb_job_new_operation_id(op_id) == 0, "operation id");

    KuttiDBJobOutput output = {"index-text", 10, out_inc,
                               (const unsigned char *)"pdf:42", 6};
    KuttiDBJobCompletion completion = {0};
    completion.operation_id = op_id;
    completion.input_queue = "extract-pdf";
    completion.input_queue_len = 11;
    completion.input_incarnation = delivery.queue_incarnation;
    completion.input_message_id = delivery.message_id;
    completion.proof = delivery.proof;
    completion.state_key = (const unsigned char *)"pdf:42";
    completion.state_key_len = 6;
    completion.expected_version = 0;
    completion.state_value = (const unsigned char *)"extracted";
    completion.state_value_len = 9;
    completion.output = &output;

    KuttiDBJobCompletionResult result;
    memset(&result, 0, sizeof result);
    CHECK(kuttidb_job_complete(client, &completion, &result) ==
                  KUTTIDB_JOB_OK &&
              !result.replayed && result.commit_id != 0 &&
              result.output_message_id != 0,
          "atomic completion");

    KuttiDBStateValue state;
    memset(&state, 0, sizeof state);
    CHECK(kuttidb_state_get(client, "pdf:42", 6, &state) == KUTTIDB_JOB_OK &&
              state.version == result.state_version && state.value_len == 9,
          "state read-back");
    kuttidb_state_value_free(&state);

    KuttiDBJobReceipt receipt;
    memset(&receipt, 0, sizeof receipt);
    CHECK(kuttidb_job_completion(client, op_id, &receipt) ==
              KUTTIDB_JOB_OK &&
              receipt.commit_id == result.commit_id,
          "receipt lookup");

    /* Restart on the SAME WAL: the same operation id replays its original
     * result. */
    kuttidb_client_destroy(client);
    stop_server(server);
    spawn_existing_server(wal_path, port_text, &server);
    opts.port = port;
    client = kuttidb_client_create(&opts);
    CHECK(kuttidb_job_check_supported(client, &supported) == KUTTIDB_JOB_OK,
          "reconnect after restart");

    KuttiDBJobCompletionResult replay;
    memset(&replay, 0, sizeof replay);
    CHECK(kuttidb_job_complete(client, &completion, &replay) ==
              KUTTIDB_JOB_OK,
          "replay after restart");
    CHECK(replay.replayed == 1 && replay.commit_id == result.commit_id &&
              replay.state_version == result.state_version &&
              replay.output_message_id == result.output_message_id,
          "replay: immutable original result");

    /* Direct state mutations with the shared ledger. */
    unsigned char put_id[16];
    kuttidb_job_new_operation_id(put_id);
    KuttiDBJobMutationReceipt mutation;
    memset(&mutation, 0, sizeof mutation);
    CHECK(kuttidb_state_put(client, "pdf:42", 6,
                            (const unsigned char *)"corrected", 8, 1,
                            put_id, &mutation) == KUTTIDB_JOB_OK &&
              !mutation.replayed && mutation.state_version == 2,
          "state put");
    KuttiDBJobMutationReceipt replay_mutation;
    CHECK(kuttidb_state_put(client, "pdf:42", 6,
                            (const unsigned char *)"corrected", 8, 1,
                            put_id, &replay_mutation) == KUTTIDB_JOB_OK &&
              replay_mutation.replayed == 1 &&
              replay_mutation.commit_id == mutation.commit_id,
          "state put replay");
    unsigned char del_id[16];
    kuttidb_job_new_operation_id(del_id);
    KuttiDBJobMutationReceipt deletion;
    CHECK(kuttidb_state_delete(client, "pdf:42", 6, 2, del_id, &deletion) ==
              KUTTIDB_JOB_OK,
          "state delete");
    CHECK(kuttidb_state_get(client, "pdf:42", 6, &state) ==
              KUTTIDB_JOB_NOT_FOUND,
          "state absent after delete");
    KuttiDBDurableOperation durable;
    CHECK(kuttidb_durable_operation(client, put_id, &durable) ==
                  KUTTIDB_JOB_OK &&
              durable.kind == 2,
          "durable operation lookup");
    CHECK(kuttidb_state_put(client, "pdf:42", 6,
                            (const unsigned char *)"other", 5, 0, put_id,
                            &mutation) == KUTTIDB_JOB_IDEMPOTENCY_CONFLICT,
          "idempotency conflict");
    kuttidb_client_destroy(client);
    stop_server(server);
    unlink(wal_path);

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("job_client_test: OK\n");
    return 0;
}
