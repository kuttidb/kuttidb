/* Examples target the client APIs in clients/. Keep snippets complete and copyable. */
window.kuttiExamples = {
  python: {
    name: 'Python',
    install: 'python3 -m pip install --pre kuttidb',
    source: 'clients/python',
    note: 'Install the client, then connect to a server on port 7379. Save as app.py and run with python3 app.py.',
    cache: `from kuttidb import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.put("report:42", b"ready", ttl=60)
    status = db.get("report:42")
    print(status)

# b'ready'`,
    queue: `from kuttidb import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.queue_declare("reports", durable=True)
    db.queue_publish("reports", b"report:42")
    job = db.queue_consume("reports", visibility=30)
    if job:
        print(job["value"])
        db.queue_ack("reports", job["id"])`,
    stream: `from kuttidb import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    db.stream_declare("events", partitions=1)
    db.stream_append("events", b"report.ready")
    history = db.stream_fetch(
        "events", partition=0, offset=0
    )
    for event in history:
        print(event["value"])`,
    complete: `from kuttidb import KuttiDBClient

with KuttiDBClient(port=7379) as db:
    # One-time setup: durable queues + a named consumer.
    db.queue_declare("extract-pdf", durable=True)
    db.queue_declare("index-text", durable=True)
    db.queue_consumer_register("pdf-worker")

    delivery = db.job_consume("extract-pdf", "pdf-worker")
    if delivery is None:
        raise SystemExit(0)

    text = extract_pdf(delivery.value)  # your work, outside KuttiDB

    intent = delivery.to_intent(
        state_key="pdf:42", expected_version=0,
        state_value=text,
        output_queue="index-text",
        output_incarnation=out_inc,   # from db.queue_manifest()
        output_value=b"pdf:42",
    )
    result = db.job_complete(intent, proof=delivery.proof)
    print(result.commit_id, result.replayed)

# No separate ACK: state, ACK, next message, and the receipt
# committed together. Retry the same intent after a restart.`
  },
  node: {
    name: 'Node.js',
    install: 'npm install @kuttidb/client@beta',
    source: 'clients/nodejs',
    note: 'Install in your Node.js project. With a server on port 7379, save as app.cjs and run node app.cjs.',
    cache: `const { Client } = require("@kuttidb/client");

async function main() {
  const db = new Client({ port: 7379 });
  try {
    await db.put("report:42", Buffer.from("ready"), { ttl: 60 });
    const status = await db.get("report:42");
    console.log(status?.toString());
  } finally {
    await db.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });`,
    queue: `const { Client } = require("@kuttidb/client");

async function main() {
  const db = new Client({ port: 7379 });
  try {
    await db.queueDeclare("reports", { durable: true });
    await db.queuePublish("reports", Buffer.from("report:42"));
    const job = await db.queueConsume("reports", { visibility: 30 });
    if (job) {
      console.log(job.value.toString());
      await db.queueAck("reports", job.id);
    }
  } finally {
    await db.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });`,
    stream: `const { Client } = require("@kuttidb/client");

async function main() {
  const db = new Client({ port: 7379 });
  try {
    await db.streamDeclare("events", { partitions: 1 });
    await db.streamAppend("events", Buffer.from("report.ready"));
    const history = await db.streamFetch("events", { offset: 0 });
    for (const event of history) console.log(event.value.toString());
  } finally {
    await db.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });`,
    complete: `const { Client } = require("@kuttidb/client");

async function main() {
  const db = new Client({ port: 7379 });
  try {
    await db.queueDeclare("extract-pdf", { durable: true });
    await db.queueDeclare("index-text", { durable: true });
    await db.queueConsumerRegister("pdf-worker");

    const delivery = await db.jobConsume("extract-pdf", "pdf-worker");
    if (!delivery) return;

    const text = extractPdf(delivery.value); // your work

    const intent = delivery.toIntent({
      stateKey: "pdf:42", expectedVersion: 0,
      stateValue: text,
      outputQueue: "index-text",
      outputIncarnation: outInc,      // from db.queueManifest()
      outputValue: Buffer.from("pdf:42")
    });
    const result = await db.jobComplete(intent, delivery.proof);
    console.log(result.commitId, result.replayed);
  } finally {
    await db.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });`,
    complete: `const { Client } = require("@kuttidb/client");

async function main() {
  const db = new Client({ port: 7379 });
  try {
    await db.queueDeclare("extract-pdf", { durable: true });
    await db.queueDeclare("index-text", { durable: true });
    await db.queueConsumerRegister("pdf-worker");

    const delivery = await db.jobConsume("extract-pdf", "pdf-worker");
    if (!delivery) return;

    const text = extractPdf(delivery.value); // your work

    const intent = delivery.toIntent({
      stateKey: "pdf:42", expectedVersion: 0,
      stateValue: text,
      outputQueue: "index-text",
      outputIncarnation: outInc,      // from db.queueManifest()
      outputValue: Buffer.from("pdf:42")
    });
    const result = await db.jobComplete(intent, delivery.proof);
    console.log(result.commitId, result.replayed);
  } finally {
    await db.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });`
  },
  go: {
    name: 'Go',
    install: 'go get github.com/kuttidb/kuttidb/clients/go',
    source: 'clients/go',
    note: 'Add to an initialized Go module. With a server on port 7379, save as main.go and run go run .',
    cache: `package main

import (
    "fmt"
    "time"
    kuttidb "github.com/kuttidb/kuttidb/clients/go"
)

func main() {
    db, err := kuttidb.New("127.0.0.1:7379", 1)
    if err != nil { panic(err) }
    defer db.Close()

    err = db.PutWithTTL("report:42", []byte("ready"), time.Minute)
    if err != nil { panic(err) }
    status, err := db.Get("report:42")
    if err != nil { panic(err) }
    fmt.Println(string(status))
}`,
    queue: `package main

import (
    "fmt"
    "time"
    kuttidb "github.com/kuttidb/kuttidb/clients/go"
)

func main() {
    db, err := kuttidb.New("127.0.0.1:7379", 1)
    if err != nil { panic(err) }
    defer db.Close()

    err = db.QueueDeclare("reports", kuttidb.QueueOptions{Durable: true})
    if err != nil { panic(err) }
    _, err = db.QueuePublish("reports", []byte("report:42"), 0)
    if err != nil { panic(err) }
    job, err := db.QueueConsume("reports", 30*time.Second)
    if err != nil { panic(err) }
    if job != nil {
        fmt.Println(string(job.Value))
        if _, err = db.QueueAck("reports", job.DeliveryTag); err != nil {
            panic(err)
        }
    }
}`,
    stream: `package main

import (
    "fmt"
    kuttidb "github.com/kuttidb/kuttidb/clients/go"
)

func main() {
    db, err := kuttidb.New("127.0.0.1:7379", 1)
    if err != nil { panic(err) }
    defer db.Close()

    err = db.StreamDeclare("events", kuttidb.StreamOptions{Partitions: 1})
    if err != nil { panic(err) }
    _, err = db.StreamAppend("events", []byte("report.ready"), nil, nil)
    if err != nil { panic(err) }
    history, err := db.StreamFetch("events", 0, 0, 100)
    if err != nil { panic(err) }
    for _, event := range history { fmt.Println(string(event.Value)) }
}`,
    complete: `package main

import (
    "context"
    "fmt"
    "time"

    "github.com/kuttidb/kuttidb/clients/go"
)

// One commit: durable result + ACK + next message + receipt.
func main() {
    db, err := kuttidb.New("127.0.0.1:7379", 8)
    if err != nil { panic(err) }
    ctx := context.Background()

    delivery, err := db.JobConsume(ctx, "extract-pdf", "pdf-worker",
        30*time.Second)
    if err != nil { panic(err) }
    if delivery == nil { return }

    text := extractPdf(delivery.Value) // your work
    intent := delivery.ToIntent(kuttidb.IntentOptions{
        StateKey:          []byte("pdf:42"),
        ExpectedVersion:   0,
        StateValue:        text,
        OutputQueue:       "index-text",
        OutputIncarnation: outInc, // from db.QueueManifest(ctx)
        OutputValue:       []byte("pdf:42"),
    })
    result, err := db.JobComplete(ctx, intent, delivery.Proof)
    if err != nil { panic(err) }
    fmt.Println(result.CommitID, result.Replayed)
}`
  },
  java: {
    name: 'Java',
    install: `<dependency>
  <groupId>io.github.kuttidb</groupId>
  <artifactId>kuttidb-client</artifactId>
  <version>0.1.2</version>
</dependency>`,
    source: 'clients/java',
    note: 'Add this dependency to your Maven project (Java 17+). Save the example as App.java; connect to a server on port 7379.',
    cache: `import io.github.kuttidb.client.KuttiDBClient;
import static java.nio.charset.StandardCharsets.UTF_8;

public class App {
    public static void main(String[] args) throws Exception {
        try (var db = new KuttiDBClient("127.0.0.1", 7379)) {
            db.put("report:42", "ready".getBytes(UTF_8), 60_000);
            byte[] status = db.get("report:42");
            if (status != null) System.out.println(new String(status, UTF_8));
        }
    }
}`,
    queue: `import io.github.kuttidb.client.KuttiDBClient;
import static java.nio.charset.StandardCharsets.UTF_8;

public class App {
    public static void main(String[] args) throws Exception {
        try (var db = new KuttiDBClient("127.0.0.1", 7379)) {
            db.queueDeclare("reports",
                new KuttiDBClient.QueueOptions().durable(true));
            db.queuePublish("reports", "report:42".getBytes(UTF_8));
            var job = db.queueConsume("reports", 30_000);
            if (job != null) {
                System.out.println(new String(job.value, UTF_8));
                db.queueAck("reports", job.deliveryTag);
            }
        }
    }
}`,
    stream: `import io.github.kuttidb.client.KuttiDBClient;
import static java.nio.charset.StandardCharsets.UTF_8;

public class App {
    public static void main(String[] args) throws Exception {
        try (var db = new KuttiDBClient("127.0.0.1", 7379)) {
            db.streamDeclare("events",
                new KuttiDBClient.StreamOptions().partitions(1));
            db.streamAppend("events", "report.ready".getBytes(UTF_8), null, null);
            var history = db.streamFetch("events", 0, 0, 100);
            for (var event : history) {
                System.out.println(new String(event.value, UTF_8));
            }
        }
    }
}`,
    complete: `import io.github.kuttidb.client.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.time.Duration;

// One commit: durable result + ACK + next message + receipt.
try (KuttiDBClient db = new KuttiDBClient("127.0.0.1", 7379)) {
    JobDelivery delivery = db.jobConsume(
        "extract-pdf", "pdf-worker", Duration.ofSeconds(30));
    if (delivery != null) {
        byte[] text = extractPdf(delivery.value()); // your work
        JobCompletionIntent intent = delivery.toIntent()
            .stateKey("pdf:42".getBytes(UTF_8))
            .expectedVersion(0)
            .stateValue(text)
            .outputQueue("index-text")
            .outputIncarnation(outInc)   // from db.queueManifest()
            .outputValue("pdf:42".getBytes(UTF_8))
            .build();
        JobCompletionResult result = db.jobComplete(intent, delivery.proof());
        System.out.println(result.commitId() + " replayed=" + result.replayed());
    }
}`
  },
  rust: {
    name: 'Rust',
    install: 'cargo add kuttidb@0.1.0',
    source: 'clients/rust',
    note: 'Add to a Cargo project. With a server on port 7379, save as src/main.rs and run cargo run.',
    cache: `use std::time::Duration;
use kuttidb::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Client::connect("127.0.0.1:7379")?;
    db.put("report:42", b"ready", Some(Duration::from_secs(60)))?;
    if let Some(status) = db.get("report:42")? {
        println!("{}", String::from_utf8_lossy(&status));
    }
    Ok(())
}`,
    queue: `use std::time::Duration;
use kuttidb::{Client, QueueOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Client::connect("127.0.0.1:7379")?;
    db.queue_declare("reports", QueueOptions {
        durable: true, ..Default::default()
    })?;
    db.queue_publish("reports", b"report:42", None)?;
    if let Some(job) = db.queue_consume("reports", Duration::from_secs(30))? {
        println!("{}", String::from_utf8_lossy(&job.value));
        db.queue_ack("reports", job.delivery_tag)?;
    }
    Ok(())
}`,
    stream: `use kuttidb::{Client, StreamOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Client::connect("127.0.0.1:7379")?;
    db.stream_declare("events", StreamOptions {
        partitions: 1, ..Default::default()
    })?;
    db.stream_append("events", b"report.ready", b"", None)?;
    for event in db.stream_fetch("events", 0, 0, 100)? {
        println!("{}", String::from_utf8_lossy(&event.value));
    }
    Ok(())
}`,
    complete: `use kuttidb::{Client};
use std::time::Duration;

// One commit: durable result + ACK + next message + receipt.
fn main() -> Result<(), kuttidb::Error> {
    let db = Client::connect("127.0.0.1:7379")?;
    db.queue_consumer_register("pdf-worker")?;
    let delivery = db
        .job_consume("extract-pdf", "pdf-worker", Duration::from_secs(30))?
        .expect("queue empty");

    let text = extract_pdf(&delivery.value); // your work
    let intent = delivery.to_intent(
        b"pdf:42",          // state key
        0,                  // expected version (create-only)
        text,
        Some(("index-text", out_inc, &b"pdf:42"[..])), // next job
    );
    let result = db.job_complete(&intent, &delivery.proof)?;
    println!("{} replayed={}", result.commit_id, result.replayed);
    Ok(())
}`
  },
  c: {
    name: 'C / C++',
    install: 'make\ncc app.c -Isrc -L. -lkuttidb_embed -Wl,-rpath,. -o app',
    source: 'src/embed.h',
    note: 'Shared-memory cache only. Start the server with an embed region at ./data/db.embed (see client setup). Use the socket client (libkuttidb_client) for queues, streams, and atomic job completion. For C++, use c++ in place of cc.',
    cache: `#include <stdio.h>
#ifdef __cplusplus
extern "C" {
#endif
#include "embed.h"
#ifdef __cplusplus
}
#endif

int main(void) {
    KuttiEmbed *client = kuttidb_embed_open("./data/db.embed");
    if (!client) return 1;
    if (kuttidb_embed_put(client, "report:42", 9, "ready", 5, 60000) < 0) {
        kuttidb_embed_close(client);
        return 1;
    }

    KuttiVec value = {0};
    int found = kuttidb_get_into(kuttidb_embed_cache(client),
                               "report:42", 9, &value);
    if (found > 0) printf("%.*s\\n", (int)value.len, value.data);
    kuttidb_free_value(value.data);
    kuttidb_embed_close(client);
    return found < 0 ? 1 : 0;`,
    complete: `/* Atomic job completion uses the companion socket client
 * (libkuttidb_client + kuttidb_client.h); the shared-memory cache ABI
 * stays cache-only. Link: cc app.c -lkuttidb_client. */
#include <kuttidb_client.h>
#include <stdio.h>

int main(void) {
    KuttiDBClientOptions opts = {0};
    opts.port = 7379;
    KuttiDBClient *db = kuttidb_client_create(&opts);
    int supported = 0;
    if (kuttidb_job_check_supported(db, &supported) != KUTTIDB_JOB_OK
        || !supported) return 1;

    KuttiDBJobDelivery d;
    if (kuttidb_job_consume(db, "extract-pdf", 11, "pdf-worker", 10,
                            30.0, &d) != KUTTIDB_JOB_OK) return 1;

    unsigned char op_id[KUTTIDB_JOB_ID_LEN];
    kuttidb_job_new_operation_id(op_id);
    KuttiDBJobOutput out = {"index-text", 10, out_inc,
                            (const unsigned char *)"pdf:42", 6};
    KuttiDBJobCompletion req = {0};
    req.operation_id = op_id;
    req.input_queue = "extract-pdf"; req.input_queue_len = 11;
    req.input_incarnation = d.queue_incarnation;
    req.input_message_id = d.message_id;
    req.proof = d.proof;
    req.state_key = (const unsigned char *)"pdf:42";
    req.state_key_len = 6;
    req.expected_version = 0;
    req.state_value = extracted;          /* your work's result */
    req.state_value_len = extracted_len;
    req.output = &out;

    KuttiDBJobCompletionResult result;
    if (kuttidb_job_complete(db, &req, &result) != KUTTIDB_JOB_OK)
        return 1;
    printf("%llu replayed=%d\n",
           (unsigned long long)result.commit_id, result.replayed);
    return 0;
}`
  }
};
