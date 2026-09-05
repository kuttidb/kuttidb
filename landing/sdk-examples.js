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
        print(event["value"])`
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
}`
  },
  java: {
    name: 'Java',
    install: `<dependency>
  <groupId>io.github.kuttidb</groupId>
  <artifactId>kuttidb-client</artifactId>
  <version>0.0.8-beta</version>
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
}`
  },
  rust: {
    name: 'Rust',
    install: 'cargo add kuttidb@0.0.6-beta',
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
}`
  },
  c: {
    name: 'C / C++',
    install: 'make\ncc app.c -Isrc -L. -lkuttidb_embed -Wl,-rpath,. -o app',
    source: 'src/embed.h',
    note: 'Shared-memory cache only. Start the server with an embed region at ./data/db.embed (see client setup). Use a socket client for queues and streams. For C++, use c++ in place of cc.',
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
    return found < 0 ? 1 : 0;
}`
  }
};
