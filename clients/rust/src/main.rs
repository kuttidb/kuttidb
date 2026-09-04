use kuttidb::{
    Client, Error, ExchangeOptions, ExchangeType, Item, Pool, QueueOptions, SingleFlightState,
    StreamCommit, StreamOptions,
};
use std::time::Duration;

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn main() -> Result<(), Error> {
    let addr = "127.0.0.1:7394";
    let auth_token = std::env::var("KUTTIDB_AUTH_TOKEN").ok();

    // single ops
    let mut c = match &auth_token {
        Some(token) => Client::connect_authenticated(addr, token.as_bytes())?,
        None => Client::connect(addr)?,
    };
    c.put("hello", b"world", None)?;
    assert_eq!(c.get("hello")?, Some(b"world".to_vec()));
    assert_eq!(c.get("nope")?, None);
    c.put("hello", b"world2", None)?;
    assert_eq!(c.get("hello")?, Some(b"world2".to_vec()));
    assert!(c.delete("hello")?);
    assert!(!c.delete("hello")?);
    assert_eq!(c.get("hello")?, None);

    // ttl
    c.put("ttl-key", b"brief", Some(Duration::from_secs(30)))?;
    assert_eq!(c.get("ttl-key")?, Some(b"brief".to_vec()));

    // batched without ttl
    let items: Vec<(&str, &[u8])> = (0..1000)
        .map(|i| (leak_str(format!("g{i}")), b"v".as_slice()))
        .collect();
    c.put_many(&items)?;
    let keys: Vec<&str> = (0..1000).map(|i| leak_str(format!("g{i}"))).collect();
    let got = c.get_many(&keys)?;
    assert!(got.iter().all(|v| v.as_deref() == Some(b"v".as_slice())));

    // batched with per-item ttl
    let ttl_items: Vec<Item> = (0..100)
        .map(|i| {
            let key: &'static str = leak_str(format!("bt{i}"));
            let ttl = if i % 2 == 0 {
                None
            } else {
                Some(Duration::from_secs(30))
            };
            Item {
                key,
                value: b"y10",
                ttl,
            }
        })
        .collect();
    let refs: Vec<(&str, &[u8], Option<Duration>)> = ttl_items
        .iter()
        .map(|it| (it.key, it.value, it.ttl))
        .collect();
    c.put_many_ttl(&refs)?;
    assert_eq!(c.get("bt0")?, Some(b"y10".to_vec()));
    assert_eq!(c.get("bt1")?, Some(b"y10".to_vec()));

    let caps = c.capabilities()?;
    assert_eq!(caps.major, 1);
    assert!(c.health()?);

    c.queue_declare("rust-jobs", QueueOptions::default())?;
    let id = c.queue_publish("rust-jobs", b"one", None)?;
    assert!(id > 0);
    let delivery = c
        .queue_consume("rust-jobs", Duration::from_secs(5))?
        .unwrap();
    assert_eq!(delivery.value, b"one");
    assert!(c.queue_ack("rust-jobs", delivery.delivery_tag)?);
    let ids = c.queue_publish_batch("rust-jobs", &[b"two", b"three"])?;
    assert_eq!(ids.len(), 2);
    let deliveries = c.queue_consume_batch("rust-jobs", 2)?;
    let tags: Vec<u64> = deliveries.iter().map(|d| d.delivery_tag).collect();
    assert_eq!(c.queue_ack_batch("rust-jobs", &tags)?, 2);
    c.queue_consumer_register("rust-worker")?;
    c.queue_publish("rust-jobs", b"named", None)?;
    let named = c
        .queue_consume_as("rust-jobs", "rust-worker", Duration::from_secs(5))?
        .unwrap();
    assert!(c.queue_nack("rust-jobs", named.delivery_tag, false, Duration::ZERO)?);
    c.queue_consumer_unregister("rust-worker")?;
    assert!(!c.queue_list()?.is_empty());

    c.exchange_declare(
        "rust-events",
        ExchangeOptions {
            exchange_type: ExchangeType::Topic,
            ..Default::default()
        },
    )?;
    c.exchange_bind("rust-events", "rust-jobs", "order.*")?;
    assert_eq!(
        c.exchange_publish("rust-events", "order.new", b"event", None)?,
        1
    );
    assert!(
        c.put_and_enqueue("rust-a1", b"value", "rust-jobs", None)?
            .transaction_id
            > 0
    );
    assert_eq!(
        c.put_and_publish("rust-a2", b"value", "rust-events", "order.x", None)?
            .routed,
        1
    );
    assert_eq!(
        c.update_and_emit("rust-a2", b"updated", "rust-events", "order.x", None)?
            .routed,
        1
    );
    assert_eq!(
        c.delete_and_publish("rust-a2", "rust-events", "order.x", b"deleted")?
            .routed,
        1
    );

    let loaded = c.get_or_load(
        "rust-load",
        || Ok(Some(b"loaded".to_vec())),
        Some(Duration::from_secs(30)),
        Duration::from_secs(5),
        Duration::from_secs(10),
    )?;
    assert_eq!(loaded.as_deref(), Some(b"loaded".as_slice()));
    c.put_swr(
        "rust-swr",
        b"fresh",
        Duration::from_secs(30),
        Duration::from_secs(60),
        None,
    )?;
    let swr = c.get_or_refresh("rust-swr", Duration::from_secs(5))?;
    assert_eq!(swr.state, SingleFlightState::Value);

    c.stream_declare(
        "rust-stream",
        StreamOptions {
            partitions: 2,
            ..Default::default()
        },
    )?;
    let pos = c.stream_append("rust-stream", b"record", b"key-1", None)?;
    let partition = pos.partition as u32;
    c.stream_append_batch("rust-stream", &[(b"key-2", b"record-2")], Some(partition))?;
    let records = c.stream_fetch("rust-stream", partition, 0, 10)?;
    assert!(records.len() >= 2);
    assert_eq!(records[0].key.as_deref(), Some(b"key-1".as_slice()));
    let assignment = c.stream_group_join("rust-stream", "rust-group", Duration::from_secs(30))?;
    assert!(!assignment.partitions.is_empty());
    c.stream_commit("rust-stream", "rust-group", partition, 1)?;
    c.stream_commit_batch(
        "rust-stream",
        "rust-group",
        &[StreamCommit {
            partition,
            offset: 2,
        }],
    )?;
    assert_eq!(
        c.stream_group_offset("rust-stream", "rust-group", partition)?,
        Some(2)
    );
    assert!(c
        .stream_group_lag("rust-stream", "rust-group", partition)?
        .is_some());
    assert!(!c.stream_list()?.is_empty());
    assert!(!c.stream_group_list()?.is_empty());
    c.stream_group_leave("rust-stream", "rust-group")?;

    let stats = c.stats()?;
    println!("rust single-client ok — stats: {stats}");

    // concurrent pool: 8 threads verify distinct ranges
    let pool = match &auth_token {
        Some(token) => Pool::new_authenticated(addr, 8, token.as_bytes())?,
        None => Pool::new(addr, 8)?,
    };
    std::thread::scope(|s| {
        for w in 0..8u32 {
            let pool = &pool;
            s.spawn(move || -> Result<(), Error> {
                pool.with(|client| {
                    let keys: Vec<&str> = (w * 125..(w + 1) * 125)
                        .map(|i| leak_str(format!("g{}", 1000 + i)))
                        .collect();
                    let vals: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
                    let pairs: Vec<(&str, &[u8])> = keys
                        .iter()
                        .zip(vals.iter())
                        .map(|(k, v)| (*k, v.as_slice()))
                        .collect();
                    client.put_many(&pairs)?;
                    let got = client.get_many(&keys)?;
                    for (k, v) in keys.iter().zip(got.iter()) {
                        assert_eq!(v.as_deref(), Some(k.as_bytes()), "mismatch for {k}");
                    }
                    Ok(())
                })
            });
        }
    });

    let stats = c.stats()?;
    println!("RUST CLIENT OK — stats: {stats}");
    Ok(())
}
