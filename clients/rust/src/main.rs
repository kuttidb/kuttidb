use kuttidb::{Client, Error, Item, Pool};
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
