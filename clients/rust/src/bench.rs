use kuttidb::{Error, Item};
use std::time::{Duration, Instant};

fn main() -> Result<(), Error> {
    let workers = 8usize;
    let per = 50_000usize;
    let pool = kuttidb::Pool::new("127.0.0.1:7394", workers)?;
    let start = Instant::now();
    std::thread::scope(|s| {
        for w in 0..workers {
            let pool = &pool;
            s.spawn(move || -> Result<(), Error> {
                pool.with(|client| {
                    let mut items: Vec<Item> = Vec::with_capacity(per);
                    let mut keys: Vec<&str> = Vec::with_capacity(per);
                    for i in 0..per {
                        let key = Box::leak(format!("b{w}-{i}").into_boxed_str());
                        items.push(Item {
                            key,
                            value: b"x100bytes-padding-padding-padding-padding-pad",
                            ttl: if i % 10 == 0 {
                                Some(Duration::from_secs(60))
                            } else {
                                None
                            },
                        });
                        keys.push(key);
                    }
                    client.put_many_ttl(
                        &items
                            .iter()
                            .map(|it| (it.key, it.value, it.ttl))
                            .collect::<Vec<_>>(),
                    )?;
                    let got = client.get_many(&keys)?;
                    if got.iter().any(|v| v.is_none()) {
                        return Err(Error::Server);
                    }
                    Ok(())
                })
            });
        }
    });
    let dt = start.elapsed();
    println!(
        "{} Go... er, {} rust-client ops in {:.2}s = {:.0} ops/s",
        workers * per * 2,
        workers * per * 2,
        dt.as_secs_f64(),
        (workers * per * 2) as f64 / dt.as_secs_f64()
    );
    Ok(())
}
