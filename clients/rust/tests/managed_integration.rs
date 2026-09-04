use std::net::{Ipv4Addr, TcpListener};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kuttidb::{Client, ManagedOptions, ManagedTransport};

/// macOS $TMPDIR (/var/folders/...) pushes "<dir>/kuttidb.sock" past the
/// 104-byte sockaddr_un limit, so prefer /tmp when it exists. t.TempDir()
/// equivalents on Linux CI stay short either way.
fn short_temp_root() -> PathBuf {
    if std::path::Path::new("/tmp").is_dir() {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    }
}

#[test]
fn managed_lifecycle_integration() {
    if std::env::var("KUTTIDB_MANAGED_INTEGRATION").ok().as_deref() != Some("1") {
        return;
    }
    let executable =
        PathBuf::from(std::env::var_os("KUTTIDB_SERVER").expect("KUTTIDB_SERVER required"));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = short_temp_root().join(format!(
        "kuttidb-managed-rust-{}-{nonce}",
        std::process::id()
    ));
    let mut client = Client::connect_managed(ManagedOptions {
        data_dir: data_dir.clone(),
        executable: Some(executable),
        transport: ManagedTransport::Unix,
        idle_timeout: Duration::from_millis(250),
        startup_timeout: Duration::from_secs(5),
        auth_token: None,
    })
    .expect("managed connect");
    client
        .put("managed-rust", b"value", None)
        .expect("managed put");
    assert_eq!(
        client.get("managed-rust").expect("managed get"),
        Some(b"value".to_vec())
    );
    drop(client);
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve TCP port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let tcp_dir = short_temp_root().join(format!(
        "kuttidb-managed-rust-tcp-{}-{nonce}",
        std::process::id()
    ));
    let mut tcp = Client::connect_managed(ManagedOptions {
        data_dir: tcp_dir.clone(),
        executable: Some(PathBuf::from(std::env::var_os("KUTTIDB_SERVER").unwrap())),
        transport: ManagedTransport::Tcp {
            host: Ipv4Addr::LOCALHOST,
            port,
        },
        idle_timeout: Duration::from_millis(250),
        startup_timeout: Duration::from_secs(5),
        auth_token: None,
    })
    .expect("managed TCP connect");
    tcp.put("managed-rust-tcp", b"value", None)
        .expect("managed TCP put");
    assert_eq!(
        tcp.get("managed-rust-tcp").expect("managed TCP get"),
        Some(b"value".to_vec())
    );
    drop(tcp);
    std::thread::sleep(Duration::from_millis(600));
    let _ = std::fs::remove_dir_all(data_dir);
    let _ = std::fs::remove_dir_all(tcp_dir);
}
