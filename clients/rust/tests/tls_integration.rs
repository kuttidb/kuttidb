use kuttidb::Client;
use rustls::pki_types::CertificateDer;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn verified_tls_integration() {
    if std::env::var("KUTTIDB_TLS_INTEGRATION").ok().as_deref() != Some("1") {
        return;
    }
    let server = PathBuf::from(std::env::var_os("KUTTIDB_SERVER").expect("KUTTIDB_SERVER"));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kuttidb-rust-tls-{}-{nonce}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert = dir.join("cert.pem");
    let der = dir.join("cert.der");
    let key = dir.join("key.pem");
    let token = dir.join("token");
    std::fs::write(&token, b"rust-tls-token\n").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            cert.to_str().unwrap()
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success());
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(Command::new("openssl")
        .args([
            "x509",
            "-in",
            cert.to_str().unwrap(),
            "-outform",
            "DER",
            "-out",
            der.to_str().unwrap()
        ])
        .status()
        .unwrap()
        .success());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut child = Command::new(server)
        .args([
            port.to_string(),
            "-".into(),
            "50".into(),
            "--bind".into(),
            "127.0.0.1".into(),
            "--auth-file".into(),
            token.display().to_string(),
            "--tls-cert".into(),
            cert.display().to_string(),
            "--tls-key".into(),
            key.display().to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(std::fs::read(&der).unwrap()))
        .unwrap();
    let config = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let addr = format!("127.0.0.1:{port}");
    let mut connected = None;
    let mut last_error = None;
    for _ in 0..100 {
        match Client::connect_tls_with_config(
            &addr,
            "localhost",
            config.clone(),
            Some(b"rust-tls-token"),
        ) {
            Ok(c) => {
                connected = Some(c);
                break;
            }
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(30));
            }
        }
    }
    let mut client = connected.unwrap_or_else(|| panic!("verified TLS connection: {last_error:?}"));
    assert!(client.health().unwrap());
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(dir);
}
