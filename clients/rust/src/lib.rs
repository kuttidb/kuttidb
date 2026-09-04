//! Rust client for the KuttiDB binary protocol.
//!
//! Full KuttiDB v1.8 protocol support. [`Client`] owns one connection and is
//! not thread-safe; use [`Pool`] for concurrent access from many threads.
//!
//! ```no_run
//! use kuttidb::Client;
//! let mut c = Client::connect("127.0.0.1:7379")?;
//! c.put("k", b"v", None)?;
//! assert_eq!(c.get("k")?, Some(b"v".to_vec()));
//! # Ok::<(), kuttidb::Error>(())
//! ```

use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod features;
pub use features::*;

pub const BATCH_SIZE: usize = 256;
const MAX_KEY: usize = (1 << 16) - 1;
const MAX_VALUE: u32 = 64 << 20;

const OP_PUT: u8 = 0x01;
const OP_GET: u8 = 0x02;
const OP_DELETE: u8 = 0x03;
const OP_STATS: u8 = 0x04;
const OP_PUT_TTL: u8 = 0x05;
const OP_AUTH: u8 = 0x06;
const OP_SERVER_INFO: u8 = 0x0c;
const OP_PUT_BATCH: u8 = 0x11;
const OP_GET_BATCH: u8 = 0x12;
const OP_PUT_BATCH_TTL: u8 = 0x13;

const ST_OK: u8 = 0x00;
const ST_MISS: u8 = 0x01;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Server,
    KeyTooLarge,
    ValueTooLarge,
    Closed,
    AddressConflict,
    Authentication,
    ResponseTooLarge,
    Managed(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Server => write!(f, "server error"),
            Error::KeyTooLarge => write!(f, "key too large"),
            Error::ValueTooLarge => write!(f, "value too large"),
            Error::Closed => write!(f, "connection closed"),
            Error::AddressConflict => write!(f, "pool exhausted and connect failed"),
            Error::Authentication => write!(f, "authentication failed"),
            Error::ResponseTooLarge => write!(f, "response too large"),
            Error::Managed(message) => write!(f, "managed server: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Key/value with optional TTL for [`Client::put_many_ttl`].
pub struct Item<'a> {
    pub key: &'a str,
    pub value: &'a [u8],
    /// None = no expiry
    pub ttl: Option<Duration>,
}

/// A single-connection client. Not `Sync`; see [`Pool`].
pub struct Client {
    stream: Stream,
}

enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}
impl Read for Stream {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(s) => s.read(b),
            Self::Unix(s) => s.read(b),
            Self::Tls(s) => s.read(b),
        }
    }
}
impl Write for Stream {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(s) => s.write(b),
            Self::Unix(s) => s.write(b),
            Self::Tls(s) => s.write(b),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(s) => s.flush(),
            Self::Unix(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

/// Typed settings for [`Client::connect_managed`].
#[derive(Clone, Debug, Default)]
pub enum ManagedTransport {
    #[default]
    Unix,
    Tcp {
        host: Ipv4Addr,
        port: u16,
    },
}

#[derive(Clone)]
pub struct ManagedOptions {
    pub data_dir: PathBuf,
    pub executable: Option<PathBuf>,
    pub transport: ManagedTransport,
    pub idle_timeout: Duration,
    pub startup_timeout: Duration,
    pub auth_token: Option<Vec<u8>>,
}

fn ttl_ms_u32(ttl: Option<Duration>) -> u32 {
    match ttl {
        None => 0,
        Some(d) => {
            let ms = d.as_millis() as u64;
            if ms == 0 {
                1
            } else {
                ms.min(u32::MAX as u64) as u32
            }
        }
    }
}

fn read_instance_id(data_dir: &Path) -> Result<Option<String>, Error> {
    match std::fs::read_to_string(data_dir.join("instance.id")) {
        Ok(value) => {
            let id = value.trim().to_owned();
            if id.len() == 32
                && id
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                Ok(Some(id))
            } else {
                Err(Error::Managed("invalid instance identity".into()))
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Error::Io(error)),
    }
}

impl Client {
    pub fn connect(addr: &str) -> Result<Client, Error> {
        Self::connect_inner(addr, None)
    }

    /// Connect and authenticate with a 1..1024 byte pre-shared token.
    pub fn connect_authenticated(addr: &str, token: &[u8]) -> Result<Client, Error> {
        if token.is_empty() || token.len() > 1024 {
            return Err(Error::Authentication);
        }
        Self::connect_inner(addr, Some(token))
    }

    /// Connect with TLS using the platform-independent Mozilla root store and
    /// verify the certificate against `server_name`.
    pub fn connect_tls(addr: &str, server_name: &str) -> Result<Client, Error> {
        Self::connect_tls_inner(addr, server_name, None, None)
    }

    /// Connect with verified TLS and authenticate with a pre-shared token.
    pub fn connect_tls_authenticated(
        addr: &str,
        server_name: &str,
        token: &[u8],
    ) -> Result<Client, Error> {
        if token.is_empty() || token.len() > 1024 {
            return Err(Error::Authentication);
        }
        Self::connect_tls_inner(addr, server_name, None, Some(token))
    }

    /// Connect with a caller-supplied rustls configuration, for example to
    /// trust a private CA or present a client certificate.
    pub fn connect_tls_with_config(
        addr: &str,
        server_name: &str,
        config: Arc<rustls::ClientConfig>,
        token: Option<&[u8]>,
    ) -> Result<Client, Error> {
        Self::connect_tls_inner(addr, server_name, Some(config), token)
    }

    fn connect_tls_inner(
        addr: &str,
        server_name: &str,
        config: Option<Arc<rustls::ClientConfig>>,
        token: Option<&[u8]>,
    ) -> Result<Client, Error> {
        let config = config.unwrap_or_else(|| {
            let roots =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        });
        let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|_| Error::Managed("invalid TLS server name".into()))?;
        let tcp = TcpStream::connect(addr)?;
        tcp.set_nodelay(true)?;
        let connection = rustls::ClientConnection::new(config, name)
            .map_err(|_| Error::Managed("invalid TLS configuration".into()))?;
        let mut client = Client {
            stream: Stream::Tls(Box::new(rustls::StreamOwned::new(connection, tcp))),
        };
        if let Some(token) = token {
            client.authenticate(token)?;
        }
        Ok(client)
    }

    fn connect_inner(addr: &str, token: Option<&[u8]>) -> Result<Client, Error> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let mut client = Client {
            stream: Stream::Tcp(stream),
        };
        if let Some(token) = token {
            let req = Self::single_header(OP_AUTH, token, 0);
            client.send_all(&req)?;
            let (status, _) = client.read_head()?;
            if status != ST_OK {
                return Err(Error::Authentication);
            }
        }
        Ok(client)
    }

    /// Ensure, connect to, and identity-verify a local managed instance.
    pub fn connect_managed(options: ManagedOptions) -> Result<Client, Error> {
        let supplied = if options.data_dir.is_absolute() {
            options.data_dir.clone()
        } else {
            std::env::current_dir()
                .map_err(Error::Io)?
                .join(&options.data_dir)
        };
        let data_dir = std::fs::canonicalize(&supplied).unwrap_or(supplied);
        if !data_dir.is_absolute() {
            return Err(Error::Managed("data_dir must be absolute".into()));
        }
        let (endpoint, transport) = match &options.transport {
            ManagedTransport::Unix => (
                format!("unix:{}", data_dir.join("kuttidb.sock").display()),
                false,
            ),
            ManagedTransport::Tcp { host, port } => {
                if !host.is_loopback() || *port == 0 {
                    return Err(Error::Managed(
                        "managed TCP requires a literal IPv4 loopback endpoint".into(),
                    ));
                }
                (format!("tcp:{host}:{port}"), true)
            }
        };
        let socket_path = data_dir.join("kuttidb.sock");
        let mut expected = read_instance_id(&data_dir)?;
        let needs_start = if transport {
            let address = endpoint.strip_prefix("tcp:").unwrap();
            match TcpStream::connect(address) {
                Ok(stream) => {
                    drop(stream);
                    false
                }
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => true,
                Err(error) => return Err(Error::Io(error)),
            }
        } else {
            match UnixStream::connect(&socket_path) {
                Ok(stream) => {
                    drop(stream);
                    false
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    true
                }
                Err(error) => return Err(Error::Io(error)),
            }
        };
        if needs_start {
            let executable = options.executable.unwrap_or_else(|| {
                std::env::var_os("KUTTIDB_SERVER")
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("kuttidb"))
            });
            let output = Command::new(executable)
                .args([
                    "ensure",
                    "--data-dir",
                    data_dir
                        .to_str()
                        .ok_or_else(|| Error::Managed("non-utf8 data_dir".into()))?,
                    "--listen",
                    &endpoint,
                    "--idle-timeout-ms",
                    &options.idle_timeout.as_millis().max(1).to_string(),
                    "--startup-timeout-ms",
                    &options.startup_timeout.as_millis().max(1).to_string(),
                    "--json",
                ])
                .output()
                .map_err(Error::Io)?;
            if !output.status.success() {
                return Err(Error::Managed("startup failed".into()));
            }
            let text = String::from_utf8_lossy(&output.stdout);
            expected = text
                .split("\"instance_id\":\"")
                .nth(1)
                .and_then(|v| v.get(..32))
                .map(str::to_owned);
        }
        let expected =
            expected.ok_or_else(|| Error::Managed("instance identity missing".into()))?;
        let stream = if transport {
            Stream::Tcp(TcpStream::connect(endpoint.strip_prefix("tcp:").unwrap())?)
        } else {
            Stream::Unix(UnixStream::connect(&socket_path)?)
        };
        let mut client = Client { stream };
        if let Some(token) = options.auth_token.as_deref() {
            client.authenticate(token)?;
        }
        client.verify_managed(&expected)?;
        Ok(client)
    }

    fn authenticate(&mut self, token: &[u8]) -> Result<(), Error> {
        if token.is_empty() || token.len() > 1024 {
            return Err(Error::Authentication);
        }
        let req = Self::single_header(OP_AUTH, token, 0);
        self.send_all(&req)?;
        if self.read_head()?.0 == ST_OK {
            Ok(())
        } else {
            Err(Error::Authentication)
        }
    }
    fn verify_managed(&mut self, expected: &str) -> Result<(), Error> {
        self.send_all(&[OP_SERVER_INFO, 0, 0, 0, 0, 0, 0])?;
        let (status, len) = self.read_head()?;
        if status != ST_OK || len != 52 {
            return Err(Error::Managed("server identity unavailable".into()));
        }
        let payload = self.read_full(52)?;
        if payload.get(0..2) != Some(&[1, 32])
            || std::str::from_utf8(&payload[2..34]).ok() != Some(expected)
        {
            return Err(Error::Managed(
                "endpoint belongs to another instance".into(),
            ));
        }
        Ok(())
    }

    fn send_all(&mut self, mut data: &[u8]) -> Result<(), Error> {
        while !data.is_empty() {
            let n = self.stream.write(data)?;
            if n == 0 {
                return Err(Error::Closed);
            }
            data = &data[n..];
        }
        Ok(())
    }

    fn read_full(&mut self, n: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; n];
        self.stream.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Response head: [status:1][vlen:4]
    fn read_head(&mut self) -> Result<(u8, u32), Error> {
        let head = self.read_full(5)?;
        let vlen = u32::from_le_bytes([head[1], head[2], head[3], head[4]]);
        if vlen > MAX_VALUE {
            return Err(Error::ResponseTooLarge);
        }
        Ok((head[0], vlen))
    }

    fn single_header(op: u8, key: &[u8], vlen: u32) -> Vec<u8> {
        let mut req = Vec::with_capacity(7 + key.len() + vlen as usize);
        req.push(op);
        req.extend_from_slice(&(key.len() as u16).to_le_bytes());
        req.extend_from_slice(&vlen.to_le_bytes());
        req.extend_from_slice(key);
        req
    }

    fn request(&mut self, op: u8, key: &[u8], value: &[u8]) -> Result<(u8, Vec<u8>), Error> {
        if key.len() > MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        if value.len() > MAX_VALUE as usize {
            return Err(Error::ValueTooLarge);
        }
        let mut req = Self::single_header(op, key, value.len() as u32);
        req.extend_from_slice(value);
        self.send_all(&req)?;
        let (status, len) = self.read_head()?;
        Ok((status, self.read_full(len as usize)?))
    }

    fn request_meta(
        &mut self,
        op: u8,
        key: &[u8],
        meta: &[u8],
        value: &[u8],
    ) -> Result<(u8, Vec<u8>), Error> {
        if key.len() > MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        if value.len() > MAX_VALUE as usize {
            return Err(Error::ValueTooLarge);
        }
        let mut req = Vec::with_capacity(7 + meta.len() + key.len() + value.len());
        req.push(op);
        req.extend_from_slice(&(key.len() as u16).to_le_bytes());
        req.extend_from_slice(&(value.len() as u32).to_le_bytes());
        req.extend_from_slice(meta);
        req.extend_from_slice(key);
        req.extend_from_slice(value);
        self.send_all(&req)?;
        let (status, len) = self.read_head()?;
        Ok((status, self.read_full(len as usize)?))
    }

    pub fn put(&mut self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), Error> {
        let kb = key.as_bytes();
        if kb.len() > MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        if value.len() > MAX_VALUE as usize {
            return Err(Error::ValueTooLarge);
        }
        let req = match ttl {
            None => {
                let mut req = Self::single_header(OP_PUT, kb, value.len() as u32);
                req.extend_from_slice(value);
                req
            }
            Some(_) => {
                // wire layout: [op:1][klen:2][vlen:4][ttl_ms:4][key][value]
                let mut req = Vec::with_capacity(11 + kb.len() + value.len());
                req.push(OP_PUT_TTL);
                req.extend_from_slice(&(kb.len() as u16).to_le_bytes());
                req.extend_from_slice(&(value.len() as u32).to_le_bytes());
                req.extend_from_slice(&ttl_ms_u32(ttl).to_le_bytes());
                req.extend_from_slice(kb);
                req.extend_from_slice(value);
                req
            }
        };
        self.send_all(&req)?;
        let (status, _) = self.read_head()?;
        match status {
            ST_OK => Ok(()),
            _ => Err(Error::Server),
        }
    }

    /// Returns `Ok(None)` on miss.
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let kb = key.as_bytes();
        if kb.len() > MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        let req = Self::single_header(OP_GET, kb, 0);
        self.send_all(&req)?;
        let (status, vlen) = self.read_head()?;
        match status {
            ST_OK => {
                let val = if vlen > 0 {
                    self.read_full(vlen as usize)?
                } else {
                    Vec::new()
                };
                Ok(Some(val))
            }
            ST_MISS => Ok(None),
            _ => Err(Error::Server),
        }
    }

    /// Returns `Ok(true)` if the key existed.
    pub fn delete(&mut self, key: &str) -> Result<bool, Error> {
        let kb = key.as_bytes();
        if kb.len() > MAX_KEY {
            return Err(Error::KeyTooLarge);
        }
        let req = Self::single_header(OP_DELETE, kb, 0);
        self.send_all(&req)?;
        let (status, _) = self.read_head()?;
        match status {
            ST_OK => Ok(true),
            ST_MISS => Ok(false),
            _ => Err(Error::Server),
        }
    }

    pub fn stats(&mut self) -> Result<String, Error> {
        let req = Self::single_header(OP_STATS, b"", 0);
        self.send_all(&req)?;
        let (status, vlen) = self.read_head()?;
        if status != ST_OK {
            return Err(Error::Server);
        }
        let val = self.read_full(vlen as usize)?;
        String::from_utf8(val).map_err(|_| Error::Server)
    }

    /// Batched put without TTL: one round trip per [`BATCH_SIZE`] pairs.
    pub fn put_many(&mut self, items: &[(&str, &[u8])]) -> Result<(), Error> {
        self.put_many_generic(
            items
                .iter()
                .map(|(k, v)| (*k, *v, None))
                .collect::<Vec<_>>()
                .as_slice(),
            false,
        )
    }

    /// Batched put with per-item TTL: one round trip per [`BATCH_SIZE`].
    pub fn put_many_ttl(&mut self, items: &[(&str, &[u8], Option<Duration>)]) -> Result<(), Error> {
        self.put_many_generic(items, true)
    }

    fn put_many_generic(
        &mut self,
        items: &[(&str, &[u8], Option<Duration>)],
        any_ttl: bool,
    ) -> Result<(), Error> {
        let op = if any_ttl {
            OP_PUT_BATCH_TTL
        } else {
            OP_PUT_BATCH
        };
        for chunk in items.chunks(BATCH_SIZE) {
            let mut req = Vec::with_capacity(7 + chunk.len() * 64);
            req.push(op);
            req.extend_from_slice(&[0, 0]);
            req.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            for (k, v, ttl) in chunk {
                let kb = k.as_bytes();
                if kb.len() > MAX_KEY {
                    return Err(Error::KeyTooLarge);
                }
                if v.len() > MAX_VALUE as usize {
                    return Err(Error::ValueTooLarge);
                }
                let item_len = 6 + if any_ttl { 4 } else { 0 } + kb.len() + v.len();
                if req.len().saturating_add(item_len) > MAX_VALUE as usize {
                    return Err(Error::ValueTooLarge);
                }
                req.extend_from_slice(&(kb.len() as u16).to_le_bytes());
                req.extend_from_slice(&(v.len() as u32).to_le_bytes());
                if any_ttl {
                    req.extend_from_slice(&ttl_ms_u32(*ttl).to_le_bytes());
                }
                req.extend_from_slice(kb);
                req.extend_from_slice(v);
            }
            self.send_all(&req)?;
            let resp = self.read_full(1)?;
            if resp[0] != ST_OK {
                return Err(Error::Server);
            }
        }
        Ok(())
    }

    /// Batched get: one round trip per [`BATCH_SIZE`] keys; misses are `None`.
    pub fn get_many(&mut self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let mut result = vec![None; keys.len()];
        for (start, chunk) in keys.chunks(BATCH_SIZE).enumerate() {
            let mut req = Vec::with_capacity(7 + chunk.len() * 32);
            req.push(OP_GET_BATCH);
            req.extend_from_slice(&[0, 0]);
            req.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            for k in chunk {
                let kb = k.as_bytes();
                if kb.len() > MAX_KEY {
                    return Err(Error::KeyTooLarge);
                }
                req.extend_from_slice(&(kb.len() as u16).to_le_bytes());
                req.extend_from_slice(kb);
            }
            self.send_all(&req)?;
            let count = {
                let raw = self.read_full(4)?;
                u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]])
            };
            for i in 0..count as usize {
                let (status, vlen) = self.read_head()?;
                let val = match status {
                    ST_OK => {
                        let val = if vlen > 0 {
                            self.read_full(vlen as usize)?
                        } else {
                            Vec::new()
                        };
                        Some(val)
                    }
                    ST_MISS => None,
                    _ => return Err(Error::Server),
                };
                result[start * BATCH_SIZE + i] = val;
            }
        }
        Ok(result)
    }
}

/// A fixed pool of connections for concurrent use from many threads.
pub struct Pool {
    addr: String,
    auth_token: Option<Vec<u8>>,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    tls_server_name: Option<String>,
    managed_dir: Option<PathBuf>,
    managed_transport: Option<ManagedTransport>,
    idle: Mutex<Vec<Client>>,
    max: usize,
}

impl Pool {
    pub fn new(addr: &str, size: usize) -> Result<Pool, Error> {
        Self::new_inner(addr, size, None)
    }

    pub fn new_authenticated(addr: &str, size: usize, token: &[u8]) -> Result<Pool, Error> {
        if token.is_empty() || token.len() > 1024 {
            return Err(Error::Authentication);
        }
        Self::new_inner(addr, size, Some(token.to_vec()))
    }

    /// Create a pool of certificate- and hostname-verified TLS connections.
    pub fn new_tls(addr: &str, server_name: &str, size: usize) -> Result<Pool, Error> {
        Self::new_tls_inner(addr, server_name, size, None)
    }

    /// Create an authenticated pool of verified TLS connections.
    pub fn new_tls_authenticated(
        addr: &str,
        server_name: &str,
        size: usize,
        token: &[u8],
    ) -> Result<Pool, Error> {
        if token.is_empty() || token.len() > 1024 {
            return Err(Error::Authentication);
        }
        Self::new_tls_inner(addr, server_name, size, Some(token.to_vec()))
    }

    fn new_tls_inner(
        addr: &str,
        server_name: &str,
        size: usize,
        auth_token: Option<Vec<u8>>,
    ) -> Result<Pool, Error> {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let count = size.max(1);
        let mut idle = Vec::with_capacity(count);
        for _ in 0..count {
            idle.push(Client::connect_tls_with_config(
                addr,
                server_name,
                config.clone(),
                auth_token.as_deref(),
            )?)
        }
        Ok(Pool {
            addr: addr.to_owned(),
            auth_token,
            tls_config: Some(config),
            tls_server_name: Some(server_name.to_owned()),
            managed_dir: None,
            managed_transport: None,
            idle: Mutex::new(idle),
            max: count,
        })
    }

    fn new_inner(addr: &str, size: usize, auth_token: Option<Vec<u8>>) -> Result<Pool, Error> {
        let mut idle = Vec::with_capacity(size);
        for _ in 0..size {
            idle.push(match &auth_token {
                Some(token) => Client::connect_authenticated(addr, token)?,
                None => Client::connect(addr)?,
            });
        }
        Ok(Pool {
            addr: addr.to_string(),
            auth_token,
            tls_config: None,
            tls_server_name: None,
            managed_dir: None,
            managed_transport: None,
            idle: Mutex::new(idle),
            max: size.max(1),
        })
    }

    /// Build a pool backed by a managed local instance. The filled pool holds
    /// leases for its complete lifetime, so closing it starts the idle grace.
    pub fn connect_managed(options: ManagedOptions, size: usize) -> Result<Pool, Error> {
        let count = size.max(1);
        let data_dir = if options.data_dir.is_absolute() {
            options.data_dir.clone()
        } else {
            std::env::current_dir()
                .map_err(Error::Io)?
                .join(&options.data_dir)
        };
        let auth_token = options.auth_token.clone();
        let mut idle = Vec::with_capacity(count);
        for _ in 0..count {
            idle.push(Client::connect_managed(ManagedOptions {
                data_dir: data_dir.clone(),
                executable: options.executable.clone(),
                transport: options.transport.clone(),
                idle_timeout: options.idle_timeout,
                startup_timeout: options.startup_timeout,
                auth_token: auth_token.clone(),
            })?);
        }
        Ok(Pool {
            addr: data_dir.join("kuttidb.sock").display().to_string(),
            auth_token,
            tls_config: None,
            tls_server_name: None,
            managed_dir: Some(data_dir),
            managed_transport: Some(options.transport.clone()),
            idle: Mutex::new(idle),
            max: count,
        })
    }

    /// Run `f` with a checked-out connection; the connection returns to the
    /// pool afterwards (even on error).
    pub fn with<T>(&self, f: impl FnOnce(&mut Client) -> Result<T, Error>) -> Result<T, Error> {
        let mut client = self.checkout()?;
        let out = f(&mut client);
        self.checkin(client);
        out
    }

    fn checkout(&self) -> Result<Client, Error> {
        let mut idle = self.idle.lock().unwrap();
        match idle.pop() {
            Some(c) => Ok(c),
            None if self.tls_config.is_some() => Client::connect_tls_with_config(
                &self.addr,
                self.tls_server_name
                    .as_deref()
                    .ok_or(Error::AddressConflict)?,
                self.tls_config.as_ref().unwrap().clone(),
                self.auth_token.as_deref(),
            ),
            None => match &self.auth_token {
                Some(token) => match &self.managed_dir {
                    Some(dir) => Client::connect_managed(ManagedOptions {
                        data_dir: dir.clone(),
                        executable: None,
                        transport: self.managed_transport.clone().unwrap_or_default(),
                        idle_timeout: Duration::from_secs(60),
                        startup_timeout: Duration::from_secs(10),
                        auth_token: Some(token.clone()),
                    }),
                    None => Client::connect_authenticated(&self.addr, token),
                },
                None => match &self.managed_dir {
                    Some(dir) => Client::connect_managed(ManagedOptions {
                        data_dir: dir.clone(),
                        executable: None,
                        transport: self.managed_transport.clone().unwrap_or_default(),
                        idle_timeout: Duration::from_secs(60),
                        startup_timeout: Duration::from_secs(10),
                        auth_token: None,
                    }),
                    None => Client::connect(&self.addr),
                },
            },
        }
    }

    fn checkin(&self, client: Client) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.max {
            idle.push(client);
        }
    }
}
