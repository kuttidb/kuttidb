use super::{Client, Error, ST_MISS, ST_OK};
use std::time::Duration;

const OP_HEALTH: u8 = 0x09;
const OP_CAPABILITIES: u8 = 0x0a;
const OP_PUT_SWR: u8 = 0x0b;
const Q_DECLARE: u8 = 0x20;
const Q_PUBLISH: u8 = 0x21;
const Q_CONSUME: u8 = 0x22;
const Q_ACK: u8 = 0x23;
const Q_NACK: u8 = 0x24;
const Q_PUBLISH_TTL: u8 = 0x25;
const Q_STATS: u8 = 0x26;
const Q_PREFETCH: u8 = 0x27;
const Q_CANCEL: u8 = 0x28;
const Q_REGISTER: u8 = 0x29;
const Q_UNREGISTER: u8 = 0x2a;
const Q_CONSUME_AS: u8 = 0x2b;
const Q_LIST: u8 = 0x2c;
const Q_PUBLISH_BATCH: u8 = 0x2d;
const Q_CONSUME_BATCH: u8 = 0x2e;
const Q_ACK_BATCH: u8 = 0x2f;
const X_DECLARE: u8 = 0x30;
const X_BIND: u8 = 0x31;
const X_UNBIND: u8 = 0x32;
const X_PUBLISH: u8 = 0x33;
const A_PUT_PUBLISH: u8 = 0x40;
const A_PUT_ENQUEUE: u8 = 0x41;
const A_DELETE_PUBLISH: u8 = 0x42;
const A_UPDATE_EMIT: u8 = 0x43;
const SF_CLAIM: u8 = 0x50;
const SF_WAIT: u8 = 0x51;
const SF_PUT_RELEASE: u8 = 0x52;
const SF_RELEASE: u8 = 0x53;
const SF_REFRESH: u8 = 0x54;
const S_DECLARE: u8 = 0x60;
const S_APPEND: u8 = 0x61;
const S_FETCH: u8 = 0x62;
const S_COMMIT: u8 = 0x63;
const S_OFFSET: u8 = 0x64;
const S_JOIN: u8 = 0x65;
const S_LAG: u8 = 0x66;
const S_APPEND_BATCH: u8 = 0x67;
const S_LEAVE: u8 = 0x68;
const S_LIST: u8 = 0x69;
const S_GROUP_LIST: u8 = 0x6a;
const S_COMMIT_BATCH: u8 = 0x6b;
const S_FETCH_KEYS: u8 = 0x6c;

pub const FEATURE_CACHE: u64 = 1 << 0;
pub const FEATURE_QUEUES: u64 = 1 << 1;
pub const FEATURE_EXCHANGES: u64 = 1 << 2;
pub const FEATURE_ATOMIC: u64 = 1 << 3;
pub const FEATURE_SINGLEFLIGHT: u64 = 1 << 4;
pub const FEATURE_STREAMS: u64 = 1 << 5;
pub const FEATURE_STREAM_BATCH: u64 = 1 << 6;
pub const FEATURE_HEALTH: u64 = 1 << 7;
pub const FEATURE_STREAM_GENERATIONS: u64 = 1 << 8;
pub const FEATURE_QUEUE_CONSUMERS: u64 = 1 << 9;
pub const FEATURE_ATOMIC_UPDATE: u64 = 1 << 10;
pub const FEATURE_SWR: u64 = 1 << 11;
pub const FEATURE_QUEUE_BATCH: u64 = 1 << 12;
pub const FEATURE_STREAM_COMMIT_BATCH: u64 = 1 << 13;
pub const FEATURE_STREAM_KEYS: u64 = 1 << 14;
pub const FEATURE_SERVER_INFO: u64 = 1 << 15;

fn ok(status: u8) -> Result<(), Error> {
    if status == ST_OK {
        Ok(())
    } else {
        Err(Error::Server)
    }
}
fn ms(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}
fn u16v(v: usize) -> [u8; 2] {
    (v as u16).to_le_bytes()
}
fn u32v(v: usize) -> [u8; 4] {
    (v as u32).to_le_bytes()
}

struct Dec<'a> {
    b: &'a [u8],
    i: usize,
}
impl<'a> Dec<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.i + n > self.b.len() {
            return Err(Error::Server);
        }
        let x = &self.b[self.i..self.i + n];
        self.i += n;
        Ok(x)
    }
    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn done(&self) -> Result<(), Error> {
        if self.i == self.b.len() {
            Ok(())
        } else {
            Err(Error::Server)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
    pub major: u16,
    pub minor: u16,
    pub features: u64,
}
#[derive(Clone, Debug)]
pub struct QueueOptions<'a> {
    pub durable: bool,
    pub max_depth: u64,
    pub dead_letter_queue: Option<&'a str>,
    pub max_deliveries: u32,
}
impl Default for QueueOptions<'_> {
    fn default() -> Self {
        Self {
            durable: true,
            max_depth: 0,
            dead_letter_queue: None,
            max_deliveries: 0,
        }
    }
}
#[derive(Clone, Debug)]
pub struct QueueInfo {
    pub name: String,
    pub depth: u64,
    pub inflight: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct QueueStats {
    pub depth: u64,
    pub inflight: u64,
}
#[derive(Clone, Debug)]
pub struct Delivery {
    pub delivery_tag: u64,
    pub message_id: u64,
    pub delivery_count: u32,
    pub redelivered: bool,
    pub value: Vec<u8>,
}

impl Client {
    pub fn capabilities(&mut self) -> Result<Capabilities, Error> {
        let mut p = 1u16.to_le_bytes().to_vec();
        p.extend_from_slice(&8u16.to_le_bytes());
        let (s, v) = self.request(OP_CAPABILITIES, b"", &p)?;
        if s == ST_MISS {
            return Err(Error::Server);
        };
        ok(s)?;
        if v.len() != 12 {
            return Err(Error::Server);
        }
        Ok(Capabilities {
            major: u16::from_le_bytes(v[0..2].try_into().unwrap()),
            minor: u16::from_le_bytes(v[2..4].try_into().unwrap()),
            features: u64::from_le_bytes(v[4..12].try_into().unwrap()),
        })
    }
    fn require(&mut self, f: u64) -> Result<(), Error> {
        if self.capabilities()?.features & f != 0 {
            Ok(())
        } else {
            Err(Error::Server)
        }
    }
    pub fn health(&mut self) -> Result<bool, Error> {
        let (s, _) = self.request(OP_HEALTH, b"", b"")?;
        Ok(s == ST_OK)
    }
    pub fn queue_declare(&mut self, name: &str, o: QueueOptions<'_>) -> Result<(), Error> {
        if name.is_empty() || name.len() > 255 {
            return Err(Error::KeyTooLarge);
        }
        let mut p = vec![o.durable as u8];
        p.extend_from_slice(&o.max_depth.to_le_bytes());
        if let Some(dlq) = o.dead_letter_queue {
            if dlq.is_empty() || dlq.len() > 255 {
                return Err(Error::KeyTooLarge);
            }
            let mut ext = u16v(dlq.len()).to_vec();
            ext.extend_from_slice(dlq.as_bytes());
            ext.extend_from_slice(&o.max_deliveries.to_le_bytes());
            p.extend_from_slice(&u16v(ext.len()));
            p.extend_from_slice(&ext)
        }
        let (s, _) = self.request(Q_DECLARE, name.as_bytes(), &p)?;
        ok(s)
    }
    pub fn queue_list(&mut self) -> Result<Vec<QueueInfo>, Error> {
        let (s, v) = self.request(Q_LIST, b"", b"")?;
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u16()?;
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let l = d.u16()? as usize;
            let name = String::from_utf8(d.take(l)?.to_vec()).map_err(|_| Error::Server)?;
            out.push(QueueInfo {
                name,
                depth: d.u64()?,
                inflight: d.u64()?,
            })
        }
        d.done()?;
        Ok(out)
    }
    pub fn queue_publish(
        &mut self,
        name: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<u64, Error> {
        let (op, p) = if let Some(t) = ttl {
            let mut p = ms(t).to_le_bytes().to_vec();
            p.extend_from_slice(value);
            (Q_PUBLISH_TTL, p)
        } else {
            (Q_PUBLISH, value.to_vec())
        };
        let (s, v) = self.request(op, name.as_bytes(), &p)?;
        ok(s)?;
        if v.len() != 8 {
            return Err(Error::Server);
        }
        Ok(u64::from_le_bytes(v.try_into().unwrap()))
    }
    pub fn queue_consume(
        &mut self,
        name: &str,
        visibility: Duration,
    ) -> Result<Option<Delivery>, Error> {
        self.queue_consume_raw(Q_CONSUME, name, None, visibility)
    }
    pub fn queue_consume_as(
        &mut self,
        name: &str,
        consumer: &str,
        visibility: Duration,
    ) -> Result<Option<Delivery>, Error> {
        self.queue_consume_raw(Q_CONSUME_AS, name, Some(consumer), visibility)
    }
    fn queue_consume_raw(
        &mut self,
        op: u8,
        name: &str,
        consumer: Option<&str>,
        visibility: Duration,
    ) -> Result<Option<Delivery>, Error> {
        let mut p = Vec::new();
        if let Some(c) = consumer {
            if c.is_empty() || c.len() > 255 {
                return Err(Error::KeyTooLarge);
            }
            p.extend_from_slice(&u16v(c.len()));
            p.extend_from_slice(c.as_bytes())
        }
        p.extend_from_slice(&ms(visibility).to_le_bytes());
        let (s, v) = self.request(op, name.as_bytes(), &p)?;
        if s == ST_MISS {
            return Ok(None);
        }
        ok(s)?;
        Ok(Some(decode_delivery(&v)?))
    }
    pub fn queue_ack(&mut self, name: &str, tag: u64) -> Result<bool, Error> {
        let (s, _) = self.request(Q_ACK, name.as_bytes(), &tag.to_le_bytes())?;
        if s == 2 {
            Err(Error::Server)
        } else {
            Ok(s == ST_OK)
        }
    }
    pub fn queue_nack(
        &mut self,
        name: &str,
        tag: u64,
        requeue: bool,
        delay: Duration,
    ) -> Result<bool, Error> {
        let mut p = tag.to_le_bytes().to_vec();
        p.push(requeue as u8);
        if !delay.is_zero() {
            p.extend_from_slice(&ms(delay).to_le_bytes())
        }
        let (s, _) = self.request(Q_NACK, name.as_bytes(), &p)?;
        if s == 2 {
            Err(Error::Server)
        } else {
            Ok(s == ST_OK)
        }
    }
    pub fn queue_stats(&mut self, name: &str) -> Result<Option<QueueStats>, Error> {
        let (s, v) = self.request(Q_STATS, name.as_bytes(), b"")?;
        if s == ST_MISS {
            return Ok(None);
        }
        ok(s)?;
        if v.len() != 16 {
            return Err(Error::Server);
        }
        Ok(Some(QueueStats {
            depth: u64::from_le_bytes(v[0..8].try_into().unwrap()),
            inflight: u64::from_le_bytes(v[8..].try_into().unwrap()),
        }))
    }
    pub fn queue_prefetch(&mut self, n: u32) -> Result<(), Error> {
        let (s, _) = self.request(Q_PREFETCH, b"_", &n.to_le_bytes())?;
        ok(s)
    }
    pub fn queue_cancel(&mut self) -> Result<(), Error> {
        let (s, _) = self.request(Q_CANCEL, b"_", b"")?;
        ok(s)
    }
    pub fn queue_consumer_register(&mut self, name: &str) -> Result<u64, Error> {
        let (s, v) = self.request(Q_REGISTER, name.as_bytes(), b"")?;
        ok(s)?;
        if v.len() != 8 {
            return Err(Error::Server);
        }
        Ok(u64::from_le_bytes(v.try_into().unwrap()))
    }
    pub fn queue_consumer_unregister(&mut self, name: &str) -> Result<(), Error> {
        let (s, _) = self.request(Q_UNREGISTER, name.as_bytes(), b"")?;
        ok(s)
    }
    pub fn queue_publish_batch(&mut self, name: &str, values: &[&[u8]]) -> Result<Vec<u64>, Error> {
        if values.is_empty() || values.len() > 256 {
            return Err(Error::ValueTooLarge);
        }
        self.require(FEATURE_QUEUE_BATCH)?;
        let mut p = u32v(values.len()).to_vec();
        for v in values {
            p.extend_from_slice(&u32v(v.len()));
            p.extend_from_slice(v)
        }
        let (s, v) = self.request(Q_PUBLISH_BATCH, name.as_bytes(), &p)?;
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u32()? as usize;
        if n != values.len() {
            return Err(Error::Server);
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(d.u64()?)
        }
        d.done()?;
        Ok(out)
    }
    pub fn queue_consume_batch(&mut self, name: &str, max: u32) -> Result<Vec<Delivery>, Error> {
        if max == 0 || max > 256 {
            return Err(Error::ValueTooLarge);
        }
        self.require(FEATURE_QUEUE_BATCH)?;
        let (s, v) = self.request(Q_CONSUME_BATCH, name.as_bytes(), &max.to_le_bytes())?;
        if s == ST_MISS {
            return Ok(vec![]);
        }
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u32()?;
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let tag = d.u64()?;
            let id = d.u64()?;
            let count = d.u32()?;
            let red = d.take(1)?[0] != 0;
            let l = d.u32()? as usize;
            out.push(Delivery {
                delivery_tag: tag,
                message_id: id,
                delivery_count: count,
                redelivered: red,
                value: d.take(l)?.to_vec(),
            })
        }
        d.done()?;
        Ok(out)
    }
    fn queue_disposition_batch(
        &mut self,
        name: &str,
        tags: &[u64],
        mode: u8,
    ) -> Result<u32, Error> {
        if tags.is_empty() || tags.len() > 256 {
            return Err(Error::ValueTooLarge);
        }
        self.require(FEATURE_QUEUE_BATCH)?;
        let mut p = vec![mode];
        p.extend_from_slice(&u32v(tags.len()));
        for t in tags {
            p.extend_from_slice(&t.to_le_bytes())
        }
        let (s, v) = self.request(Q_ACK_BATCH, name.as_bytes(), &p)?;
        ok(s)?;
        if v.len() != 4 {
            return Err(Error::Server);
        }
        Ok(u32::from_le_bytes(v.try_into().unwrap()))
    }
    pub fn queue_ack_batch(&mut self, name: &str, tags: &[u64]) -> Result<u32, Error> {
        self.queue_disposition_batch(name, tags, 0)
    }
    pub fn queue_nack_batch(
        &mut self,
        name: &str,
        tags: &[u64],
        requeue: bool,
    ) -> Result<u32, Error> {
        self.queue_disposition_batch(name, tags, if requeue { 1 } else { 2 })
    }
}
fn decode_delivery(v: &[u8]) -> Result<Delivery, Error> {
    let mut d = Dec { b: v, i: 0 };
    let delivery_tag = d.u64()?;
    let message_id = d.u64()?;
    let redelivered = d.take(1)?[0] != 0;
    let delivery_count = d.u32()?;
    let value = d.take(v.len() - d.i)?.to_vec();
    Ok(Delivery {
        delivery_tag,
        message_id,
        delivery_count,
        redelivered,
        value,
    })
}

#[derive(Clone, Copy, Debug)]
pub enum ExchangeType {
    Direct = 0,
    Fanout = 1,
    Topic = 2,
}
#[derive(Clone, Debug)]
pub struct ExchangeOptions<'a> {
    pub exchange_type: ExchangeType,
    pub durable: bool,
    pub alternate_exchange: Option<&'a str>,
}
impl Default for ExchangeOptions<'_> {
    fn default() -> Self {
        Self {
            exchange_type: ExchangeType::Direct,
            durable: true,
            alternate_exchange: None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct AtomicResult {
    pub transaction_id: u64,
    pub routed: u32,
    pub unroutable: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SingleFlightState {
    Value,
    Claimed,
    Wait,
    Negative,
    Released,
    Timeout,
    Lost,
    Stale,
    Refresh,
}
#[derive(Clone, Debug)]
pub struct SingleFlightResult {
    pub state: SingleFlightState,
    pub holder: bool,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, Copy, Debug)]
pub struct SwrLoadOptions {
    pub ttl: Duration,
    pub stale_for: Duration,
    pub refresh_after: Option<Duration>,
    pub lease: Duration,
    pub wait: Duration,
}
impl Default for SwrLoadOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            stale_for: Duration::from_secs(300),
            refresh_after: None,
            lease: Duration::from_secs(5),
            wait: Duration::from_secs(10),
        }
    }
}

impl Client {
    pub fn exchange_declare(&mut self, name: &str, o: ExchangeOptions<'_>) -> Result<(), Error> {
        let mut p = vec![o.durable as u8, o.exchange_type as u8];
        if let Some(alt) = o.alternate_exchange {
            if alt.len() > 255 {
                return Err(Error::KeyTooLarge);
            }
            let mut ext = u16v(alt.len()).to_vec();
            ext.extend_from_slice(alt.as_bytes());
            p.extend_from_slice(&u16v(ext.len()));
            p.extend_from_slice(&ext)
        }
        let (s, _) = self.request(X_DECLARE, name.as_bytes(), &p)?;
        ok(s)
    }
    fn binding(queue: &str, key: &str) -> Result<Vec<u8>, Error> {
        if queue.is_empty() || queue.len() > 255 || key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(queue.len()).to_vec();
        p.extend_from_slice(queue.as_bytes());
        p.extend_from_slice(&u16v(key.len()));
        p.extend_from_slice(key.as_bytes());
        Ok(p)
    }
    pub fn exchange_bind(&mut self, exchange: &str, queue: &str, key: &str) -> Result<(), Error> {
        let p = Self::binding(queue, key)?;
        let (s, _) = self.request(X_BIND, exchange.as_bytes(), &p)?;
        ok(s)
    }
    pub fn exchange_unbind(
        &mut self,
        exchange: &str,
        queue: &str,
        key: &str,
    ) -> Result<bool, Error> {
        let p = Self::binding(queue, key)?;
        let (s, _) = self.request(X_UNBIND, exchange.as_bytes(), &p)?;
        if s == 2 {
            Err(Error::Server)
        } else {
            Ok(s == ST_OK)
        }
    }
    pub fn exchange_publish(
        &mut self,
        exchange: &str,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<u32, Error> {
        if key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(key.len()).to_vec();
        p.extend_from_slice(&ttl.map(ms).unwrap_or(0).to_le_bytes());
        p.extend_from_slice(key.as_bytes());
        p.extend_from_slice(value);
        let (s, v) = self.request(X_PUBLISH, exchange.as_bytes(), &p)?;
        if s == ST_MISS {
            return Ok(0);
        }
        ok(s)?;
        if v.len() != 4 {
            return Err(Error::Server);
        }
        Ok(u32::from_le_bytes(v.try_into().unwrap()))
    }
    fn atomic(&mut self, op: u8, key: &str, p: &[u8]) -> Result<AtomicResult, Error> {
        let (s, v) = self.request(op, key.as_bytes(), p)?;
        if s == ST_MISS {
            return Ok(AtomicResult {
                transaction_id: 0,
                routed: 0,
                unroutable: true,
            });
        }
        ok(s)?;
        if v.len() != 12 {
            return Err(Error::Server);
        }
        Ok(AtomicResult {
            transaction_id: u64::from_le_bytes(v[0..8].try_into().unwrap()),
            routed: u32::from_le_bytes(v[8..12].try_into().unwrap()),
            unroutable: false,
        })
    }
    fn atomic_exchange(
        exchange: &str,
        routing_key: &str,
        ttl: Option<Duration>,
        value: &[u8],
    ) -> Result<Vec<u8>, Error> {
        if exchange.len() > u16::MAX as usize || routing_key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(exchange.len()).to_vec();
        p.extend_from_slice(exchange.as_bytes());
        p.extend_from_slice(&u16v(routing_key.len()));
        p.extend_from_slice(routing_key.as_bytes());
        p.extend_from_slice(&(ttl.map(ms).unwrap_or(0).min(u32::MAX as u64) as u32).to_le_bytes());
        p.extend_from_slice(value);
        Ok(p)
    }
    pub fn put_and_publish(
        &mut self,
        key: &str,
        value: &[u8],
        exchange: &str,
        routing_key: &str,
        ttl: Option<Duration>,
    ) -> Result<AtomicResult, Error> {
        let p = Self::atomic_exchange(exchange, routing_key, ttl, value)?;
        self.atomic(A_PUT_PUBLISH, key, &p)
    }
    pub fn update_and_emit(
        &mut self,
        key: &str,
        value: &[u8],
        exchange: &str,
        routing_key: &str,
        ttl: Option<Duration>,
    ) -> Result<AtomicResult, Error> {
        let p = Self::atomic_exchange(exchange, routing_key, ttl, value)?;
        self.atomic(A_UPDATE_EMIT, key, &p)
    }
    pub fn put_and_enqueue(
        &mut self,
        key: &str,
        value: &[u8],
        queue: &str,
        ttl: Option<Duration>,
    ) -> Result<AtomicResult, Error> {
        if queue.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(queue.len()).to_vec();
        p.extend_from_slice(queue.as_bytes());
        p.extend_from_slice(&(ttl.map(ms).unwrap_or(0).min(u32::MAX as u64) as u32).to_le_bytes());
        p.extend_from_slice(value);
        self.atomic(A_PUT_ENQUEUE, key, &p)
    }
    pub fn delete_and_publish(
        &mut self,
        key: &str,
        exchange: &str,
        routing_key: &str,
        message: &[u8],
    ) -> Result<AtomicResult, Error> {
        if exchange.len() > u16::MAX as usize || routing_key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(exchange.len()).to_vec();
        p.extend_from_slice(exchange.as_bytes());
        p.extend_from_slice(&u16v(routing_key.len()));
        p.extend_from_slice(routing_key.as_bytes());
        p.extend_from_slice(&u32v(message.len()));
        p.extend_from_slice(message);
        self.atomic(A_DELETE_PUBLISH, key, &p)
    }
    fn sf(
        &mut self,
        op: u8,
        key: &str,
        d: Duration,
        holder: bool,
    ) -> Result<SingleFlightResult, Error> {
        let n = ms(d).clamp(1, 60000) as u32;
        let (s, v) = self.request(op, key.as_bytes(), &n.to_le_bytes())?;
        decode_sf(s, &v, holder)
    }
    pub fn get_or_claim(
        &mut self,
        key: &str,
        lease: Duration,
    ) -> Result<SingleFlightResult, Error> {
        self.sf(SF_CLAIM, key, lease, false)
    }
    pub fn wait_for_key(
        &mut self,
        key: &str,
        timeout: Duration,
    ) -> Result<SingleFlightResult, Error> {
        self.sf(SF_WAIT, key, timeout, false)
    }
    pub fn get_or_refresh(
        &mut self,
        key: &str,
        lease: Duration,
    ) -> Result<SingleFlightResult, Error> {
        self.require(FEATURE_SWR)?;
        self.sf(SF_REFRESH, key, lease, true)
    }
    pub fn put_and_release(
        &mut self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
        negative: bool,
    ) -> Result<(), Error> {
        let mut p = (ttl.map(ms).unwrap_or(0).min(u32::MAX as u64) as u32)
            .to_le_bytes()
            .to_vec();
        p.push(negative as u8);
        p.extend_from_slice(value);
        let (s, _) = self.request(SF_PUT_RELEASE, key.as_bytes(), &p)?;
        ok(s)
    }
    pub fn release_claim(&mut self, key: &str) -> Result<(), Error> {
        let (s, _) = self.request(SF_RELEASE, key.as_bytes(), b"")?;
        ok(s)
    }
    pub fn put_swr(
        &mut self,
        key: &str,
        value: &[u8],
        ttl: Duration,
        stale_for: Duration,
        refresh_after: Option<Duration>,
    ) -> Result<(), Error> {
        self.require(FEATURE_SWR)?;
        let t = ms(ttl);
        let stale = ms(stale_for);
        let refresh = refresh_after.map(ms).unwrap_or(0);
        const WEEK: u64 = 604800000;
        if t == 0 || t > u32::MAX as u64 || stale == 0 || stale > WEEK || refresh > WEEK {
            return Err(Error::ValueTooLarge);
        }
        let mut meta = (t as u32).to_le_bytes().to_vec();
        meta.extend_from_slice(&(stale as u32).to_le_bytes());
        meta.extend_from_slice(&(refresh as u32).to_le_bytes());
        let (s, _) = self.request_meta(OP_PUT_SWR, key.as_bytes(), &meta, value)?;
        ok(s)
    }
    pub fn get_or_load<F>(
        &mut self,
        key: &str,
        mut loader: F,
        ttl: Option<Duration>,
        lease: Duration,
        wait: Duration,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        F: FnMut() -> Result<Option<Vec<u8>>, Error>,
    {
        let mut r = self.get_or_claim(key, lease)?;
        if r.state == SingleFlightState::Value {
            return Ok(r.value);
        }
        if r.state == SingleFlightState::Negative {
            return Ok(None);
        }
        if r.state == SingleFlightState::Wait {
            for _ in 0..3 {
                let w = self.wait_for_key(key, wait)?;
                if w.state == SingleFlightState::Value {
                    return Ok(w.value);
                }
                if matches!(
                    w.state,
                    SingleFlightState::Negative | SingleFlightState::Timeout
                ) {
                    return Ok(None);
                }
                r = self.get_or_claim(key, lease)?;
                if r.state == SingleFlightState::Claimed {
                    break;
                }
            }
        }
        if r.state != SingleFlightState::Claimed {
            return Ok(None);
        }
        let loaded = match loader() {
            Ok(v) => v,
            Err(e) => {
                let _ = self.release_claim(key);
                return Err(e);
            }
        };
        match loaded {
            Some(v) => {
                self.put_and_release(key, &v, ttl, false)?;
                Ok(Some(v))
            }
            None => {
                self.put_and_release(key, b"", ttl, true)?;
                Ok(None)
            }
        }
    }
    pub fn get_or_load_swr<F>(
        &mut self,
        key: &str,
        mut loader: F,
        options: SwrLoadOptions,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        F: FnMut() -> Result<Option<Vec<u8>>, Error>,
    {
        let mut r = self.get_or_refresh(key, options.lease)?;
        if r.state == SingleFlightState::Value {
            return Ok(r.value);
        }
        if r.state == SingleFlightState::Negative {
            return Ok(None);
        }
        if matches!(
            r.state,
            SingleFlightState::Stale | SingleFlightState::Refresh
        ) && !r.holder
        {
            return Ok(r.value);
        }
        if r.state == SingleFlightState::Wait {
            for _ in 0..3 {
                let w = self.wait_for_key(key, options.wait)?;
                if w.state == SingleFlightState::Value {
                    return Ok(w.value);
                }
                if matches!(
                    w.state,
                    SingleFlightState::Negative | SingleFlightState::Timeout
                ) {
                    return Ok(None);
                }
                r = self.get_or_refresh(key, options.lease)?;
                if r.state == SingleFlightState::Claimed || r.holder {
                    break;
                }
            }
        }
        if r.state != SingleFlightState::Claimed && !r.holder {
            return Ok(None);
        }
        let loaded = match loader() {
            Ok(v) => v,
            Err(e) => {
                let _ = self.release_claim(key);
                return Err(e);
            }
        };
        match loaded {
            Some(v) => {
                self.put_swr(
                    key,
                    &v,
                    options.ttl,
                    options.stale_for,
                    options.refresh_after,
                )?;
                self.release_claim(key)?;
                Ok(Some(v))
            }
            None => {
                self.put_and_release(key, b"", Some(options.ttl), true)?;
                Ok(None)
            }
        }
    }
}
fn decode_sf(s: u8, v: &[u8], with_holder: bool) -> Result<SingleFlightResult, Error> {
    ok(s)?;
    if v.len() < (if with_holder { 2 } else { 1 }) {
        return Err(Error::Server);
    }
    let state = match v[0] {
        0 => SingleFlightState::Value,
        1 => SingleFlightState::Claimed,
        2 => SingleFlightState::Wait,
        3 => SingleFlightState::Negative,
        4 => SingleFlightState::Released,
        5 => SingleFlightState::Timeout,
        6 => SingleFlightState::Lost,
        7 => SingleFlightState::Stale,
        8 => SingleFlightState::Refresh,
        _ => return Err(Error::Server),
    };
    let at = if with_holder { 2 } else { 1 };
    Ok(SingleFlightResult {
        state,
        holder: with_holder && v[1] != 0,
        value: if matches!(
            state,
            SingleFlightState::Value | SingleFlightState::Stale | SingleFlightState::Refresh
        ) {
            Some(v[at..].to_vec())
        } else {
            None
        },
    })
}

#[derive(Clone, Debug)]
pub struct StreamOptions {
    pub partitions: u32,
    pub max_bytes: u64,
    pub max_age: Option<Duration>,
}
impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            partitions: 1,
            max_bytes: 0,
            max_age: None,
        }
    }
}
#[derive(Clone, Debug)]
pub struct StreamInfo {
    pub topic: String,
    pub partitions: u32,
    pub records: u64,
    pub bytes: u64,
}
#[derive(Clone, Debug)]
pub struct StreamGroupInfo {
    pub topic: String,
    pub group: String,
    pub generation: u64,
    pub members: u32,
}
#[derive(Clone, Debug)]
pub struct StreamRecord {
    pub offset: u64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}
#[derive(Clone, Copy, Debug)]
pub struct StreamPosition {
    pub partition: u64,
    pub offset: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct StreamCommit {
    pub partition: u32,
    pub offset: u64,
}
#[derive(Clone, Debug)]
pub struct StreamAssignment {
    pub partitions: Vec<u32>,
    pub generation: u64,
}

impl Client {
    pub fn stream_declare(&mut self, topic: &str, o: StreamOptions) -> Result<(), Error> {
        if topic.is_empty() || topic.len() > 255 || o.partitions == 0 || o.partitions > 256 {
            return Err(Error::KeyTooLarge);
        }
        let mut p = o.partitions.to_le_bytes().to_vec();
        p.extend_from_slice(&o.max_bytes.to_le_bytes());
        p.extend_from_slice(&o.max_age.map(ms).unwrap_or(0).to_le_bytes());
        let (s, _) = self.request(S_DECLARE, topic.as_bytes(), &p)?;
        ok(s)
    }
    pub fn stream_list(&mut self) -> Result<Vec<StreamInfo>, Error> {
        let (s, v) = self.request(S_LIST, b"", b"")?;
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u16()?;
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let l = d.u16()? as usize;
            let topic = String::from_utf8(d.take(l)?.to_vec()).map_err(|_| Error::Server)?;
            out.push(StreamInfo {
                topic,
                partitions: d.u32()?,
                records: d.u64()?,
                bytes: d.u64()?,
            })
        }
        d.done()?;
        Ok(out)
    }
    pub fn stream_group_list(&mut self) -> Result<Vec<StreamGroupInfo>, Error> {
        let (s, v) = self.request(S_GROUP_LIST, b"", b"")?;
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u16()?;
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let tl = d.u16()? as usize;
            let topic = String::from_utf8(d.take(tl)?.to_vec()).map_err(|_| Error::Server)?;
            let gl = d.u16()? as usize;
            let group = String::from_utf8(d.take(gl)?.to_vec()).map_err(|_| Error::Server)?;
            out.push(StreamGroupInfo {
                topic,
                group,
                generation: d.u64()?,
                members: d.u32()?,
            })
        }
        d.done()?;
        Ok(out)
    }
    pub fn stream_append(
        &mut self,
        topic: &str,
        value: &[u8],
        key: &[u8],
        partition: Option<u32>,
    ) -> Result<StreamPosition, Error> {
        if key.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge);
        }
        let mut p = partition.unwrap_or(u32::MAX).to_le_bytes().to_vec();
        p.extend_from_slice(&u16v(key.len()));
        p.extend_from_slice(key);
        p.extend_from_slice(value);
        let (s, v) = self.request(S_APPEND, topic.as_bytes(), &p)?;
        ok(s)?;
        position(&v)
    }
    pub fn stream_append_batch(
        &mut self,
        topic: &str,
        items: &[(&[u8], &[u8])],
        partition: Option<u32>,
    ) -> Result<Vec<StreamPosition>, Error> {
        if items.is_empty() || items.len() > 1024 {
            return Err(Error::ValueTooLarge);
        }
        self.require(FEATURE_STREAM_BATCH)?;
        let mut p = partition.unwrap_or(u32::MAX).to_le_bytes().to_vec();
        p.extend_from_slice(&u32v(items.len()));
        for (key, value) in items {
            if key.len() > u16::MAX as usize {
                return Err(Error::KeyTooLarge);
            }
            p.extend_from_slice(&u16v(key.len()));
            p.extend_from_slice(&u32v(value.len()));
            p.extend_from_slice(key);
            p.extend_from_slice(value)
        }
        let (s, v) = self.request(S_APPEND_BATCH, topic.as_bytes(), &p)?;
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u32()? as usize;
        if n != items.len() {
            return Err(Error::Server);
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(StreamPosition {
                partition: d.u64()?,
                offset: d.u64()?,
            })
        }
        d.done()?;
        Ok(out)
    }
    pub fn stream_fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_records: u32,
    ) -> Result<Vec<StreamRecord>, Error> {
        if max_records == 0 || max_records > 1024 {
            return Err(Error::ValueTooLarge);
        }
        let keyed = self.capabilities()?.features & FEATURE_STREAM_KEYS != 0;
        let mut p = partition.to_le_bytes().to_vec();
        p.extend_from_slice(&offset.to_le_bytes());
        p.extend_from_slice(&max_records.to_le_bytes());
        let (s, v) = self.request(
            if keyed { S_FETCH_KEYS } else { S_FETCH },
            topic.as_bytes(),
            &p,
        )?;
        if s == ST_MISS {
            return Ok(vec![]);
        }
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u32()?;
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let offset = d.u64()?;
            let kl = if keyed { d.u16()? as usize } else { 0 };
            let vl = d.u32()? as usize;
            let key = d.take(kl)?.to_vec();
            let value = d.take(vl)?.to_vec();
            out.push(StreamRecord {
                offset,
                key: if keyed { Some(key) } else { None },
                value,
            })
        }
        d.done()?;
        Ok(out)
    }
    fn group_partition(group: &str, partition: u32) -> Result<Vec<u8>, Error> {
        if group.is_empty() || group.len() > 255 {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(group.len()).to_vec();
        p.extend_from_slice(group.as_bytes());
        p.extend_from_slice(&partition.to_le_bytes());
        Ok(p)
    }
    pub fn stream_commit(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
        offset: u64,
    ) -> Result<(), Error> {
        let mut p = Self::group_partition(group, partition)?;
        p.extend_from_slice(&offset.to_le_bytes());
        let (s, _) = self.request(S_COMMIT, topic.as_bytes(), &p)?;
        ok(s)
    }
    pub fn stream_commit_batch(
        &mut self,
        topic: &str,
        group: &str,
        commits: &[StreamCommit],
    ) -> Result<(), Error> {
        if commits.is_empty() || commits.len() > 256 {
            return Err(Error::ValueTooLarge);
        }
        self.require(FEATURE_STREAM_COMMIT_BATCH)?;
        if group.is_empty() || group.len() > 255 {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(group.len()).to_vec();
        p.extend_from_slice(group.as_bytes());
        p.extend_from_slice(&u32v(commits.len()));
        for c in commits {
            p.extend_from_slice(&c.partition.to_le_bytes());
            p.extend_from_slice(&c.offset.to_le_bytes())
        }
        let (s, _) = self.request(S_COMMIT_BATCH, topic.as_bytes(), &p)?;
        ok(s)
    }
    fn stream_group_value(
        &mut self,
        op: u8,
        topic: &str,
        group: &str,
        partition: u32,
    ) -> Result<Option<u64>, Error> {
        let p = Self::group_partition(group, partition)?;
        let (s, v) = self.request(op, topic.as_bytes(), &p)?;
        if s == ST_MISS {
            return Ok(None);
        }
        ok(s)?;
        if v.len() != 8 {
            return Err(Error::Server);
        }
        Ok(Some(u64::from_le_bytes(v.try_into().unwrap())))
    }
    pub fn stream_group_offset(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
    ) -> Result<Option<u64>, Error> {
        self.stream_group_value(S_OFFSET, topic, group, partition)
    }
    pub fn stream_group_lag(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
    ) -> Result<Option<u64>, Error> {
        self.stream_group_value(S_LAG, topic, group, partition)
    }
    pub fn stream_group_join(
        &mut self,
        topic: &str,
        group: &str,
        lease: Duration,
    ) -> Result<StreamAssignment, Error> {
        if group.is_empty() || group.len() > 255 {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(group.len()).to_vec();
        p.extend_from_slice(group.as_bytes());
        p.extend_from_slice(&(ms(lease).clamp(1, 60000) as u32).to_le_bytes());
        let (s, v) = self.request(S_JOIN, topic.as_bytes(), &p)?;
        ok(s)?;
        let mut d = Dec { b: &v, i: 0 };
        let n = d.u32()?;
        if n > 256 {
            return Err(Error::Server);
        }
        let mut partitions = Vec::with_capacity(n as usize);
        for _ in 0..n {
            partitions.push(d.u32()?)
        }
        let generation = if d.i < v.len() { d.u64()? } else { 0 };
        d.done()?;
        Ok(StreamAssignment {
            partitions,
            generation,
        })
    }
    pub fn stream_group_leave(&mut self, topic: &str, group: &str) -> Result<(), Error> {
        if group.is_empty() || group.len() > 255 {
            return Err(Error::KeyTooLarge);
        }
        let mut p = u16v(group.len()).to_vec();
        p.extend_from_slice(group.as_bytes());
        let (s, _) = self.request(S_LEAVE, topic.as_bytes(), &p)?;
        ok(s)
    }
}
fn position(v: &[u8]) -> Result<StreamPosition, Error> {
    if v.len() != 16 {
        return Err(Error::Server);
    }
    Ok(StreamPosition {
        partition: u64::from_le_bytes(v[0..8].try_into().unwrap()),
        offset: u64::from_le_bytes(v[8..16].try_into().unwrap()),
    })
}
