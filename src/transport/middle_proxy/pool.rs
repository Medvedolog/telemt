use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64};
use bytes::BytesMut;
use rand::Rng;
use rand::seq::SliceRandom;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};
use std::time::Duration;

use crate::crypto::SecureRandom;
use crate::error::{ProxyError, Result};
use crate::protocol::constants::*;

use super::ConnRegistry;
use super::codec::RpcWriter;
use super::reader::reader_loop;

const ME_ACTIVE_PING_SECS: u64 = 25;
const ME_ACTIVE_PING_JITTER_SECS: i64 = 5;

pub struct MePool {
    pub(super) registry: Arc<ConnRegistry>,
    pub(super) writers: Arc<RwLock<Vec<(SocketAddr, Arc<Mutex<RpcWriter>>)>>> ,
    pub(super) rr: AtomicU64,
    pub(super) proxy_tag: Option<Vec<u8>>,
    pub(super) proxy_secret: Arc<RwLock<Vec<u8>>>,
    pub(super) nat_ip_cfg: Option<IpAddr>,
    pub(super) nat_ip_detected: Arc<RwLock<Option<IpAddr>>>,
    pub(super) nat_probe: bool,
    pub(super) nat_stun: Option<String>,
    pub(super) proxy_map_v4: Arc<RwLock<HashMap<i32, Vec<(IpAddr, u16)>>>>,
    pub(super) proxy_map_v6: Arc<RwLock<HashMap<i32, Vec<(IpAddr, u16)>>>>,
    pub(super) default_dc: AtomicI32,
    pool_size: usize,
}

impl MePool {
    pub fn new(
        proxy_tag: Option<Vec<u8>>,
        proxy_secret: Vec<u8>,
        nat_ip: Option<IpAddr>,
        nat_probe: bool,
        nat_stun: Option<String>,
        proxy_map_v4: HashMap<i32, Vec<(IpAddr, u16)>>,
        proxy_map_v6: HashMap<i32, Vec<(IpAddr, u16)>>,
        default_dc: Option<i32>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry: Arc::new(ConnRegistry::new()),
            writers: Arc::new(RwLock::new(Vec::new())),
            rr: AtomicU64::new(0),
            proxy_tag,
            proxy_secret: Arc::new(RwLock::new(proxy_secret)),
            nat_ip_cfg: nat_ip,
            nat_ip_detected: Arc::new(RwLock::new(None)),
            nat_probe,
            nat_stun,
            pool_size: 2,
            proxy_map_v4: Arc::new(RwLock::new(proxy_map_v4)),
            proxy_map_v6: Arc::new(RwLock::new(proxy_map_v6)),
            default_dc: AtomicI32::new(default_dc.unwrap_or(0)),
        })
    }

    pub fn has_proxy_tag(&self) -> bool {
        self.proxy_tag.is_some()
    }

    pub fn translate_our_addr(&self, addr: SocketAddr) -> SocketAddr {
        let ip = self.translate_ip_for_nat(addr.ip());
        SocketAddr::new(ip, addr.port())
    }

    pub fn registry(&self) -> &Arc<ConnRegistry> {
        &self.registry
    }

    fn writers_arc(&self) -> Arc<RwLock<Vec<(SocketAddr, Arc<Mutex<RpcWriter>>)>>>
    {
        self.writers.clone()
    }

    pub async fn reconcile_connections(&self, rng: &SecureRandom) {
        use std::collections::HashSet;
        let map = self.proxy_map_v4.read().await.clone();
        let writers = self.writers.read().await;
        let current: HashSet<SocketAddr> = writers.iter().map(|(a, _)| *a).collect();
        drop(writers);

        for (_dc, addrs) in map.iter() {
            let dc_addrs: Vec<SocketAddr> = addrs
                .iter()
                .map(|(ip, port)| SocketAddr::new(*ip, *port))
                .collect();
            if !dc_addrs.iter().any(|a| current.contains(a)) {
                let mut shuffled = dc_addrs.clone();
                shuffled.shuffle(&mut rand::rng());
                for addr in shuffled {
                    if self.connect_one(addr, rng).await.is_ok() {
                        break;
                    }
                }
            }
        }
    }

    pub async fn update_proxy_maps(
        &self,
        new_v4: HashMap<i32, Vec<(IpAddr, u16)>>,
        new_v6: Option<HashMap<i32, Vec<(IpAddr, u16)>>>,
    ) -> bool {
        let mut changed = false;
        {
            let mut guard = self.proxy_map_v4.write().await;
            if !new_v4.is_empty() && *guard != new_v4 {
                *guard = new_v4;
                changed = true;
            }
        }
        if let Some(v6) = new_v6 {
            let mut guard = self.proxy_map_v6.write().await;
            if !v6.is_empty() && *guard != v6 {
                *guard = v6;
            }
        }
        changed
    }

    pub async fn update_secret(&self, new_secret: Vec<u8>) -> bool {
        if new_secret.len() < 32 {
            warn!(len = new_secret.len(), "proxy-secret update ignored (too short)");
            return false;
        }
        let mut guard = self.proxy_secret.write().await;
        if *guard != new_secret {
            *guard = new_secret;
            drop(guard);
            self.reconnect_all().await;
            return true;
        }
        false
    }

    pub async fn reconnect_all(&self) {
        // Graceful: do not drop all at once. New connections will use updated secret.
        // Existing writers remain until health monitor replaces them.
        // No-op here to avoid total outage.
    }

    pub(super) async fn key_selector(&self) -> u32 {
        let secret = self.proxy_secret.read().await;
        if secret.len() >= 4 {
            u32::from_le_bytes([secret[0], secret[1], secret[2], secret[3]])
        } else {
            0
        }
    }

    pub async fn init(self: &Arc<Self>, pool_size: usize, rng: &SecureRandom) -> Result<()> {
        let map = self.proxy_map_v4.read().await;
        let ks = self.key_selector().await;
        info!(
            me_servers = map.len(),
            pool_size,
            key_selector = format_args!("0x{ks:08x}"),
            secret_len = self.proxy_secret.read().await.len(),
            "Initializing ME pool"
        );

        // Ensure at least one connection per DC with failover over all addresses
        for (dc, addrs) in map.iter() {
            if addrs.is_empty() {
                continue;
            }
            let mut connected = false;
            let mut shuffled = addrs.clone();
            shuffled.shuffle(&mut rand::rng());
            for (ip, port) in shuffled {
                let addr = SocketAddr::new(ip, port);
                match self.connect_one(addr, rng).await {
                    Ok(()) => {
                        info!(%addr, dc = %dc, "ME connected");
                        connected = true;
                        break;
                    }
                    Err(e) => warn!(%addr, dc = %dc, error = %e, "ME connect failed, trying next"),
                }
            }
            if !connected {
                warn!(dc = %dc, "All ME servers for DC failed at init");
            }
        }

        // Additional connections up to pool_size total (round-robin across DCs)
        for (dc, addrs) in map.iter() {
            for (ip, port) in addrs {
                if self.connection_count() >= pool_size {
                    break;
                }
                let addr = SocketAddr::new(*ip, *port);
                if let Err(e) = self.connect_one(addr, rng).await {
                    debug!(%addr, dc = %dc, error = %e, "Extra ME connect failed");
                }
            }
            if self.connection_count() >= pool_size {
                break;
            }
        }

        if self.writers.read().await.is_empty() {
            return Err(ProxyError::Proxy("No ME connections".into()));
        }
        Ok(())
    }

    pub(crate) async fn connect_one(&self, addr: SocketAddr, rng: &SecureRandom) -> Result<()> {
        let secret_len = self.proxy_secret.read().await.len();
        if secret_len < 32 {
            return Err(ProxyError::Proxy("proxy-secret too short for ME auth".into()));
        }

        let (stream, _connect_ms) = self.connect_tcp(addr).await?;
        let hs = self.handshake_only(stream, addr, rng).await?;

        let rpc_w = Arc::new(Mutex::new(RpcWriter {
            writer: hs.wr,
            key: hs.write_key,
            iv: hs.write_iv,
            seq_no: 0,
        }));
        self.writers.write().await.push((addr, rpc_w.clone()));

        let reg = self.registry.clone();
        let w_pong = rpc_w.clone();
        let w_pool = self.writers_arc();
        let w_ping = rpc_w.clone();
        let w_pool_ping = self.writers_arc();
        tokio::spawn(async move {
            if let Err(e) =
                reader_loop(hs.rd, hs.read_key, hs.read_iv, reg, BytesMut::new(), BytesMut::new(), w_pong.clone()).await
            {
                warn!(error = %e, "ME reader ended");
            }
            let mut ws = w_pool.write().await;
            ws.retain(|(_, w)| !Arc::ptr_eq(w, &w_pong));
            info!(remaining = ws.len(), "Dead ME writer removed from pool");
        });
        tokio::spawn(async move {
            let mut ping_id: i64 = rand::random::<i64>();
            loop {
                let jitter = rand::rng()
                    .random_range(-ME_ACTIVE_PING_JITTER_SECS..=ME_ACTIVE_PING_JITTER_SECS);
                let wait = (ME_ACTIVE_PING_SECS as i64 + jitter).max(5) as u64;
                tokio::time::sleep(Duration::from_secs(wait)).await;
                let mut p = Vec::with_capacity(12);
                p.extend_from_slice(&RPC_PING_U32.to_le_bytes());
                p.extend_from_slice(&ping_id.to_le_bytes());
                ping_id = ping_id.wrapping_add(1);
                if let Err(e) = w_ping.lock().await.send(&p).await {
                    debug!(error = %e, "Active ME ping failed, removing dead writer");
                    let mut ws = w_pool_ping.write().await;
                    ws.retain(|(_, w)| !Arc::ptr_eq(w, &w_ping));
                    break;
                }
            }
        });

        Ok(())
    }

}

fn hex_dump(data: &[u8]) -> String {
    const MAX: usize = 64;
    let mut out = String::with_capacity(data.len() * 2 + 3);
    for (i, b) in data.iter().take(MAX).enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{b:02x}"));
    }
    if data.len() > MAX {
        out.push_str(" …");
    }
    out
}
