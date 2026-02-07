//! Upstream Management with RTT tracking and startup ping

use std::net::{SocketAddr, IpAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::Instant;
use rand::Rng;
use tracing::{debug, warn, error, info};

use crate::config::{UpstreamConfig, UpstreamType};
use crate::error::{Result, ProxyError};
use crate::protocol::constants::{TG_DATACENTERS_V4, TG_DATACENTERS_V6, TG_DATACENTER_PORT};
use crate::transport::socket::create_outgoing_socket_bound;
use crate::transport::socks::{connect_socks4, connect_socks5};

// ============= RTT Tracking =============

/// Exponential moving average for latency tracking
#[derive(Debug, Clone)]
struct LatencyEma {
    /// Current EMA value in milliseconds (None = no data yet)
    value_ms: Option<f64>,
    /// Smoothing factor (0.0 - 1.0, higher = more weight to recent)
    alpha: f64,
}

impl LatencyEma {
    fn new(alpha: f64) -> Self {
        Self { value_ms: None, alpha }
    }
    
    fn update(&mut self, sample_ms: f64) {
        self.value_ms = Some(match self.value_ms {
            None => sample_ms,
            Some(prev) => prev * (1.0 - self.alpha) + sample_ms * self.alpha,
        });
    }
    
    fn get(&self) -> Option<f64> {
        self.value_ms
    }
}

// ============= Upstream State =============

#[derive(Debug)]
struct UpstreamState {
    config: UpstreamConfig,
    healthy: bool,
    fails: u32,
    last_check: std::time::Instant,
    /// Latency EMA (alpha=0.3 — moderate smoothing)
    latency: LatencyEma,
}

/// Result of a single DC ping
#[derive(Debug, Clone)]
pub struct DcPingResult {
    pub dc_idx: usize,
    pub dc_addr: SocketAddr,
    pub rtt_ms: Option<f64>,
    pub error: Option<String>,
}

/// Result of startup ping across all DCs
#[derive(Debug, Clone)]
pub struct StartupPingResult {
    pub results: Vec<DcPingResult>,
    pub upstream_name: String,
}

// ============= Upstream Manager =============

#[derive(Clone)]
pub struct UpstreamManager {
    upstreams: Arc<RwLock<Vec<UpstreamState>>>,
}

impl UpstreamManager {
    pub fn new(configs: Vec<UpstreamConfig>) -> Self {
        let states = configs.into_iter()
            .filter(|c| c.enabled)
            .map(|c| UpstreamState {
                config: c,
                healthy: true,
                fails: 0,
                last_check: std::time::Instant::now(),
                latency: LatencyEma::new(0.3),
            })
            .collect();
            
        Self {
            upstreams: Arc::new(RwLock::new(states)),
        }
    }
    
    /// Select an upstream using weighted selection among healthy upstreams
    async fn select_upstream(&self) -> Option<usize> {
        let upstreams = self.upstreams.read().await;
        if upstreams.is_empty() {
            return None;
        }

        let healthy_indices: Vec<usize> = upstreams.iter()
            .enumerate()
            .filter(|(_, u)| u.healthy)
            .map(|(i, _)| i)
            .collect();
            
        if healthy_indices.is_empty() {
            return Some(rand::rng().gen_range(0..upstreams.len()));
        }
        
        let total_weight: u32 = healthy_indices.iter()
            .map(|&i| upstreams[i].config.weight as u32)
            .sum();
            
        if total_weight == 0 {
            return Some(healthy_indices[rand::rng().gen_range(0..healthy_indices.len())]);
        }
        
        let mut choice = rand::rng().gen_range(0..total_weight);
        
        for &idx in &healthy_indices {
            let weight = upstreams[idx].config.weight as u32;
            if choice < weight {
                return Some(idx);
            }
            choice -= weight;
        }
        
        Some(healthy_indices[0])
    }
    
    pub async fn connect(&self, target: SocketAddr) -> Result<TcpStream> {
        let idx = self.select_upstream().await
            .ok_or_else(|| ProxyError::Config("No upstreams available".to_string()))?;
            
        let upstream = {
            let guard = self.upstreams.read().await;
            guard[idx].config.clone()
        };
        
        let start = Instant::now();
        
        match self.connect_via_upstream(&upstream, target).await {
            Ok(stream) => {
                let rtt_ms = start.elapsed().as_secs_f64() * 1000.0;
                let mut guard = self.upstreams.write().await;
                if let Some(u) = guard.get_mut(idx) {
                    if !u.healthy {
                        debug!(rtt_ms = rtt_ms, "Upstream recovered: {:?}", u.config);
                    }
                    u.healthy = true;
                    u.fails = 0;
                    u.latency.update(rtt_ms);
                }
                Ok(stream)
            },
            Err(e) => {
                let mut guard = self.upstreams.write().await;
                if let Some(u) = guard.get_mut(idx) {
                    u.fails += 1;
                    warn!("Upstream {:?} failed: {}. Consecutive fails: {}", u.config, e, u.fails);
                    if u.fails > 3 {
                        u.healthy = false;
                        warn!("Upstream marked unhealthy: {:?}", u.config);
                    }
                }
                Err(e)
            }
        }
    }
    
    async fn connect_via_upstream(&self, config: &UpstreamConfig, target: SocketAddr) -> Result<TcpStream> {
        match &config.upstream_type {
            UpstreamType::Direct { interface } => {
                let bind_ip = interface.as_ref()
                    .and_then(|s| s.parse::<IpAddr>().ok());
                
                let socket = create_outgoing_socket_bound(target, bind_ip)?;
                
                socket.set_nonblocking(true)?;
                match socket.connect(&target.into()) {
                    Ok(()) => {},
                    Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) || err.kind() == std::io::ErrorKind::WouldBlock => {},
                    Err(err) => return Err(ProxyError::Io(err)),
                }
                
                let std_stream: std::net::TcpStream = socket.into();
                let stream = TcpStream::from_std(std_stream)?;
                
                stream.writable().await?;
                if let Some(e) = stream.take_error()? {
                    return Err(ProxyError::Io(e));
                }
                
                Ok(stream)
            },
            UpstreamType::Socks4 { address, interface, user_id } => {
                info!("Connecting to {} via SOCKS4 {}", target, address);
                
                let proxy_addr: SocketAddr = address.parse()
                    .map_err(|_| ProxyError::Config("Invalid SOCKS4 address".to_string()))?;
                    
                let bind_ip = interface.as_ref()
                    .and_then(|s| s.parse::<IpAddr>().ok());
                
                let socket = create_outgoing_socket_bound(proxy_addr, bind_ip)?;
                
                socket.set_nonblocking(true)?;
                match socket.connect(&proxy_addr.into()) {
                    Ok(()) => {},
                    Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) || err.kind() == std::io::ErrorKind::WouldBlock => {},
                    Err(err) => return Err(ProxyError::Io(err)),
                }
                
                let std_stream: std::net::TcpStream = socket.into();
                let mut stream = TcpStream::from_std(std_stream)?;
                
                stream.writable().await?;
                if let Some(e) = stream.take_error()? {
                    return Err(ProxyError::Io(e));
                }
                
                connect_socks4(&mut stream, target, user_id.as_deref()).await?;
                Ok(stream)
            },
            UpstreamType::Socks5 { address, interface, username, password } => {
                info!("Connecting to {} via SOCKS5 {}", target, address);
                
                let proxy_addr: SocketAddr = address.parse()
                    .map_err(|_| ProxyError::Config("Invalid SOCKS5 address".to_string()))?;
                    
                let bind_ip = interface.as_ref()
                    .and_then(|s| s.parse::<IpAddr>().ok());
                
                let socket = create_outgoing_socket_bound(proxy_addr, bind_ip)?;
                
                socket.set_nonblocking(true)?;
                match socket.connect(&proxy_addr.into()) {
                    Ok(()) => {},
                    Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) || err.kind() == std::io::ErrorKind::WouldBlock => {},
                    Err(err) => return Err(ProxyError::Io(err)),
                }
                
                let std_stream: std::net::TcpStream = socket.into();
                let mut stream = TcpStream::from_std(std_stream)?;
                
                stream.writable().await?;
                if let Some(e) = stream.take_error()? {
                    return Err(ProxyError::Io(e));
                }
                
                connect_socks5(&mut stream, target, username.as_deref(), password.as_deref()).await?;
                Ok(stream)
            },
        }
    }
    
    // ============= Startup Ping =============
    
    /// Ping all Telegram DCs through all upstreams and return results.
    ///
    /// Used at startup to display connectivity and latency info.
    pub async fn ping_all_dcs(&self, prefer_ipv6: bool) -> Vec<StartupPingResult> {
        let upstreams: Vec<(usize, UpstreamConfig)> = {
            let guard = self.upstreams.read().await;
            guard.iter().enumerate()
                .map(|(i, u)| (i, u.config.clone()))
                .collect()
        };
        
        let datacenters = if prefer_ipv6 { &*TG_DATACENTERS_V6 } else { &*TG_DATACENTERS_V4 };
        
        let mut all_results = Vec::new();
        
        for (upstream_idx, upstream_config) in &upstreams {
            let upstream_name = match &upstream_config.upstream_type {
                UpstreamType::Direct { interface } => {
                    format!("direct{}", interface.as_ref().map(|i| format!(" ({})", i)).unwrap_or_default())
                }
                UpstreamType::Socks4 { address, .. } => format!("socks4://{}", address),
                UpstreamType::Socks5 { address, .. } => format!("socks5://{}", address),
            };
            
            let mut dc_results = Vec::new();
            
            for (dc_zero_idx, dc_ip) in datacenters.iter().enumerate() {
                let dc_addr = SocketAddr::new(*dc_ip, TG_DATACENTER_PORT);
                
                let ping_result = tokio::time::timeout(
                    Duration::from_secs(5),
                    self.ping_single_dc(upstream_config, dc_addr)
                ).await;
                
                let result = match ping_result {
                    Ok(Ok(rtt_ms)) => {
                        // Update latency EMA
                        let mut guard = self.upstreams.write().await;
                        if let Some(u) = guard.get_mut(*upstream_idx) {
                            u.latency.update(rtt_ms);
                        }
                        DcPingResult {
                            dc_idx: dc_zero_idx + 1,
                            dc_addr,
                            rtt_ms: Some(rtt_ms),
                            error: None,
                        }
                    }
                    Ok(Err(e)) => DcPingResult {
                        dc_idx: dc_zero_idx + 1,
                        dc_addr,
                        rtt_ms: None,
                        error: Some(e.to_string()),
                    },
                    Err(_) => DcPingResult {
                        dc_idx: dc_zero_idx + 1,
                        dc_addr,
                        rtt_ms: None,
                        error: Some("timeout (5s)".to_string()),
                    },
                };
                
                dc_results.push(result);
            }
            
            all_results.push(StartupPingResult {
                results: dc_results,
                upstream_name,
            });
        }
        
        all_results
    }
    
    /// Ping a single DC: TCP connect, measure RTT, then drop.
    async fn ping_single_dc(&self, config: &UpstreamConfig, target: SocketAddr) -> Result<f64> {
        let start = Instant::now();
        let _stream = self.connect_via_upstream(config, target).await?;
        let rtt = start.elapsed();
        Ok(rtt.as_secs_f64() * 1000.0)
    }
    
    // ============= Health Checks =============
    
    /// Background health check task.
    ///
    /// Every 30 seconds, pings one representative DC per upstream.
    /// Measures RTT and updates health status.
    pub async fn run_health_checks(&self, prefer_ipv6: bool) {
        let datacenters = if prefer_ipv6 { &*TG_DATACENTERS_V6 } else { &*TG_DATACENTERS_V4 };
        
        // Rotate through DCs across check cycles
        let mut dc_rotation = 0usize;
        
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            
            let check_dc_idx = dc_rotation % datacenters.len();
            dc_rotation += 1;
            
            let check_target = SocketAddr::new(datacenters[check_dc_idx], TG_DATACENTER_PORT);
            
            let count = self.upstreams.read().await.len();
            for i in 0..count {
                let config = {
                    let guard = self.upstreams.read().await;
                    guard[i].config.clone()
                };
                
                let start = Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_secs(10),
                    self.connect_via_upstream(&config, check_target)
                ).await;
                
                let mut guard = self.upstreams.write().await;
                let u = &mut guard[i];
                
                match result {
                    Ok(Ok(_stream)) => {
                        let rtt_ms = start.elapsed().as_secs_f64() * 1000.0;
                        u.latency.update(rtt_ms);
                        
                        if !u.healthy {
                            info!(
                                rtt_ms = format!("{:.1}", rtt_ms),
                                dc = check_dc_idx + 1,
                                "Upstream recovered: {:?}", u.config
                            );
                        }
                        u.healthy = true;
                        u.fails = 0;
                    }
                    Ok(Err(e)) => {
                        u.fails += 1;
                        debug!(
                            dc = check_dc_idx + 1,
                            fails = u.fails,
                            "Health check failed for {:?}: {}", u.config, e
                        );
                        if u.fails > 3 {
                            u.healthy = false;
                            warn!("Upstream unhealthy (health check): {:?}", u.config);
                        }
                    }
                    Err(_) => {
                        u.fails += 1;
                        debug!(
                            dc = check_dc_idx + 1,
                            fails = u.fails,
                            "Health check timeout for {:?}", u.config
                        );
                        if u.fails > 3 {
                            u.healthy = false;
                            warn!("Upstream unhealthy (timeout): {:?}", u.config);
                        }
                    }
                }
                u.last_check = std::time::Instant::now();
            }
        }
    }
}