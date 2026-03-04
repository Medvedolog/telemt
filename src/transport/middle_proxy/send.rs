use std::cmp::Reverse;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, warn};

use crate::error::{ProxyError, Result};
use crate::network::IpFamily;
use crate::protocol::constants::RPC_CLOSE_EXT_U32;

use super::MePool;
use super::codec::WriterCommand;
use super::pool::WriterContour;
use super::wire::build_proxy_req_payload;
use rand::seq::SliceRandom;
use super::registry::ConnMeta;

const IDLE_WRITER_PENALTY_MID_SECS: u64 = 45;
const IDLE_WRITER_PENALTY_HIGH_SECS: u64 = 55;

impl MePool {
    /// Send RPC_PROXY_REQ. `tag_override`: per-user ad_tag (from access.user_ad_tags); if None, uses pool default.
    pub async fn send_proxy_req(
        self: &Arc<Self>,
        conn_id: u64,
        target_dc: i16,
        client_addr: SocketAddr,
        our_addr: SocketAddr,
        data: &[u8],
        proto_flags: u32,
        tag_override: Option<&[u8]>,
    ) -> Result<()> {
        let tag = tag_override.or(self.proxy_tag.as_deref());
        let payload = build_proxy_req_payload(
            conn_id,
            client_addr,
            our_addr,
            data,
            tag,
            proto_flags,
        );
        let meta = ConnMeta {
            target_dc,
            client_addr,
            our_addr,
            proto_flags,
        };
        let mut emergency_attempts = 0;

        loop {
            if let Some(current) = self.registry.get_writer(conn_id).await {
                match current.tx.try_send(WriterCommand::Data(payload.clone())) {
                    Ok(()) => return Ok(()),
                    Err(TrySendError::Full(cmd)) => {
                        if current.tx.send(cmd).await.is_ok() {
                            return Ok(());
                        }
                        warn!(writer_id = current.writer_id, "ME writer channel closed");
                        self.remove_writer_and_close_clients(current.writer_id).await;
                        continue;
                    }
                    Err(TrySendError::Closed(_)) => {
                        warn!(writer_id = current.writer_id, "ME writer channel closed");
                        self.remove_writer_and_close_clients(current.writer_id).await;
                        continue;
                    }
                }
            }

            let mut writers_snapshot = {
                let ws = self.writers.read().await;
                if ws.is_empty() {
                    // Create waiter before recovery attempts so notify_one permits are not missed.
                    let waiter = self.writer_available.notified();
                    drop(ws);
                    for family in self.family_order() {
                        let map = match family {
                            IpFamily::V4 => self.proxy_map_v4.read().await.clone(),
                            IpFamily::V6 => self.proxy_map_v6.read().await.clone(),
                        };
                        for (_dc, addrs) in map.iter() {
                            for (ip, port) in addrs {
                                let addr = SocketAddr::new(*ip, *port);
                                if self.connect_one(addr, self.rng.as_ref()).await.is_ok() {
                                    self.writer_available.notify_one();
                                    break;
                                }
                            }
                        }
                    }
                    if !self.writers.read().await.is_empty() {
                        continue;
                    }
                    if tokio::time::timeout(Duration::from_secs(3), waiter).await.is_err() {
                        if !self.writers.read().await.is_empty() {
                            continue;
                        }
                        return Err(ProxyError::Proxy("All ME connections dead (waited 3s)".into()));
                    }
                    continue;
                }
                ws.clone()
            };

            let mut candidate_indices = self
                .candidate_indices_for_dc(&writers_snapshot, target_dc, false)
                .await;
            if candidate_indices.is_empty() {
                candidate_indices = self
                    .candidate_indices_for_dc(&writers_snapshot, target_dc, true)
                    .await;
            }
            if candidate_indices.is_empty() {
                // Emergency connect-on-demand
                if emergency_attempts >= 3 {
                    return Err(ProxyError::Proxy("No ME writers available for target DC".into()));
                }
                emergency_attempts += 1;
                for family in self.family_order() {
                    let map_guard = match family {
                        IpFamily::V4 => self.proxy_map_v4.read().await,
                        IpFamily::V6 => self.proxy_map_v6.read().await,
                    };
                    if let Some(addrs) = map_guard.get(&(target_dc as i32)) {
                        let mut shuffled = addrs.clone();
                        shuffled.shuffle(&mut rand::rng());
                        drop(map_guard);
                        for (ip, port) in shuffled {
                            let addr = SocketAddr::new(ip, port);
                            if self.connect_one(addr, self.rng.as_ref()).await.is_ok() {
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(100 * emergency_attempts)).await;
                        let ws2 = self.writers.read().await;
                        writers_snapshot = ws2.clone();
                        drop(ws2);
                        candidate_indices = self
                            .candidate_indices_for_dc(&writers_snapshot, target_dc, false)
                            .await;
                        if candidate_indices.is_empty() {
                            candidate_indices = self
                                .candidate_indices_for_dc(&writers_snapshot, target_dc, true)
                                .await;
                        }
                        if !candidate_indices.is_empty() {
                            break;
                        }
                    }
                }
                if candidate_indices.is_empty() {
                    return Err(ProxyError::Proxy("No ME writers available for target DC".into()));
                }
            }
            let writer_idle_since = self.registry.writer_idle_since_snapshot().await;
            let now_epoch_secs = Self::now_epoch_secs();

            if self.me_deterministic_writer_sort.load(Ordering::Relaxed) {
                candidate_indices.sort_by(|lhs, rhs| {
                    let left = &writers_snapshot[*lhs];
                    let right = &writers_snapshot[*rhs];
                    let left_key = (
                        self.writer_contour_rank_for_selection(left),
                        (left.generation < self.current_generation()) as usize,
                        left.degraded.load(Ordering::Relaxed) as usize,
                        self.writer_idle_rank_for_selection(
                            left,
                            &writer_idle_since,
                            now_epoch_secs,
                        ),
                        Reverse(left.tx.capacity()),
                        left.addr,
                        left.id,
                    );
                    let right_key = (
                        self.writer_contour_rank_for_selection(right),
                        (right.generation < self.current_generation()) as usize,
                        right.degraded.load(Ordering::Relaxed) as usize,
                        self.writer_idle_rank_for_selection(
                            right,
                            &writer_idle_since,
                            now_epoch_secs,
                        ),
                        Reverse(right.tx.capacity()),
                        right.addr,
                        right.id,
                    );
                    left_key.cmp(&right_key)
                });
            } else {
                candidate_indices.sort_by_key(|idx| {
                    let w = &writers_snapshot[*idx];
                    let degraded = w.degraded.load(Ordering::Relaxed);
                    let stale = (w.generation < self.current_generation()) as usize;
                    (
                        self.writer_contour_rank_for_selection(w),
                        stale,
                        degraded as usize,
                        self.writer_idle_rank_for_selection(
                            w,
                            &writer_idle_since,
                            now_epoch_secs,
                        ),
                        Reverse(w.tx.capacity()),
                    )
                });
            }

            let start = self.rr.fetch_add(1, Ordering::Relaxed) as usize % candidate_indices.len();
            let mut fallback_blocking_idx: Option<usize> = None;

            for offset in 0..candidate_indices.len() {
                let idx = candidate_indices[(start + offset) % candidate_indices.len()];
                let w = &writers_snapshot[idx];
                if !self.writer_accepts_new_binding(w) {
                    continue;
                }
                match w.tx.try_send(WriterCommand::Data(payload.clone())) {
                    Ok(()) => {
                        self.registry
                            .bind_writer(conn_id, w.id, w.tx.clone(), meta.clone())
                            .await;
                        if w.generation < self.current_generation() {
                            self.stats.increment_pool_stale_pick_total();
                            debug!(
                                conn_id,
                                writer_id = w.id,
                                writer_generation = w.generation,
                                current_generation = self.current_generation(),
                                "Selected stale ME writer for fallback bind"
                            );
                        }
                        return Ok(());
                    }
                    Err(TrySendError::Full(_)) => {
                        if fallback_blocking_idx.is_none() {
                            fallback_blocking_idx = Some(idx);
                        }
                    }
                    Err(TrySendError::Closed(_)) => {
                        warn!(writer_id = w.id, "ME writer channel closed");
                        self.remove_writer_and_close_clients(w.id).await;
                        continue;
                    }
                }
            }

            let Some(blocking_idx) = fallback_blocking_idx else {
                continue;
            };

            let w = writers_snapshot[blocking_idx].clone();
            if !self.writer_accepts_new_binding(&w) {
                continue;
            }
            match w.tx.send(WriterCommand::Data(payload.clone())).await {
                Ok(()) => {
                    self.registry
                        .bind_writer(conn_id, w.id, w.tx.clone(), meta.clone())
                        .await;
                    if w.generation < self.current_generation() {
                        self.stats.increment_pool_stale_pick_total();
                    }
                    return Ok(());
                }
                Err(_) => {
                    warn!(writer_id = w.id, "ME writer channel closed (blocking)");
                    self.remove_writer_and_close_clients(w.id).await;
                }
            }
        }
    }

    pub async fn send_close(self: &Arc<Self>, conn_id: u64) -> Result<()> {
        if let Some(w) = self.registry.get_writer(conn_id).await {
            let mut p = Vec::with_capacity(12);
            p.extend_from_slice(&RPC_CLOSE_EXT_U32.to_le_bytes());
            p.extend_from_slice(&conn_id.to_le_bytes());
            if w.tx.send(WriterCommand::DataAndFlush(p)).await.is_err() {
                debug!("ME close write failed");
                self.remove_writer_and_close_clients(w.writer_id).await;
            }
        } else {
            debug!(conn_id, "ME close skipped (writer missing)");
        }

        self.registry.unregister(conn_id).await;
        Ok(())
    }

    pub fn connection_count(&self) -> usize {
        self.conn_count.load(Ordering::Relaxed)
    }
    
    pub(super) async fn candidate_indices_for_dc(
        &self,
        writers: &[super::pool::MeWriter],
        target_dc: i16,
        include_warm: bool,
    ) -> Vec<usize> {
        let key = target_dc as i32;
        let mut preferred = Vec::<SocketAddr>::new();

        for family in self.family_order() {
            let map_guard = match family {
                IpFamily::V4 => self.proxy_map_v4.read().await,
                IpFamily::V6 => self.proxy_map_v6.read().await,
            };

            if let Some(v) = map_guard.get(&key) {
                preferred.extend(v.iter().map(|(ip, port)| SocketAddr::new(*ip, *port)));
            }
            if preferred.is_empty() {
                let abs = key.abs();
                if let Some(v) = map_guard.get(&abs) {
                    preferred.extend(v.iter().map(|(ip, port)| SocketAddr::new(*ip, *port)));
                }
            }
            if preferred.is_empty() {
                let abs = key.abs();
                if let Some(v) = map_guard.get(&-abs) {
                    preferred.extend(v.iter().map(|(ip, port)| SocketAddr::new(*ip, *port)));
                }
            }
            if preferred.is_empty() {
                let def = self.default_dc.load(Ordering::Relaxed);
                if def != 0
                    && let Some(v) = map_guard.get(&def)
                {
                    preferred.extend(v.iter().map(|(ip, port)| SocketAddr::new(*ip, *port)));
                }
            }

            drop(map_guard);

            if !preferred.is_empty() && !self.decision.effective_multipath {
                break;
            }
        }

        if preferred.is_empty() {
            return (0..writers.len())
                .filter(|i| self.writer_eligible_for_selection(&writers[*i], include_warm))
                .collect();
        }

        let mut out = Vec::new();
        for (idx, w) in writers.iter().enumerate() {
            if !self.writer_eligible_for_selection(w, include_warm) {
                continue;
            }
            if preferred.contains(&w.addr) {
                out.push(idx);
            }
        }
        if out.is_empty() {
            return (0..writers.len())
                .filter(|i| self.writer_eligible_for_selection(&writers[*i], include_warm))
                .collect();
        }
        out
    }

    fn writer_eligible_for_selection(
        &self,
        writer: &super::pool::MeWriter,
        include_warm: bool,
    ) -> bool {
        if !self.writer_accepts_new_binding(writer) {
            return false;
        }

        match WriterContour::from_u8(writer.contour.load(Ordering::Relaxed)) {
            WriterContour::Active => true,
            WriterContour::Warm => include_warm,
            WriterContour::Draining => true,
        }
    }

    fn writer_contour_rank_for_selection(&self, writer: &super::pool::MeWriter) -> usize {
        match WriterContour::from_u8(writer.contour.load(Ordering::Relaxed)) {
            WriterContour::Active => 0,
            WriterContour::Warm => 1,
            WriterContour::Draining => 2,
        }
    }

    fn writer_idle_rank_for_selection(
        &self,
        writer: &super::pool::MeWriter,
        idle_since_by_writer: &HashMap<u64, u64>,
        now_epoch_secs: u64,
    ) -> usize {
        let Some(idle_since) = idle_since_by_writer.get(&writer.id).copied() else {
            return 0;
        };
        let idle_age_secs = now_epoch_secs.saturating_sub(idle_since);
        if idle_age_secs >= IDLE_WRITER_PENALTY_HIGH_SECS {
            2
        } else if idle_age_secs >= IDLE_WRITER_PENALTY_MID_SECS {
            1
        } else {
            0
        }
    }
}
