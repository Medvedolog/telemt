use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, warn};

use crate::config::MeRouteNoWriterMode;
use crate::error::{ProxyError, Result};
use crate::network::IpFamily;
use crate::protocol::constants::{RPC_CLOSE_CONN_U32, RPC_CLOSE_EXT_U32};

use super::MePool;
use super::codec::WriterCommand;
use super::pool::WriterContour;
use super::wire::build_proxy_req_payload;
use rand::seq::SliceRandom;
use super::registry::ConnMeta;

const IDLE_WRITER_PENALTY_MID_SECS: u64 = 45;
const IDLE_WRITER_PENALTY_HIGH_SECS: u64 = 55;
const HYBRID_GLOBAL_BURST_PERIOD_ROUNDS: u32 = 4;

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
        let no_writer_mode =
            MeRouteNoWriterMode::from_u8(self.me_route_no_writer_mode.load(Ordering::Relaxed));
        let mut no_writer_deadline: Option<Instant> = None;
        let mut emergency_attempts = 0u32;
        let mut async_recovery_triggered = false;
        let mut hybrid_recovery_round = 0u32;
        let mut hybrid_last_recovery_at: Option<Instant> = None;
        let hybrid_wait_step = self.me_route_no_writer_wait.max(Duration::from_millis(50));

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
                    drop(ws);
                    match no_writer_mode {
                        MeRouteNoWriterMode::AsyncRecoveryFailfast => {
                            let deadline = *no_writer_deadline.get_or_insert_with(|| {
                                Instant::now() + self.me_route_no_writer_wait
                            });
                            if !async_recovery_triggered {
                                let triggered =
                                    self.trigger_async_recovery_for_target_dc(target_dc).await;
                                if !triggered {
                                    self.trigger_async_recovery_global().await;
                                }
                                async_recovery_triggered = true;
                            }
                            if self.wait_for_writer_until(deadline).await {
                                continue;
                            }
                            self.stats.increment_me_no_writer_failfast_total();
                            return Err(ProxyError::Proxy(
                                "No ME writer available in failfast window".into(),
                            ));
                        }
                        MeRouteNoWriterMode::InlineRecoveryLegacy => {
                            self.stats.increment_me_inline_recovery_total();
                            for _ in 0..self.me_route_inline_recovery_attempts.max(1) {
                                for family in self.family_order() {
                                    let map = match family {
                                        IpFamily::V4 => self.proxy_map_v4.read().await.clone(),
                                        IpFamily::V6 => self.proxy_map_v6.read().await.clone(),
                                    };
                                    for (_dc, addrs) in &map {
                                        for (ip, port) in addrs {
                                            let addr = SocketAddr::new(*ip, *port);
                                            let _ = self.connect_one(addr, self.rng.as_ref()).await;
                                        }
                                    }
                                }
                                if !self.writers.read().await.is_empty() {
                                    break;
                                }
                            }
                            if !self.writers.read().await.is_empty() {
                                continue;
                            }
                            let waiter = self.writer_available.notified();
                            if tokio::time::timeout(self.me_route_inline_recovery_wait, waiter)
                                .await
                                .is_err()
                            {
                                if !self.writers.read().await.is_empty() {
                                    continue;
                                }
                                self.stats.increment_me_no_writer_failfast_total();
                                return Err(ProxyError::Proxy(
                                    "All ME connections dead (legacy wait timeout)".into(),
                                ));
                            }
                            continue;
                        }
                        MeRouteNoWriterMode::HybridAsyncPersistent => {
                            self.maybe_trigger_hybrid_recovery(
                                target_dc,
                                &mut hybrid_recovery_round,
                                &mut hybrid_last_recovery_at,
                                hybrid_wait_step,
                            )
                            .await;
                            let deadline = Instant::now() + hybrid_wait_step;
                            let _ = self.wait_for_writer_until(deadline).await;
                            continue;
                        }
                    }
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
                match no_writer_mode {
                    MeRouteNoWriterMode::AsyncRecoveryFailfast => {
                        let deadline = *no_writer_deadline.get_or_insert_with(|| {
                            Instant::now() + self.me_route_no_writer_wait
                        });
                        if !async_recovery_triggered {
                            let triggered = self.trigger_async_recovery_for_target_dc(target_dc).await;
                            if !triggered {
                                self.trigger_async_recovery_global().await;
                            }
                            async_recovery_triggered = true;
                        }
                        if self.wait_for_candidate_until(target_dc, deadline).await {
                            continue;
                        }
                        self.stats.increment_me_no_writer_failfast_total();
                        return Err(ProxyError::Proxy(
                            "No ME writers available for target DC in failfast window".into(),
                        ));
                    }
                    MeRouteNoWriterMode::InlineRecoveryLegacy => {
                        self.stats.increment_me_inline_recovery_total();
                        if emergency_attempts >= self.me_route_inline_recovery_attempts.max(1) {
                            self.stats.increment_me_no_writer_failfast_total();
                            return Err(ProxyError::Proxy("No ME writers available for target DC".into()));
                        }
                        emergency_attempts += 1;
                        let mut endpoints = self.endpoint_candidates_for_target_dc(target_dc).await;
                        endpoints.shuffle(&mut rand::rng());
                        for addr in endpoints {
                            if self.connect_one(addr, self.rng.as_ref()).await.is_ok() {
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(100 * emergency_attempts as u64)).await;
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
                        if candidate_indices.is_empty() {
                            return Err(ProxyError::Proxy("No ME writers available for target DC".into()));
                        }
                    }
                    MeRouteNoWriterMode::HybridAsyncPersistent => {
                        self.maybe_trigger_hybrid_recovery(
                            target_dc,
                            &mut hybrid_recovery_round,
                            &mut hybrid_last_recovery_at,
                            hybrid_wait_step,
                        )
                        .await;
                        let deadline = Instant::now() + hybrid_wait_step;
                        let _ = self.wait_for_candidate_until(target_dc, deadline).await;
                        continue;
                    }
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

    async fn wait_for_writer_until(&self, deadline: Instant) -> bool {
        let waiter = self.writer_available.notified();
        if !self.writers.read().await.is_empty() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return !self.writers.read().await.is_empty();
        }
        let timeout = deadline.saturating_duration_since(now);
        if tokio::time::timeout(timeout, waiter).await.is_ok() {
            return true;
        }
        !self.writers.read().await.is_empty()
    }

    async fn wait_for_candidate_until(&self, target_dc: i16, deadline: Instant) -> bool {
        loop {
            if self.has_candidate_for_target_dc(target_dc).await {
                return true;
            }

            let now = Instant::now();
            if now >= deadline {
                return self.has_candidate_for_target_dc(target_dc).await;
            }

            let remaining = deadline.saturating_duration_since(now);
            let sleep_for = remaining.min(Duration::from_millis(25));
            let waiter = self.writer_available.notified();
            tokio::select! {
                _ = waiter => {}
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
    }

    async fn has_candidate_for_target_dc(&self, target_dc: i16) -> bool {
        let writers_snapshot = {
            let ws = self.writers.read().await;
            if ws.is_empty() {
                return false;
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
        !candidate_indices.is_empty()
    }

    async fn trigger_async_recovery_for_target_dc(self: &Arc<Self>, target_dc: i16) -> bool {
        let endpoints = self.endpoint_candidates_for_target_dc(target_dc).await;
        if endpoints.is_empty() {
            return false;
        }
        self.stats.increment_me_async_recovery_trigger_total();
        for addr in endpoints.into_iter().take(8) {
            self.trigger_immediate_refill(addr);
        }
        true
    }

    async fn trigger_async_recovery_global(self: &Arc<Self>) {
        self.stats.increment_me_async_recovery_trigger_total();
        let mut seen = HashSet::<SocketAddr>::new();
        for family in self.family_order() {
            let map = match family {
                IpFamily::V4 => self.proxy_map_v4.read().await.clone(),
                IpFamily::V6 => self.proxy_map_v6.read().await.clone(),
            };
            for addrs in map.values() {
                for (ip, port) in addrs {
                    let addr = SocketAddr::new(*ip, *port);
                    if seen.insert(addr) {
                        self.trigger_immediate_refill(addr);
                    }
                    if seen.len() >= 8 {
                        return;
                    }
                }
            }
        }
    }

    async fn endpoint_candidates_for_target_dc(&self, target_dc: i16) -> Vec<SocketAddr> {
        let key = target_dc as i32;
        let mut preferred = Vec::<SocketAddr>::new();
        let mut seen = HashSet::<SocketAddr>::new();
        let lookup_keys = self.dc_lookup_chain_for_target(key);

        for family in self.family_order() {
            let map = match family {
                IpFamily::V4 => self.proxy_map_v4.read().await.clone(),
                IpFamily::V6 => self.proxy_map_v6.read().await.clone(),
            };
            let mut family_selected = Vec::<SocketAddr>::new();
            for lookup in lookup_keys.iter().copied() {
                if let Some(addrs) = map.get(&lookup) {
                    for (ip, port) in addrs {
                        family_selected.push(SocketAddr::new(*ip, *port));
                    }
                }
                if !family_selected.is_empty() {
                    break;
                }
            }
            for addr in family_selected {
                if seen.insert(addr) {
                    preferred.push(addr);
                }
            }
            if !preferred.is_empty() && !self.decision.effective_multipath {
                break;
            }
        }

        preferred
    }

    async fn maybe_trigger_hybrid_recovery(
        self: &Arc<Self>,
        target_dc: i16,
        hybrid_recovery_round: &mut u32,
        hybrid_last_recovery_at: &mut Option<Instant>,
        hybrid_wait_step: Duration,
    ) {
        if let Some(last) = *hybrid_last_recovery_at
            && last.elapsed() < hybrid_wait_step
        {
            return;
        }

        let round = *hybrid_recovery_round;
        let target_triggered = self.trigger_async_recovery_for_target_dc(target_dc).await;
        if !target_triggered || round % HYBRID_GLOBAL_BURST_PERIOD_ROUNDS == 0 {
            self.trigger_async_recovery_global().await;
        }
        *hybrid_recovery_round = round.saturating_add(1);
        *hybrid_last_recovery_at = Some(Instant::now());
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

    pub async fn send_close_conn(self: &Arc<Self>, conn_id: u64) -> Result<()> {
        if let Some(w) = self.registry.get_writer(conn_id).await {
            let mut p = Vec::with_capacity(12);
            p.extend_from_slice(&RPC_CLOSE_CONN_U32.to_le_bytes());
            p.extend_from_slice(&conn_id.to_le_bytes());
            match w.tx.try_send(WriterCommand::DataAndFlush(p)) {
                Ok(()) => {}
                Err(TrySendError::Full(cmd)) => {
                    let _ = tokio::time::timeout(Duration::from_millis(50), w.tx.send(cmd)).await;
                }
                Err(TrySendError::Closed(_)) => {
                    debug!(conn_id, "ME close_conn skipped: writer channel closed");
                }
            }
        } else {
            debug!(conn_id, "ME close_conn skipped (writer missing)");
        }

        self.registry.unregister(conn_id).await;
        Ok(())
    }

    pub async fn shutdown_send_close_conn_all(self: &Arc<Self>) -> usize {
        let conn_ids = self.registry.active_conn_ids().await;
        let total = conn_ids.len();
        for conn_id in conn_ids {
            let _ = self.send_close_conn(conn_id).await;
        }
        total
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
        let lookup_keys = self.dc_lookup_chain_for_target(key);

        for family in self.family_order() {
            let map_guard = match family {
                IpFamily::V4 => self.proxy_map_v4.read().await,
                IpFamily::V6 => self.proxy_map_v6.read().await,
            };
            let mut family_selected = Vec::<SocketAddr>::new();
            for lookup in lookup_keys.iter().copied() {
                if let Some(v) = map_guard.get(&lookup) {
                    family_selected.extend(v.iter().map(|(ip, port)| SocketAddr::new(*ip, *port)));
                }
                if !family_selected.is_empty() {
                    break;
                }
            }
            preferred.extend(family_selected);

            drop(map_guard);

            if !preferred.is_empty() && !self.decision.effective_multipath {
                break;
            }
        }

        if preferred.is_empty() {
            return Vec::new();
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
