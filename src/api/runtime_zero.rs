use std::sync::atomic::Ordering;

use serde::Serialize;

use crate::config::{MeFloorMode, ProxyConfig, UserMaxUniqueIpsMode};

use super::ApiShared;

#[derive(Serialize)]
pub(super) struct SystemInfoData {
    pub(super) version: String,
    pub(super) target_arch: String,
    pub(super) target_os: String,
    pub(super) build_profile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) git_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) build_time_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) rustc_version: Option<String>,
    pub(super) process_started_at_epoch_secs: u64,
    pub(super) uptime_seconds: f64,
    pub(super) config_path: String,
    pub(super) config_hash: String,
    pub(super) config_reload_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_config_reload_epoch_secs: Option<u64>,
}

#[derive(Serialize)]
pub(super) struct RuntimeGatesData {
    pub(super) accepting_new_connections: bool,
    pub(super) conditional_cast_enabled: bool,
    pub(super) me_runtime_ready: bool,
    pub(super) me2dc_fallback_enabled: bool,
    pub(super) use_middle_proxy: bool,
}

#[derive(Serialize)]
pub(super) struct EffectiveTimeoutLimits {
    pub(super) client_handshake_secs: u64,
    pub(super) tg_connect_secs: u64,
    pub(super) client_keepalive_secs: u64,
    pub(super) client_ack_secs: u64,
    pub(super) me_one_retry: u8,
    pub(super) me_one_timeout_ms: u64,
}

#[derive(Serialize)]
pub(super) struct EffectiveUpstreamLimits {
    pub(super) connect_retry_attempts: u32,
    pub(super) connect_retry_backoff_ms: u64,
    pub(super) connect_budget_ms: u64,
    pub(super) unhealthy_fail_threshold: u32,
    pub(super) connect_failfast_hard_errors: bool,
}

#[derive(Serialize)]
pub(super) struct EffectiveMiddleProxyLimits {
    pub(super) floor_mode: &'static str,
    pub(super) adaptive_floor_idle_secs: u64,
    pub(super) adaptive_floor_min_writers_single_endpoint: u8,
    pub(super) adaptive_floor_recover_grace_secs: u64,
    pub(super) reconnect_max_concurrent_per_dc: u32,
    pub(super) reconnect_backoff_base_ms: u64,
    pub(super) reconnect_backoff_cap_ms: u64,
    pub(super) reconnect_fast_retry_count: u32,
    pub(super) me2dc_fallback: bool,
}

#[derive(Serialize)]
pub(super) struct EffectiveUserIpPolicyLimits {
    pub(super) mode: &'static str,
    pub(super) window_secs: u64,
}

#[derive(Serialize)]
pub(super) struct EffectiveLimitsData {
    pub(super) update_every_secs: u64,
    pub(super) me_reinit_every_secs: u64,
    pub(super) me_pool_force_close_secs: u64,
    pub(super) timeouts: EffectiveTimeoutLimits,
    pub(super) upstream: EffectiveUpstreamLimits,
    pub(super) middle_proxy: EffectiveMiddleProxyLimits,
    pub(super) user_ip_policy: EffectiveUserIpPolicyLimits,
}

#[derive(Serialize)]
pub(super) struct SecurityPostureData {
    pub(super) api_read_only: bool,
    pub(super) api_whitelist_enabled: bool,
    pub(super) api_whitelist_entries: usize,
    pub(super) api_auth_header_enabled: bool,
    pub(super) proxy_protocol_enabled: bool,
    pub(super) log_level: String,
    pub(super) telemetry_core_enabled: bool,
    pub(super) telemetry_user_enabled: bool,
    pub(super) telemetry_me_level: String,
}

pub(super) fn build_system_info_data(
    shared: &ApiShared,
    _cfg: &ProxyConfig,
    revision: &str,
) -> SystemInfoData {
    let last_reload_epoch_secs = shared
        .runtime_state
        .last_config_reload_epoch_secs
        .load(Ordering::Relaxed);
    let last_config_reload_epoch_secs = (last_reload_epoch_secs > 0).then_some(last_reload_epoch_secs);

    let git_commit = option_env!("TELEMT_GIT_COMMIT")
        .or(option_env!("VERGEN_GIT_SHA"))
        .or(option_env!("GIT_COMMIT"))
        .map(ToString::to_string);
    let build_time_utc = option_env!("BUILD_TIME_UTC")
        .or(option_env!("VERGEN_BUILD_TIMESTAMP"))
        .map(ToString::to_string);
    let rustc_version = option_env!("RUSTC_VERSION")
        .or(option_env!("VERGEN_RUSTC_SEMVER"))
        .map(ToString::to_string);

    SystemInfoData {
        version: env!("CARGO_PKG_VERSION").to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        target_os: std::env::consts::OS.to_string(),
        build_profile: option_env!("PROFILE").unwrap_or("unknown").to_string(),
        git_commit,
        build_time_utc,
        rustc_version,
        process_started_at_epoch_secs: shared.runtime_state.process_started_at_epoch_secs,
        uptime_seconds: shared.stats.uptime_secs(),
        config_path: shared.config_path.display().to_string(),
        config_hash: revision.to_string(),
        config_reload_count: shared.runtime_state.config_reload_count.load(Ordering::Relaxed),
        last_config_reload_epoch_secs,
    }
}

pub(super) fn build_runtime_gates_data(shared: &ApiShared, cfg: &ProxyConfig) -> RuntimeGatesData {
    let me_runtime_ready = if !cfg.general.use_middle_proxy {
        true
    } else {
        shared
            .me_pool
            .as_ref()
            .map(|pool| pool.is_runtime_ready())
            .unwrap_or(false)
    };

    RuntimeGatesData {
        accepting_new_connections: shared.runtime_state.admission_open.load(Ordering::Relaxed),
        conditional_cast_enabled: cfg.general.use_middle_proxy,
        me_runtime_ready,
        me2dc_fallback_enabled: cfg.general.me2dc_fallback,
        use_middle_proxy: cfg.general.use_middle_proxy,
    }
}

pub(super) fn build_limits_effective_data(cfg: &ProxyConfig) -> EffectiveLimitsData {
    EffectiveLimitsData {
        update_every_secs: cfg.general.effective_update_every_secs(),
        me_reinit_every_secs: cfg.general.effective_me_reinit_every_secs(),
        me_pool_force_close_secs: cfg.general.effective_me_pool_force_close_secs(),
        timeouts: EffectiveTimeoutLimits {
            client_handshake_secs: cfg.timeouts.client_handshake,
            tg_connect_secs: cfg.timeouts.tg_connect,
            client_keepalive_secs: cfg.timeouts.client_keepalive,
            client_ack_secs: cfg.timeouts.client_ack,
            me_one_retry: cfg.timeouts.me_one_retry,
            me_one_timeout_ms: cfg.timeouts.me_one_timeout_ms,
        },
        upstream: EffectiveUpstreamLimits {
            connect_retry_attempts: cfg.general.upstream_connect_retry_attempts,
            connect_retry_backoff_ms: cfg.general.upstream_connect_retry_backoff_ms,
            connect_budget_ms: cfg.general.upstream_connect_budget_ms,
            unhealthy_fail_threshold: cfg.general.upstream_unhealthy_fail_threshold,
            connect_failfast_hard_errors: cfg.general.upstream_connect_failfast_hard_errors,
        },
        middle_proxy: EffectiveMiddleProxyLimits {
            floor_mode: me_floor_mode_label(cfg.general.me_floor_mode),
            adaptive_floor_idle_secs: cfg.general.me_adaptive_floor_idle_secs,
            adaptive_floor_min_writers_single_endpoint: cfg
                .general
                .me_adaptive_floor_min_writers_single_endpoint,
            adaptive_floor_recover_grace_secs: cfg.general.me_adaptive_floor_recover_grace_secs,
            reconnect_max_concurrent_per_dc: cfg.general.me_reconnect_max_concurrent_per_dc,
            reconnect_backoff_base_ms: cfg.general.me_reconnect_backoff_base_ms,
            reconnect_backoff_cap_ms: cfg.general.me_reconnect_backoff_cap_ms,
            reconnect_fast_retry_count: cfg.general.me_reconnect_fast_retry_count,
            me2dc_fallback: cfg.general.me2dc_fallback,
        },
        user_ip_policy: EffectiveUserIpPolicyLimits {
            mode: user_max_unique_ips_mode_label(cfg.access.user_max_unique_ips_mode),
            window_secs: cfg.access.user_max_unique_ips_window_secs,
        },
    }
}

pub(super) fn build_security_posture_data(cfg: &ProxyConfig) -> SecurityPostureData {
    SecurityPostureData {
        api_read_only: cfg.server.api.read_only,
        api_whitelist_enabled: !cfg.server.api.whitelist.is_empty(),
        api_whitelist_entries: cfg.server.api.whitelist.len(),
        api_auth_header_enabled: !cfg.server.api.auth_header.is_empty(),
        proxy_protocol_enabled: cfg.server.proxy_protocol,
        log_level: cfg.general.log_level.to_string(),
        telemetry_core_enabled: cfg.general.telemetry.core_enabled,
        telemetry_user_enabled: cfg.general.telemetry.user_enabled,
        telemetry_me_level: cfg.general.telemetry.me_level.to_string(),
    }
}

fn user_max_unique_ips_mode_label(mode: UserMaxUniqueIpsMode) -> &'static str {
    match mode {
        UserMaxUniqueIpsMode::ActiveWindow => "active_window",
        UserMaxUniqueIpsMode::TimeWindow => "time_window",
        UserMaxUniqueIpsMode::Combined => "combined",
    }
}

fn me_floor_mode_label(mode: MeFloorMode) -> &'static str {
    match mode {
        MeFloorMode::Static => "static",
        MeFloorMode::Adaptive => "adaptive",
    }
}
