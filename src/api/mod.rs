use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::AUTHORIZATION;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};
use tracing::{debug, info, warn};

use crate::config::ProxyConfig;
use crate::ip_tracker::UserIpTracker;
use crate::stats::Stats;
use crate::transport::middle_proxy::MePool;
use crate::transport::UpstreamManager;

mod config_store;
mod model;
mod runtime_stats;
mod users;

use config_store::{current_revision, parse_if_match};
use model::{
    ApiFailure, CreateUserRequest, ErrorBody, ErrorResponse, HealthData, PatchUserRequest,
    RotateSecretRequest, SuccessResponse, SummaryData,
};
use runtime_stats::{
    MinimalCacheEntry, build_dcs_data, build_me_writers_data, build_minimal_all_data,
    build_upstreams_data, build_zero_all_data,
};
use users::{create_user, delete_user, patch_user, rotate_secret, users_from_config};

#[derive(Clone)]
pub(super) struct ApiShared {
    pub(super) stats: Arc<Stats>,
    pub(super) ip_tracker: Arc<UserIpTracker>,
    pub(super) me_pool: Option<Arc<MePool>>,
    pub(super) upstream_manager: Arc<UpstreamManager>,
    pub(super) config_path: PathBuf,
    pub(super) startup_detected_ip_v4: Option<IpAddr>,
    pub(super) startup_detected_ip_v6: Option<IpAddr>,
    pub(super) mutation_lock: Arc<Mutex<()>>,
    pub(super) minimal_cache: Arc<Mutex<Option<MinimalCacheEntry>>>,
    pub(super) request_id: Arc<AtomicU64>,
}

impl ApiShared {
    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }
}

pub async fn serve(
    listen: SocketAddr,
    stats: Arc<Stats>,
    ip_tracker: Arc<UserIpTracker>,
    me_pool: Option<Arc<MePool>>,
    upstream_manager: Arc<UpstreamManager>,
    config_rx: watch::Receiver<Arc<ProxyConfig>>,
    config_path: PathBuf,
    startup_detected_ip_v4: Option<IpAddr>,
    startup_detected_ip_v6: Option<IpAddr>,
) {
    let listener = match TcpListener::bind(listen).await {
        Ok(listener) => listener,
        Err(error) => {
            warn!(
                error = %error,
                listen = %listen,
                "Failed to bind API listener"
            );
            return;
        }
    };

    info!("API endpoint: http://{}/v1/*", listen);

    let shared = Arc::new(ApiShared {
        stats,
        ip_tracker,
        me_pool,
        upstream_manager,
        config_path,
        startup_detected_ip_v4,
        startup_detected_ip_v6,
        mutation_lock: Arc::new(Mutex::new(())),
        minimal_cache: Arc::new(Mutex::new(None)),
        request_id: Arc::new(AtomicU64::new(1)),
    });

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(error) => {
                warn!(error = %error, "API accept error");
                continue;
            }
        };

        let shared_conn = shared.clone();
        let config_rx_conn = config_rx.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<Incoming>| {
                let shared_req = shared_conn.clone();
                let config_rx_req = config_rx_conn.clone();
                async move { handle(req, peer, shared_req, config_rx_req).await }
            });
            if let Err(error) = http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                .await
            {
                debug!(error = %error, "API connection error");
            }
        });
    }
}

async fn handle(
    req: Request<Incoming>,
    peer: SocketAddr,
    shared: Arc<ApiShared>,
    config_rx: watch::Receiver<Arc<ProxyConfig>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let request_id = shared.next_request_id();
    let cfg = config_rx.borrow().clone();
    let api_cfg = &cfg.server.api;

    if !api_cfg.enabled {
        return Ok(error_response(
            request_id,
            ApiFailure::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "api_disabled",
                "API is disabled",
            ),
        ));
    }

    if !api_cfg.whitelist.is_empty()
        && !api_cfg
            .whitelist
            .iter()
            .any(|net| net.contains(peer.ip()))
    {
        return Ok(error_response(
            request_id,
            ApiFailure::new(StatusCode::FORBIDDEN, "forbidden", "Source IP is not allowed"),
        ));
    }

    if !api_cfg.auth_header.is_empty() {
        let auth_ok = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == api_cfg.auth_header)
            .unwrap_or(false);
        if !auth_ok {
            return Ok(error_response(
                request_id,
                ApiFailure::new(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "Missing or invalid Authorization header",
                ),
            ));
        }
    }

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let body_limit = api_cfg.request_body_limit_bytes;

    let result: Result<Response<Full<Bytes>>, ApiFailure> = async {
        match (method.as_str(), path.as_str()) {
            ("GET", "/v1/health") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = HealthData {
                    status: "ok",
                    read_only: api_cfg.read_only,
                };
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/summary") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = SummaryData {
                    uptime_seconds: shared.stats.uptime_secs(),
                    connections_total: shared.stats.get_connects_all(),
                    connections_bad_total: shared.stats.get_connects_bad(),
                    handshake_timeouts_total: shared.stats.get_handshake_timeouts(),
                    configured_users: cfg.access.users.len(),
                };
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/zero/all") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = build_zero_all_data(&shared.stats, cfg.access.users.len());
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/upstreams") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = build_upstreams_data(shared.as_ref(), api_cfg);
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/minimal/all") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = build_minimal_all_data(shared.as_ref(), api_cfg).await;
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/me-writers") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = build_me_writers_data(shared.as_ref(), api_cfg).await;
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/dcs") => {
                let revision = current_revision(&shared.config_path).await?;
                let data = build_dcs_data(shared.as_ref(), api_cfg).await;
                Ok(success_response(StatusCode::OK, data, revision))
            }
            ("GET", "/v1/stats/users") | ("GET", "/v1/users") => {
                let revision = current_revision(&shared.config_path).await?;
                let users = users_from_config(
                    &cfg,
                    &shared.stats,
                    &shared.ip_tracker,
                    shared.startup_detected_ip_v4,
                    shared.startup_detected_ip_v6,
                )
                .await;
                Ok(success_response(StatusCode::OK, users, revision))
            }
            ("POST", "/v1/users") => {
                if api_cfg.read_only {
                    return Ok(error_response(
                        request_id,
                        ApiFailure::new(
                            StatusCode::FORBIDDEN,
                            "read_only",
                            "API runs in read-only mode",
                        ),
                    ));
                }
                let expected_revision = parse_if_match(req.headers());
                let body = read_json::<CreateUserRequest>(req.into_body(), body_limit).await?;
                let (data, revision) = create_user(body, expected_revision, &shared).await?;
                Ok(success_response(StatusCode::CREATED, data, revision))
            }
            _ => {
                if let Some(user) = path.strip_prefix("/v1/users/")
                    && !user.is_empty()
                    && !user.contains('/')
                {
                    if method == Method::GET {
                        let revision = current_revision(&shared.config_path).await?;
                        let users = users_from_config(
                            &cfg,
                            &shared.stats,
                            &shared.ip_tracker,
                            shared.startup_detected_ip_v4,
                            shared.startup_detected_ip_v6,
                        )
                        .await;
                        if let Some(user_info) = users.into_iter().find(|entry| entry.username == user)
                        {
                            return Ok(success_response(StatusCode::OK, user_info, revision));
                        }
                        return Ok(error_response(
                            request_id,
                            ApiFailure::new(StatusCode::NOT_FOUND, "not_found", "User not found"),
                        ));
                    }
                    if method == Method::PATCH {
                        if api_cfg.read_only {
                            return Ok(error_response(
                                request_id,
                                ApiFailure::new(
                                    StatusCode::FORBIDDEN,
                                    "read_only",
                                    "API runs in read-only mode",
                                ),
                            ));
                        }
                        let expected_revision = parse_if_match(req.headers());
                        let body = read_json::<PatchUserRequest>(req.into_body(), body_limit).await?;
                        let (data, revision) =
                            patch_user(user, body, expected_revision, &shared).await?;
                        return Ok(success_response(StatusCode::OK, data, revision));
                    }
                    if method == Method::DELETE {
                        if api_cfg.read_only {
                            return Ok(error_response(
                                request_id,
                                ApiFailure::new(
                                    StatusCode::FORBIDDEN,
                                    "read_only",
                                    "API runs in read-only mode",
                                ),
                            ));
                        }
                        let expected_revision = parse_if_match(req.headers());
                        let (deleted_user, revision) =
                            delete_user(user, expected_revision, &shared).await?;
                        return Ok(success_response(StatusCode::OK, deleted_user, revision));
                    }
                    if method == Method::POST
                        && let Some(base_user) = user.strip_suffix("/rotate-secret")
                        && !base_user.is_empty()
                        && !base_user.contains('/')
                    {
                        if api_cfg.read_only {
                            return Ok(error_response(
                                request_id,
                                ApiFailure::new(
                                    StatusCode::FORBIDDEN,
                                    "read_only",
                                    "API runs in read-only mode",
                                ),
                            ));
                        }
                        let expected_revision = parse_if_match(req.headers());
                        let body =
                            read_optional_json::<RotateSecretRequest>(req.into_body(), body_limit)
                                .await?;
                        let (data, revision) =
                            rotate_secret(base_user, body.unwrap_or_default(), expected_revision, &shared)
                                .await?;
                        return Ok(success_response(StatusCode::OK, data, revision));
                    }
                    if method == Method::POST {
                        return Ok(error_response(
                            request_id,
                            ApiFailure::new(StatusCode::NOT_FOUND, "not_found", "Route not found"),
                        ));
                    }
                    return Ok(error_response(
                        request_id,
                        ApiFailure::new(
                            StatusCode::METHOD_NOT_ALLOWED,
                            "method_not_allowed",
                            "Unsupported HTTP method for this route",
                        ),
                    ));
                }
                Ok(error_response(
                    request_id,
                    ApiFailure::new(StatusCode::NOT_FOUND, "not_found", "Route not found"),
                ))
            }
        }
    }
    .await;

    match result {
        Ok(resp) => Ok(resp),
        Err(error) => Ok(error_response(request_id, error)),
    }
}

fn success_response<T: Serialize>(
    status: StatusCode,
    data: T,
    revision: String,
) -> Response<Full<Bytes>> {
    let payload = SuccessResponse {
        ok: true,
        data,
        revision,
    };
    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{\"ok\":false}".to_vec());
    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn error_response(request_id: u64, failure: ApiFailure) -> Response<Full<Bytes>> {
    let payload = ErrorResponse {
        ok: false,
        error: ErrorBody {
            code: failure.code,
            message: failure.message,
        },
        request_id,
    };
    let body = serde_json::to_vec(&payload).unwrap_or_else(|_| {
        format!(
            "{{\"ok\":false,\"error\":{{\"code\":\"internal_error\",\"message\":\"serialization failed\"}},\"request_id\":{}}}",
            request_id
        )
        .into_bytes()
    });
    Response::builder()
        .status(failure.status)
        .header("content-type", "application/json; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

async fn read_json<T: DeserializeOwned>(body: Incoming, limit: usize) -> Result<T, ApiFailure> {
    let bytes = read_body_with_limit(body, limit).await?;
    serde_json::from_slice(&bytes).map_err(|_| ApiFailure::bad_request("Invalid JSON body"))
}

async fn read_optional_json<T: DeserializeOwned>(
    body: Incoming,
    limit: usize,
) -> Result<Option<T>, ApiFailure> {
    let bytes = read_body_with_limit(body, limit).await?;
    if bytes.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| ApiFailure::bad_request("Invalid JSON body"))
}

async fn read_body_with_limit(body: Incoming, limit: usize) -> Result<Vec<u8>, ApiFailure> {
    let mut collected = Vec::new();
    let mut body = body;
    while let Some(frame_result) = body.frame().await {
        let frame = frame_result.map_err(|_| ApiFailure::bad_request("Invalid request body"))?;
        if let Some(chunk) = frame.data_ref() {
            if collected.len().saturating_add(chunk.len()) > limit {
                return Err(ApiFailure::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    format!("Body exceeds {} bytes", limit),
                ));
            }
            collected.extend_from_slice(chunk);
        }
    }
    Ok(collected)
}
