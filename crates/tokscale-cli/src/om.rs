use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tokscale_core::{
    build_open_mercato_hourly_buckets, open_mercato_bucket_payload_hash,
    parse_local_open_mercato_data, LocalParseOptions, OpenMercatoHourlyBucket,
    OpenMercatoSourceSnapshot,
};
use uuid::Uuid;

const CURRENT_STATE_SCHEMA_VERSION: u32 = 2;
const DEFAULT_UPLOAD_PATH: &str = "/api/ai-usage/collector/v1/ingest";
const DEFAULT_CHANNEL: &str = "stable";
const DEFAULT_SCHEMA_VERSION: &str = "v1";
const DEFAULT_BATCH_STATUS_PENDING: &str = "pending";
const DEFAULT_BATCH_STATUS_RETRY_WAIT: &str = "retry_wait";
const DEFAULT_BATCH_STATUS_ACKED: &str = "acked";
const DEFAULT_BATCH_STATUS_BLOCKED: &str = "blocked";
const DEFAULT_BATCH_SIZE: usize = 250;
const DEFAULT_ACK_RETENTION_DAYS: i64 = 30;
const DEFAULT_TOKEN_SERVICE: &str = "com.openmercato.tokscale-om";
const DEFAULT_TOKEN_FALLBACK_FILENAME: &str = "device-token";
const DEFAULT_LAUNCHD_LABEL: &str = "io.openmercato.tokscale-om";
const DEFAULT_SYSTEMD_UNIT: &str = "tokscale-om.service";
const DEFAULT_SYSTEMD_TIMER: &str = "tokscale-om.timer";
const OM_V1_CLIENTS: &[&str] = &["codex", "claude"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmConfig {
    pub server_url: String,
    pub device_fingerprint: String,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_label_strategy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_token_ref: Option<String>,
    #[serde(default, skip_serializing, rename = "deviceToken")]
    pub legacy_device_token: Option<String>,
    #[serde(default)]
    pub scan: OmScanConfig,
    #[serde(default)]
    pub upload: OmUploadConfig,
    #[serde(default)]
    pub schedule: OmScheduleConfig,
    #[serde(default)]
    pub log: OmLogConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmScanConfig {
    #[serde(default)]
    pub extra_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmUploadConfig {
    pub interval_minutes: u32,
}

impl Default for OmUploadConfig {
    fn default() -> Self {
        Self {
            interval_minutes: 1440,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmScheduleConfig {
    pub daily_hour_local: u8,
    pub daily_minute_local: u8,
    pub weekdays_only: bool,
}

impl Default for OmScheduleConfig {
    fn default() -> Self {
        Self {
            daily_hour_local: 13,
            daily_minute_local: 0,
            weekdays_only: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OmLogConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectorState {
    #[serde(default = "current_state_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub device: DeviceState,
    #[serde(default)]
    pub sources: BTreeMap<String, SourceState>,
    #[serde(default)]
    pub finalized_buckets: BTreeMap<String, FinalizedBucketState>,
    #[serde(default)]
    pub upload_ledger: BTreeMap<String, UploadBatchState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_scan_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scan_duration_ms: Option<u64>,
}

impl Default for CollectorState {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_STATE_SCHEMA_VERSION,
            device: DeviceState::default(),
            sources: BTreeMap::new(),
            finalized_buckets: BTreeMap::new(),
            upload_ledger: BTreeMap::new(),
            last_successful_scan_time: None,
            last_scan_duration_ms: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_binding_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collector_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_path_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_consumed_offset: Option<u64>,
    #[serde(default)]
    pub parser_state: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_scan_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinalizedBucketState {
    pub bucket_key: String,
    pub payload_hash: String,
    pub finalized_at: String,
    pub acknowledged: bool,
    #[serde(default)]
    pub blocked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_membership: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_ack_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub bucket: OpenMercatoHourlyBucket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadBatchState {
    pub collector_batch_id: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<String>,
    pub ack_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_ack_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_batch_id: Option<String>,
    pub retry_count: u32,
    pub bucket_keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadBatchRequest {
    schema_version: &'static str,
    collector_batch_id: String,
    collector_version: String,
    device_fingerprint: String,
    generated_at_utc: String,
    window_start_utc: String,
    window_end_utc: String,
    buckets: Vec<UploadHourlyBucket>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadHourlyBucket {
    hour_start_utc: String,
    source_client: String,
    provider_id: String,
    model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_name: Option<String>,
    workspace_fingerprint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace_label: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    reasoning_tokens: i64,
    message_count: i32,
    turn_count: i32,
    source_session_count: i32,
    estimated_usd: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadBatchResponse {
    status: String,
    #[serde(default)]
    batch_id: Option<String>,
    #[serde(default)]
    server_batch_id: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    details: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSummary {
    pub source_count: usize,
    pub finalized_bucket_count: usize,
    pub pending_batch_count: usize,
    pub acked_batch_count: usize,
    pub blocked_batch_count: usize,
    pub uploaded_batch_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServiceStatus {
    installed: bool,
    active: bool,
    kind: &'static str,
    location: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct PendingUpload {
    batch_id: String,
    bucket_keys: Vec<String>,
    buckets: Vec<OpenMercatoHourlyBucket>,
}

fn current_state_schema_version() -> u32 {
    CURRENT_STATE_SCHEMA_VERSION
}

pub fn run_configure(
    server_url: String,
    device_token: String,
    channel: Option<String>,
    device_fingerprint: Option<String>,
    daily_hour_local: Option<u8>,
    daily_minute_local: Option<u8>,
    all_days: bool,
) -> Result<()> {
    let device_fingerprint = device_fingerprint.unwrap_or_else(generate_device_fingerprint);
    let device_token_ref = default_token_ref(&device_fingerprint);
    store_device_token(&device_token_ref, &device_token)?;
    let mut schedule = OmScheduleConfig::default();
    if let Some(hour) = daily_hour_local {
        if hour > 23 {
            return Err(anyhow!("--daily-hour-local must be between 0 and 23"));
        }
        schedule.daily_hour_local = hour;
    }
    if let Some(minute) = daily_minute_local {
        if minute > 59 {
            return Err(anyhow!("--daily-minute-local must be between 0 and 59"));
        }
        schedule.daily_minute_local = minute;
    }
    if all_days {
        schedule.weekdays_only = false;
    }

    let config = OmConfig {
        server_url: normalize_server_url(&server_url),
        device_fingerprint,
        channel: channel.unwrap_or_else(|| DEFAULT_CHANNEL.to_string()),
        workspace_label_strategy: None,
        device_token_ref: Some(device_token_ref),
        legacy_device_token: None,
        scan: OmScanConfig::default(),
        upload: OmUploadConfig::default(),
        schedule,
        log: OmLogConfig::default(),
    };

    save_config(&config)?;

    println!("\n  {}\n", "Open Mercato collector configured.".green());
    println!(
        "{}",
        format!("  Config: {}", config_path()?.display()).bright_black()
    );
    println!(
        "{}",
        format!("  Server: {}", config.server_url).bright_black()
    );
    println!(
        "{}",
        format!("  Channel: {}", config.channel).bright_black()
    );
    println!(
        "{}",
        format!(
            "  Schedule: {:02}:{:02} local{}",
            config.schedule.daily_hour_local,
            config.schedule.daily_minute_local,
            if config.schedule.weekdays_only {
                " on weekdays"
            } else {
                " daily"
            }
        )
        .bright_black()
    );
    println!(
        "{}",
        format!("  Device fingerprint: {}", config.device_fingerprint).bright_black()
    );
    println!(
        "{}",
        "  Device token: stored outside config file".bright_black()
    );
    println!();

    Ok(())
}

pub fn run_status() -> Result<()> {
    let config = load_config()?;
    let state = load_state().unwrap_or_default();
    let service = detect_service_status()?;

    let pending_batches = state
        .upload_ledger
        .values()
        .filter(|batch| {
            batch.ack_status == DEFAULT_BATCH_STATUS_PENDING
                || batch.ack_status == DEFAULT_BATCH_STATUS_RETRY_WAIT
        })
        .count();
    let acked_batches = state
        .upload_ledger
        .values()
        .filter(|batch| batch.ack_status == DEFAULT_BATCH_STATUS_ACKED)
        .count();
    let blocked_batches = state
        .upload_ledger
        .values()
        .filter(|batch| batch.ack_status == DEFAULT_BATCH_STATUS_BLOCKED)
        .count();
    let next_retry = state
        .upload_ledger
        .values()
        .filter(|batch| batch.ack_status == DEFAULT_BATCH_STATUS_RETRY_WAIT)
        .filter_map(|batch| batch.next_attempt_at.as_deref())
        .min()
        .map(str::to_string);

    println!("\n  {}\n", "Open Mercato collector status".cyan());
    println!(
        "{}",
        format!("  Config: {}", config_path()?.display()).bright_black()
    );
    println!(
        "{}",
        format!("  State:  {}", state_path()?.display()).bright_black()
    );
    println!(
        "{}",
        format!("  Server: {}", config.server_url).bright_black()
    );
    println!(
        "{}",
        format!("  Upload URL: {}", upload_url(&config)).bright_black()
    );
    println!(
        "{}",
        format!("  Channel: {}", config.channel).bright_black()
    );
    println!(
        "{}",
        format!(
            "  Schedule: {:02}:{:02} local{}",
            config.schedule.daily_hour_local,
            config.schedule.daily_minute_local,
            if config.schedule.weekdays_only {
                " on weekdays"
            } else {
                " daily"
            }
        )
        .bright_black()
    );
    println!(
        "{}",
        format!("  Device fingerprint: {}", config.device_fingerprint).bright_black()
    );
    println!(
        "{}",
        format!("  Sources tracked: {}", state.sources.len()).bright_black()
    );
    println!(
        "{}",
        format!("  Finalized buckets: {}", state.finalized_buckets.len()).bright_black()
    );
    println!(
        "{}",
        format!("  Pending batches: {}", pending_batches).bright_black()
    );
    println!(
        "{}",
        format!("  Acked batches: {}", acked_batches).bright_black()
    );
    println!(
        "{}",
        format!("  Blocked batches: {}", blocked_batches).bright_black()
    );
    if let Some(last_scan) = state.last_successful_scan_time.as_deref() {
        println!("{}", format!("  Last scan: {}", last_scan).bright_black());
    }
    if let Some(duration_ms) = state.last_scan_duration_ms {
        println!(
            "{}",
            format!("  Last scan duration: {} ms", duration_ms).bright_black()
        );
    }
    if let Some(next_retry) = next_retry {
        println!("{}", format!("  Next retry: {}", next_retry).bright_black());
    }
    println!(
        "{}",
        format!(
            "  Service: {} ({})",
            if service.active {
                "active"
            } else if service.installed {
                "installed"
            } else {
                "not installed"
            },
            service.kind
        )
        .bright_black()
    );
    if let Some(location) = service.location {
        println!(
            "{}",
            format!("  Service file: {}", location.display()).bright_black()
        );
    }
    if let Some(last_batch) = state.upload_ledger.values().last() {
        println!(
            "{}",
            format!(
                "  Last batch: {} ({})",
                last_batch.collector_batch_id, last_batch.ack_status
            )
            .bright_black()
        );
    }
    println!();

    Ok(())
}

pub async fn run_sync(
    home_dir: Option<String>,
    clients: Option<Vec<String>>,
    dry_run: bool,
) -> Result<()> {
    let summary = sync_once(home_dir, clients, dry_run, false).await?;
    println!(
        "{}",
        format!(
            "  Uploaded batches this run: {}",
            summary.uploaded_batch_count
        )
        .bright_black()
    );
    println!();
    Ok(())
}

pub async fn run_daemon(
    home_dir: Option<String>,
    clients: Option<Vec<String>>,
    dry_run: bool,
    max_cycles: Option<usize>,
    interval_seconds_override: Option<u64>,
) -> Result<()> {
    let config = load_config()?;
    let interval = interval_seconds_override
        .unwrap_or_else(|| u64::from(config.upload.interval_minutes.max(1)).saturating_mul(60));

    println!("\n  {}\n", "Open Mercato collector daemon".cyan());
    println!(
        "{}",
        format!("  Interval: {} seconds", interval).bright_black()
    );
    if dry_run {
        println!("{}", "  Dry run mode enabled.".yellow());
    }
    println!();

    daemon_loop(home_dir, clients, dry_run, interval, max_cycles).await
}

pub fn run_install_service() -> Result<()> {
    let config = load_config()?;
    let exe = std::env::current_exe().context("Could not resolve current executable")?;
    let command = scheduled_exec_command(&exe);

    match std::env::consts::OS {
        "macos" => install_launchd_service(&command, &config.schedule),
        "linux" => install_systemd_service(&command, &config.schedule),
        "windows" => Err(anyhow!(
            "Windows does not have a built-in service installer in V1. Run `tokscale om daemon` manually."
        )),
        other => Err(anyhow!("Unsupported service platform: {other}")),
    }
}

pub fn run_uninstall_service() -> Result<()> {
    match std::env::consts::OS {
        "macos" => uninstall_launchd_service(),
        "linux" => uninstall_systemd_service(),
        "windows" => Err(anyhow!(
            "Windows does not have a built-in service installer in V1. Stop the manual daemon process."
        )),
        other => Err(anyhow!("Unsupported service platform: {other}")),
    }
}

pub fn run_retry_blocked(batch_id: Option<String>) -> Result<()> {
    let mut state = load_state().unwrap_or_default();
    let mut touched_batches = 0usize;
    let target_batch = batch_id.as_deref();

    for batch in state.upload_ledger.values_mut() {
        if batch.ack_status != DEFAULT_BATCH_STATUS_BLOCKED {
            continue;
        }
        if target_batch.is_some() && target_batch != Some(batch.collector_batch_id.as_str()) {
            continue;
        }

        batch.ack_status = DEFAULT_BATCH_STATUS_PENDING.to_string();
        batch.next_attempt_at = None;
        batch.last_error = None;
        batch.retry_count = 0;
        touched_batches = touched_batches.saturating_add(1);

        for bucket_key in &batch.bucket_keys {
            if let Some(bucket) = state.finalized_buckets.get_mut(bucket_key) {
                bucket.blocked = false;
                bucket.blocked_at = None;
                bucket.last_error = None;
                if bucket.batch_membership.is_none() {
                    bucket.batch_membership = Some(batch.collector_batch_id.clone());
                }
            }
        }
    }

    if touched_batches == 0 {
        println!("{}", "  No blocked batches matched.\n".yellow());
        return Ok(());
    }

    save_state(&state)?;
    println!(
        "\n  {}\n",
        format!("Re-queued {} blocked batch(es).", touched_batches).green()
    );
    Ok(())
}

async fn daemon_loop(
    home_dir: Option<String>,
    clients: Option<Vec<String>>,
    dry_run: bool,
    interval_seconds: u64,
    max_cycles: Option<usize>,
) -> Result<()> {
    let sleep_duration = Duration::from_secs(interval_seconds.max(1));
    let mut cycle = 0usize;

    loop {
        cycle = cycle.saturating_add(1);
        let started_at = Utc::now();

        if let Err(err) = sync_once(home_dir.clone(), clients.clone(), dry_run, true).await {
            eprintln!("\n  {}\n", format!("Daemon sync failed: {err}").red());
        }

        if max_cycles.is_some_and(|max| cycle >= max) {
            return Ok(());
        }

        let next_run = started_at + ChronoDuration::seconds(sleep_duration.as_secs() as i64);
        println!(
            "{}",
            format!("  Next daemon sync after {}.", next_run.to_rfc3339()).bright_black()
        );
        println!();
        tokio::time::sleep(sleep_duration).await;
    }
}

async fn sync_once(
    home_dir: Option<String>,
    clients: Option<Vec<String>>,
    dry_run: bool,
    quiet: bool,
) -> Result<SyncSummary> {
    let config = load_config()?;
    let now = Utc::now();
    let started_at = std::time::Instant::now();
    let selected_clients = clients.unwrap_or_else(default_v1_clients);
    let mut state = load_state().unwrap_or_default();
    let scanner_settings = build_scanner_settings(&config, &selected_clients);

    if !quiet {
        println!("\n  {}\n", "Open Mercato collector sync".cyan());
        println!("{}", "  Scanning local usage sources...".bright_black());
    }

    let parse_result = parse_local_open_mercato_data(LocalParseOptions {
        home_dir,
        use_env_roots: true,
        clients: Some(selected_clients),
        since: None,
        until: None,
        year: None,
        scanner_settings,
    })
    .await
    .map_err(|err| anyhow!(err))?;

    state.device = DeviceState {
        device_id: None,
        device_fingerprint: Some(config.device_fingerprint.clone()),
        user_binding_id: None,
        collector_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        channel: Some(config.channel.clone()),
    };
    state.last_successful_scan_time = Some(now.to_rfc3339());
    state.last_scan_duration_ms = Some(started_at.elapsed().as_millis() as u64);
    upsert_sources(&mut state, &parse_result.source_snapshots, now);

    let buckets = build_open_mercato_hourly_buckets(parse_result.messages, now);
    upsert_finalized_buckets(&mut state, &buckets, now);
    prune_state(&mut state, now);
    let created_batches = create_pending_batches(&mut state, &config.device_fingerprint, now);
    let pending_uploads = pending_uploads(&state, now);
    save_state(&state)?;

    let pending_batch_count = count_batches_with_status(&state, DEFAULT_BATCH_STATUS_PENDING)
        + count_batches_with_status(&state, DEFAULT_BATCH_STATUS_RETRY_WAIT);
    let acked_batch_count = count_batches_with_status(&state, DEFAULT_BATCH_STATUS_ACKED);
    let blocked_batch_count = count_batches_with_status(&state, DEFAULT_BATCH_STATUS_BLOCKED);

    if !quiet {
        println!(
            "{}",
            format!("  Sources tracked: {}", state.sources.len()).bright_black()
        );
        println!(
            "{}",
            format!(
                "  Finalized closed buckets: {}",
                state.finalized_buckets.len()
            )
            .bright_black()
        );
        println!(
            "{}",
            format!("  New pending batches: {}", created_batches).bright_black()
        );
        println!(
            "{}",
            format!("  Upload-ready batches: {}", pending_uploads.len()).bright_black()
        );
        println!();
    }

    let mut uploaded_batch_count = 0usize;
    if dry_run {
        if !quiet {
            println!(
                "{}",
                "  Dry run - not uploading to Open Mercato.\n".yellow()
            );
        }
    } else {
        let device_token = load_device_token(&config)?;
        for upload in pending_uploads {
            if !quiet {
                println!(
                    "{}",
                    format!("  Uploading batch {}...", upload.batch_id).bright_black()
                );
            }

            let buckets = apply_workspace_label_strategy(
                upload.buckets.clone(),
                config.workspace_label_strategy.as_deref(),
            );
            let (window_start_utc, window_end_utc) = batch_window_bounds(&buckets)?;
            let request = UploadBatchRequest {
                schema_version: DEFAULT_SCHEMA_VERSION,
                collector_batch_id: upload.batch_id.clone(),
                collector_version: env!("CARGO_PKG_VERSION").to_string(),
                device_fingerprint: config.device_fingerprint.clone(),
                generated_at_utc: now.to_rfc3339(),
                window_start_utc,
                window_end_utc,
                buckets: upload_bucket_payloads(buckets),
            };

            let response = reqwest::Client::new()
                .post(upload_url(&config))
                .bearer_auth(&device_token)
                .json(&request)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status_code = resp.status();
                    let parsed = resp.json::<UploadBatchResponse>().await.ok();
                    if let Err(err) = handle_upload_response(
                        &mut state,
                        &upload.batch_id,
                        &upload.bucket_keys,
                        status_code,
                        parsed,
                    ) {
                        save_state(&state)?;
                        return Err(err);
                    }
                    uploaded_batch_count = uploaded_batch_count.saturating_add(1);
                }
                Err(err) => {
                    mark_batch_retry(
                        &mut state,
                        &upload.batch_id,
                        &upload.bucket_keys,
                        &err.to_string(),
                        now,
                    )?;
                }
            }
        }
        save_state(&state)?;

        if uploaded_batch_count > 0 && !quiet {
            println!("\n  {}\n", "Upload cycle finished.".green());
        }
    }

    Ok(SyncSummary {
        source_count: state.sources.len(),
        finalized_bucket_count: state.finalized_buckets.len(),
        pending_batch_count,
        acked_batch_count,
        blocked_batch_count,
        uploaded_batch_count,
    })
}

fn build_scanner_settings(
    config: &OmConfig,
    selected_clients: &[String],
) -> tokscale_core::scanner::ScannerSettings {
    let mut scanner_settings = crate::tui::settings::load_scanner_settings();
    for extra_path in &config.scan.extra_paths {
        let path = PathBuf::from(extra_path);
        for client in selected_clients {
            scanner_settings
                .extra_scan_paths
                .entry(client.clone())
                .or_default()
                .push(path.clone());
        }
    }
    scanner_settings
}

fn upsert_sources(
    state: &mut CollectorState,
    source_snapshots: &[OpenMercatoSourceSnapshot],
    now: DateTime<Utc>,
) {
    let seen_keys: BTreeSet<String> = source_snapshots
        .iter()
        .map(|snapshot| snapshot.source_key.clone())
        .collect();

    for snapshot in source_snapshots {
        state.sources.insert(
            snapshot.source_key.clone(),
            SourceState {
                source_type: Some(snapshot.source_type.clone()),
                canonical_path_hash: Some(snapshot.canonical_path_hash.clone()),
                last_consumed_offset: snapshot.last_consumed_offset,
                parser_state: snapshot.parser_state.clone(),
                last_successful_scan_time: Some(now.to_rfc3339()),
            },
        );
    }

    state.sources.retain(|key, _| seen_keys.contains(key));
}

fn upsert_finalized_buckets(
    state: &mut CollectorState,
    buckets: &[OpenMercatoHourlyBucket],
    now: DateTime<Utc>,
) {
    for bucket in buckets {
        let payload_hash = open_mercato_bucket_payload_hash(bucket);
        let entry = state
            .finalized_buckets
            .entry(bucket.bucket_key.clone())
            .or_insert_with(|| FinalizedBucketState {
                bucket_key: bucket.bucket_key.clone(),
                payload_hash: payload_hash.clone(),
                finalized_at: now.to_rfc3339(),
                acknowledged: false,
                blocked: false,
                acknowledged_at: None,
                blocked_at: None,
                batch_membership: None,
                server_ack_status: None,
                last_error: None,
                bucket: bucket.clone(),
            });

        if entry.payload_hash != payload_hash {
            entry.payload_hash = payload_hash;
            entry.acknowledged = false;
            entry.blocked = false;
            entry.acknowledged_at = None;
            entry.blocked_at = None;
            entry.batch_membership = None;
            entry.server_ack_status = None;
            entry.last_error = None;
        }

        entry.finalized_at = now.to_rfc3339();
        entry.bucket = bucket.clone();
    }
}

fn create_pending_batches(
    state: &mut CollectorState,
    device_fingerprint: &str,
    now: DateTime<Utc>,
) -> usize {
    let mut unbatched: Vec<String> = state
        .finalized_buckets
        .iter()
        .filter(|(_, bucket)| {
            !bucket.acknowledged && !bucket.blocked && bucket.batch_membership.is_none()
        })
        .map(|(bucket_key, _)| bucket_key.clone())
        .collect();
    unbatched.sort_unstable();

    let mut created = 0usize;
    for chunk in unbatched.chunks(DEFAULT_BATCH_SIZE) {
        let buckets: Vec<OpenMercatoHourlyBucket> = chunk
            .iter()
            .filter_map(|bucket_key| {
                state
                    .finalized_buckets
                    .get(bucket_key)
                    .map(|bucket| bucket.bucket.clone())
            })
            .collect();
        if buckets.is_empty() {
            continue;
        }

        let batch_id = stable_batch_id(device_fingerprint, &buckets);
        let batch_entry = state
            .upload_ledger
            .entry(batch_id.clone())
            .or_insert_with(|| UploadBatchState {
                collector_batch_id: batch_id.clone(),
                created_at: now.to_rfc3339(),
                sent_at: None,
                ack_status: DEFAULT_BATCH_STATUS_PENDING.to_string(),
                server_ack_status: None,
                server_batch_id: None,
                retry_count: 0,
                bucket_keys: chunk.to_vec(),
                last_error: None,
                next_attempt_at: None,
            });
        batch_entry.bucket_keys = chunk.to_vec();
        if batch_entry.ack_status == DEFAULT_BATCH_STATUS_ACKED {
            continue;
        }
        batch_entry.ack_status = DEFAULT_BATCH_STATUS_PENDING.to_string();
        batch_entry.next_attempt_at = None;

        for bucket_key in chunk {
            if let Some(bucket) = state.finalized_buckets.get_mut(bucket_key) {
                bucket.batch_membership = Some(batch_id.clone());
            }
        }
        created = created.saturating_add(1);
    }

    created
}

fn pending_uploads(state: &CollectorState, now: DateTime<Utc>) -> Vec<PendingUpload> {
    let mut uploads = Vec::new();

    for batch in state.upload_ledger.values() {
        let is_due = match batch.ack_status.as_str() {
            DEFAULT_BATCH_STATUS_PENDING => true,
            DEFAULT_BATCH_STATUS_RETRY_WAIT => batch
                .next_attempt_at
                .as_deref()
                .and_then(parse_rfc3339_utc)
                .is_some_and(|next| next <= now),
            _ => false,
        };
        if !is_due {
            continue;
        }

        let mut bucket_keys = Vec::new();
        let mut buckets = Vec::new();
        let mut batch_invalid = false;

        for bucket_key in &batch.bucket_keys {
            let Some(bucket) = state.finalized_buckets.get(bucket_key) else {
                batch_invalid = true;
                break;
            };
            if bucket.acknowledged || bucket.blocked {
                batch_invalid = true;
                break;
            }
            if bucket.batch_membership.as_deref() != Some(batch.collector_batch_id.as_str()) {
                batch_invalid = true;
                break;
            }
            bucket_keys.push(bucket_key.clone());
            buckets.push(bucket.bucket.clone());
        }

        if batch_invalid || buckets.is_empty() {
            continue;
        }

        uploads.push(PendingUpload {
            batch_id: batch.collector_batch_id.clone(),
            bucket_keys,
            buckets,
        });
    }

    uploads.sort_by(|left, right| left.batch_id.cmp(&right.batch_id));
    uploads
}

fn handle_upload_response(
    state: &mut CollectorState,
    batch_id: &str,
    bucket_keys: &[String],
    status_code: reqwest::StatusCode,
    response: Option<UploadBatchResponse>,
) -> Result<()> {
    let sent_at = Utc::now();
    if status_code.is_success() {
        let ack_status = response
            .as_ref()
            .map(|body| body.status.clone())
            .unwrap_or_else(|| "accepted".to_string());
        if ack_status != "accepted" && ack_status != "duplicate" {
            mark_batch_blocked(
                state,
                batch_id,
                bucket_keys,
                &format!("Unexpected OM ack status: {ack_status}"),
                Some(ack_status),
                sent_at,
            )?;
            return Err(anyhow!("Open Mercato returned unexpected ack status"));
        }

        let server_batch_id = response.as_ref().and_then(|body| {
            body.batch_id
                .clone()
                .or_else(|| body.server_batch_id.clone())
        });
        let batch = state
            .upload_ledger
            .get_mut(batch_id)
            .ok_or_else(|| anyhow!("Missing upload ledger entry for batch {batch_id}"))?;
        batch.sent_at = Some(sent_at.to_rfc3339());
        batch.ack_status = DEFAULT_BATCH_STATUS_ACKED.to_string();
        batch.server_ack_status = Some(ack_status.clone());
        batch.server_batch_id = server_batch_id;
        batch.last_error = None;
        batch.next_attempt_at = None;

        for key in bucket_keys {
            if let Some(bucket) = state.finalized_buckets.get_mut(key) {
                bucket.acknowledged = true;
                bucket.blocked = false;
                bucket.acknowledged_at = Some(sent_at.to_rfc3339());
                bucket.blocked_at = None;
                bucket.batch_membership = Some(batch_id.to_string());
                bucket.server_ack_status = Some(ack_status.clone());
                bucket.last_error = None;
            }
        }
        return Ok(());
    }

    let detail_lines = response
        .as_ref()
        .and_then(|body| body.details.clone())
        .unwrap_or_default();
    let error = response
        .as_ref()
        .and_then(|body| body.error.clone())
        .unwrap_or_else(|| format!("Open Mercato returned {}", status_code));
    let server_ack_status = response.as_ref().map(|body| body.status.clone());

    if status_code.is_client_error() {
        let mut message = format!("Upload rejected: {error}");
        if !detail_lines.is_empty() {
            message.push_str(&format!(" ({})", detail_lines.join("; ")));
        }
        mark_batch_blocked(
            state,
            batch_id,
            bucket_keys,
            &message,
            server_ack_status,
            sent_at,
        )?;
        return Err(anyhow!(message));
    }

    mark_batch_retry(state, batch_id, bucket_keys, &error, sent_at)?;
    Err(anyhow!("Upload failed: {error}"))
}

fn mark_batch_retry(
    state: &mut CollectorState,
    batch_id: &str,
    bucket_keys: &[String],
    error: &str,
    sent_at: DateTime<Utc>,
) -> Result<()> {
    let batch = state
        .upload_ledger
        .get_mut(batch_id)
        .ok_or_else(|| anyhow!("Missing upload ledger entry for batch {batch_id}"))?;
    batch.sent_at = Some(sent_at.to_rfc3339());
    batch.retry_count = batch.retry_count.saturating_add(1);
    batch.ack_status = DEFAULT_BATCH_STATUS_RETRY_WAIT.to_string();
    batch.last_error = Some(error.to_string());
    batch.next_attempt_at = Some(compute_backoff_time(sent_at, batch.retry_count).to_rfc3339());

    for bucket_key in bucket_keys {
        if let Some(bucket) = state.finalized_buckets.get_mut(bucket_key) {
            bucket.last_error = Some(error.to_string());
        }
    }

    Ok(())
}

fn mark_batch_blocked(
    state: &mut CollectorState,
    batch_id: &str,
    bucket_keys: &[String],
    error: &str,
    server_ack_status: Option<String>,
    sent_at: DateTime<Utc>,
) -> Result<()> {
    let batch = state
        .upload_ledger
        .get_mut(batch_id)
        .ok_or_else(|| anyhow!("Missing upload ledger entry for batch {batch_id}"))?;
    batch.sent_at = Some(sent_at.to_rfc3339());
    batch.retry_count = batch.retry_count.saturating_add(1);
    batch.ack_status = DEFAULT_BATCH_STATUS_BLOCKED.to_string();
    batch.server_ack_status = server_ack_status;
    batch.last_error = Some(error.to_string());
    batch.next_attempt_at = None;

    for bucket_key in bucket_keys {
        if let Some(bucket) = state.finalized_buckets.get_mut(bucket_key) {
            bucket.blocked = true;
            bucket.blocked_at = Some(sent_at.to_rfc3339());
            bucket.last_error = Some(error.to_string());
            bucket.server_ack_status = Some(DEFAULT_BATCH_STATUS_BLOCKED.to_string());
            bucket.batch_membership = Some(batch_id.to_string());
        }
    }

    Ok(())
}

fn prune_state(state: &mut CollectorState, now: DateTime<Utc>) {
    let cutoff = now - ChronoDuration::days(DEFAULT_ACK_RETENTION_DAYS);
    let mut removable_batches = BTreeSet::new();

    state.finalized_buckets.retain(|_, bucket| {
        if !(bucket.acknowledged && !bucket.blocked) {
            return true;
        }
        let acknowledged_at = bucket
            .acknowledged_at
            .as_deref()
            .and_then(parse_rfc3339_utc);
        if acknowledged_at.is_some_and(|value| value < cutoff) {
            if let Some(batch_id) = bucket.batch_membership.as_ref() {
                removable_batches.insert(batch_id.clone());
            }
            return false;
        }
        true
    });

    state.upload_ledger.retain(|batch_id, batch| {
        if batch.ack_status != DEFAULT_BATCH_STATUS_ACKED {
            return true;
        }
        let sent_at = batch.sent_at.as_deref().and_then(parse_rfc3339_utc);
        if sent_at.is_some_and(|value| value < cutoff) {
            return !removable_batches.contains(batch_id);
        }
        true
    });
}

fn count_batches_with_status(state: &CollectorState, status: &str) -> usize {
    state
        .upload_ledger
        .values()
        .filter(|batch| batch.ack_status == status)
        .count()
}

fn compute_backoff_time(sent_at: DateTime<Utc>, retry_count: u32) -> DateTime<Utc> {
    let minutes = 2_i64
        .saturating_pow(retry_count.saturating_sub(1))
        .clamp(1, 360);
    sent_at + ChronoDuration::minutes(minutes)
}

fn stable_batch_id(device_fingerprint: &str, buckets: &[OpenMercatoHourlyBucket]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(device_fingerprint.as_bytes());
    for bucket in buckets {
        hasher.update(bucket.bucket_key.as_bytes());
        hasher.update(open_mercato_bucket_payload_hash(bucket).as_bytes());
    }
    format!("batch-{:x}", hasher.finalize())
}

fn normalize_server_url(server_url: &str) -> String {
    server_url.trim_end_matches('/').to_string()
}

fn upload_url(config: &OmConfig) -> String {
    match std::env::var("TOKSCALE_OM_UPLOAD_PATH") {
        Ok(value) if value.starts_with("http://") || value.starts_with("https://") => value,
        Ok(value) => format!("{}{}", config.server_url, value),
        Err(_) => format!("{}{}", config.server_url, DEFAULT_UPLOAD_PATH),
    }
}

fn default_v1_clients() -> Vec<String> {
    OM_V1_CLIENTS
        .iter()
        .map(|client| (*client).to_string())
        .collect()
}

fn apply_workspace_label_strategy(
    mut buckets: Vec<OpenMercatoHourlyBucket>,
    strategy: Option<&str>,
) -> Vec<OpenMercatoHourlyBucket> {
    let preserve_labels = matches!(
        strategy.map(str::trim).filter(|value| !value.is_empty()),
        Some("preserve")
    );

    if !preserve_labels {
        for bucket in &mut buckets {
            bucket.workspace_label = None;
        }
    }

    buckets
}

fn upload_bucket_payloads(buckets: Vec<OpenMercatoHourlyBucket>) -> Vec<UploadHourlyBucket> {
    buckets
        .into_iter()
        .map(|bucket| UploadHourlyBucket {
            hour_start_utc: bucket.hour_start_utc,
            source_client: bucket.source_client,
            provider_id: bucket.provider_id,
            model_id: bucket.model_id,
            agent_name: bucket.agent_name,
            workspace_fingerprint: bucket.workspace_fingerprint,
            workspace_label: bucket.workspace_label,
            input_tokens: bucket.input_tokens,
            output_tokens: bucket.output_tokens,
            cache_read_tokens: bucket.cache_read_tokens,
            cache_write_tokens: bucket.cache_write_tokens,
            reasoning_tokens: bucket.reasoning_tokens,
            message_count: bucket.message_count,
            turn_count: bucket.turn_count,
            source_session_count: bucket.source_session_count,
            estimated_usd: bucket.estimated_usd,
        })
        .collect()
}

fn batch_window_bounds(buckets: &[OpenMercatoHourlyBucket]) -> Result<(String, String)> {
    let mut hours = buckets.iter().map(|bucket| bucket.hour_start_utc.as_str());
    let Some(mut min_hour) = hours.next() else {
        return Err(anyhow!("Cannot build OM ingest batch without buckets"));
    };
    let mut max_hour = min_hour;

    for hour in hours {
        if hour < min_hour {
            min_hour = hour;
        }
        if hour > max_hour {
            max_hour = hour;
        }
    }

    Ok((min_hour.to_string(), max_hour.to_string()))
}

fn generate_device_fingerprint() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("device-{}", Uuid::new_v4()));
    format!("{}-tokscale-om", host.replace(' ', "-").to_lowercase())
}

fn default_token_ref(device_fingerprint: &str) -> String {
    format!("device-token:{device_fingerprint}")
}

fn config_root_dir() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("TOKSCALE_OM_CONFIG_DIR") {
        return Ok(PathBuf::from(path));
    }

    let home = dirs::home_dir().context("Could not determine home directory")?;
    Ok(home.join(".config").join("tokscale-om"))
}

fn config_path() -> Result<PathBuf> {
    Ok(config_root_dir()?.join("config.json"))
}

fn state_path() -> Result<PathBuf> {
    Ok(config_root_dir()?.join("state.json"))
}

fn token_fallback_path(token_ref: &str) -> Result<PathBuf> {
    Ok(config_root_dir()?.join(format!(
        "{}-{}",
        DEFAULT_TOKEN_FALLBACK_FILENAME,
        short_hash(token_ref)
    )))
}

fn ensure_root_dir() -> Result<PathBuf> {
    let root = config_root_dir()?;
    if !root.exists() {
        fs::create_dir_all(&root)?;
        set_owner_only_permissions(&root)?;
    }
    Ok(root)
}

fn save_config(config: &OmConfig) -> Result<()> {
    ensure_root_dir()?;
    let mut stored = config.clone();
    stored.legacy_device_token = None;
    write_secure_json(&config_path()?, &stored)
}

fn load_config() -> Result<OmConfig> {
    let path = config_path()?;
    let raw = fs::read_to_string(&path).with_context(|| {
        format!(
            "Open Mercato collector config not found at {}. Run `tokscale om configure` first.",
            path.display()
        )
    })?;
    let mut config: OmConfig = serde_json::from_str(&raw)?;

    if config.device_token_ref.is_none() {
        config.device_token_ref = Some(default_token_ref(&config.device_fingerprint));
    }

    if let Some(legacy_token) = config.legacy_device_token.clone() {
        let token_ref = config
            .device_token_ref
            .clone()
            .unwrap_or_else(|| default_token_ref(&config.device_fingerprint));
        store_device_token(&token_ref, &legacy_token)?;
        config.device_token_ref = Some(token_ref);
        config.legacy_device_token = None;
        save_config(&config)?;
    }

    Ok(config)
}

fn load_device_token(config: &OmConfig) -> Result<String> {
    if let Ok(value) = std::env::var("TOKSCALE_OM_DEVICE_TOKEN") {
        if !value.trim().is_empty() {
            return Ok(value);
        }
    }

    let token_ref = config
        .device_token_ref
        .clone()
        .unwrap_or_else(|| default_token_ref(&config.device_fingerprint));
    load_stored_device_token(&token_ref).with_context(|| {
        format!(
            "No device token available for {}. Re-run `tokscale om configure --device-token ...`.",
            config.device_fingerprint
        )
    })
}

fn save_state(state: &CollectorState) -> Result<()> {
    ensure_root_dir()?;
    write_secure_json(&state_path()?, state)
}

fn load_state() -> Result<CollectorState> {
    let path = state_path()?;
    if !path.exists() {
        return Ok(CollectorState::default());
    }
    let raw = fs::read_to_string(path)?;
    let mut state: CollectorState = serde_json::from_str(&raw)?;
    normalize_loaded_state(&mut state);
    Ok(state)
}

fn normalize_loaded_state(state: &mut CollectorState) {
    if state.schema_version == 0 || state.schema_version < CURRENT_STATE_SCHEMA_VERSION {
        state.schema_version = CURRENT_STATE_SCHEMA_VERSION;
    }

    for batch in state.upload_ledger.values_mut() {
        batch.ack_status =
            normalize_batch_status(&batch.ack_status, batch.server_ack_status.as_deref());
        if batch.ack_status == DEFAULT_BATCH_STATUS_ACKED {
            batch.next_attempt_at = None;
        }
        if batch.ack_status == DEFAULT_BATCH_STATUS_RETRY_WAIT && batch.next_attempt_at.is_none() {
            let sent_at = batch
                .sent_at
                .as_deref()
                .and_then(parse_rfc3339_utc)
                .unwrap_or_else(Utc::now);
            batch.next_attempt_at =
                Some(compute_backoff_time(sent_at, batch.retry_count.max(1)).to_rfc3339());
        }
    }
}

fn normalize_batch_status(status: &str, server_ack_status: Option<&str>) -> String {
    match status {
        "accepted" | "duplicate" | DEFAULT_BATCH_STATUS_ACKED => {
            DEFAULT_BATCH_STATUS_ACKED.to_string()
        }
        DEFAULT_BATCH_STATUS_PENDING => DEFAULT_BATCH_STATUS_PENDING.to_string(),
        DEFAULT_BATCH_STATUS_RETRY_WAIT => DEFAULT_BATCH_STATUS_RETRY_WAIT.to_string(),
        DEFAULT_BATCH_STATUS_BLOCKED => DEFAULT_BATCH_STATUS_BLOCKED.to_string(),
        value if value == "network-error" => DEFAULT_BATCH_STATUS_RETRY_WAIT.to_string(),
        value if value.starts_with("http-4") => DEFAULT_BATCH_STATUS_BLOCKED.to_string(),
        value if value.starts_with("http-5") => DEFAULT_BATCH_STATUS_RETRY_WAIT.to_string(),
        _ => match server_ack_status {
            Some("accepted") | Some("duplicate") => DEFAULT_BATCH_STATUS_ACKED.to_string(),
            _ => DEFAULT_BATCH_STATUS_PENDING.to_string(),
        },
    }
}

fn write_secure_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    write_secure_string(path, &json)
}

fn write_secure_string(path: &Path, value: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(value.as_bytes())?;
    }

    #[cfg(not(unix))]
    {
        fs::write(path, value)?;
    }

    Ok(())
}

fn set_owner_only_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }

    #[cfg(not(unix))]
    {
        let _ = path;
    }

    Ok(())
}

fn keyring_disabled() -> bool {
    std::env::var("TOKSCALE_OM_DISABLE_KEYRING")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn store_device_token(token_ref: &str, token: &str) -> Result<()> {
    ensure_root_dir()?;
    let path = token_fallback_path(token_ref)?;
    write_secure_string(&path, token)?;

    if !keyring_disabled() {
        if let Ok(entry) = keyring::Entry::new(DEFAULT_TOKEN_SERVICE, token_ref) {
            let _ = entry.set_password(token);
        }
    }

    Ok(())
}

fn load_stored_device_token(token_ref: &str) -> Result<String> {
    if !keyring_disabled() {
        if let Ok(entry) = keyring::Entry::new(DEFAULT_TOKEN_SERVICE, token_ref) {
            if let Ok(token) = entry.get_password() {
                if !token.trim().is_empty() {
                    return Ok(token);
                }
            }
        }
    }

    let path = token_fallback_path(token_ref)?;
    let token = fs::read_to_string(&path)
        .with_context(|| format!("Could not load device token from {}", path.display()))?;
    let trimmed = token.trim().to_string();
    if trimmed.is_empty() {
        return Err(anyhow!("Device token is empty"));
    }
    Ok(trimmed)
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn short_hash(value: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())[..12].to_string()
}

fn scheduled_exec_command(exe: &Path) -> Vec<String> {
    vec![
        exe.display().to_string(),
        "--no-spinner".to_string(),
        "om".to_string(),
        "sync".to_string(),
    ]
}

fn detect_service_status() -> Result<ServiceStatus> {
    match std::env::consts::OS {
        "macos" => detect_launchd_status(),
        "linux" => detect_systemd_status(),
        "windows" => Ok(ServiceStatus {
            installed: false,
            active: false,
            kind: "manual-daemon",
            location: None,
        }),
        _ => Ok(ServiceStatus {
            installed: false,
            active: false,
            kind: "unsupported",
            location: None,
        }),
    }
}

fn detect_launchd_status() -> Result<ServiceStatus> {
    let plist_path = launchd_plist_path()?;
    let installed = plist_path.exists();
    let active = if installed {
        let uid = current_uid()?;
        Command::new("launchctl")
            .args(["print", &format!("gui/{uid}/{DEFAULT_LAUNCHD_LABEL}")])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    } else {
        false
    };

    Ok(ServiceStatus {
        installed,
        active,
        kind: "launchd-scheduled",
        location: Some(plist_path),
    })
}

fn detect_systemd_status() -> Result<ServiceStatus> {
    let timer_path = systemd_timer_path()?;
    let installed = timer_path.exists();
    let active = if installed {
        Command::new("systemctl")
            .args(["--user", "is-active", DEFAULT_SYSTEMD_TIMER])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    } else {
        false
    };

    Ok(ServiceStatus {
        installed,
        active,
        kind: "systemd-timer",
        location: Some(timer_path),
    })
}

fn install_launchd_service(command: &[String], schedule: &OmScheduleConfig) -> Result<()> {
    let plist_path = launchd_plist_path()?;
    let parent = plist_path
        .parent()
        .ok_or_else(|| anyhow!("LaunchAgent path has no parent"))?;
    fs::create_dir_all(parent)?;
    write_secure_string(&plist_path, &render_launchd_plist(command, schedule))?;

    let _ = Command::new("launchctl")
        .args(["unload", plist_path.to_str().unwrap_or_default()])
        .output();
    let output = Command::new("launchctl")
        .args(["load", "-w", plist_path.to_str().unwrap_or_default()])
        .output()
        .context("Failed to execute launchctl load")?;
    if !output.status.success() {
        return Err(anyhow!(
            "launchctl load failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    println!("\n  {}\n", "Installed launchd scheduled run.".green());
    println!("{}", format!("  {}", plist_path.display()).bright_black());
    println!();

    Ok(())
}

fn uninstall_launchd_service() -> Result<()> {
    let plist_path = launchd_plist_path()?;
    if plist_path.exists() {
        let _ = Command::new("launchctl")
            .args(["unload", plist_path.to_str().unwrap_or_default()])
            .output();
        fs::remove_file(&plist_path)?;
    }

    println!("\n  {}\n", "Removed launchd service.".green());
    Ok(())
}

fn install_systemd_service(command: &[String], schedule: &OmScheduleConfig) -> Result<()> {
    let unit_path = systemd_unit_path()?;
    let timer_path = systemd_timer_path()?;
    let parent = unit_path
        .parent()
        .ok_or_else(|| anyhow!("systemd unit path has no parent"))?;
    fs::create_dir_all(parent)?;
    write_secure_string(&unit_path, &render_systemd_unit(command))?;
    write_secure_string(&timer_path, &render_systemd_timer(schedule))?;

    run_systemctl_user(["daemon-reload"])?;
    run_systemctl_user(["enable", "--now", DEFAULT_SYSTEMD_TIMER])?;

    println!("\n  {}\n", "Installed systemd user timer.".green());
    println!("{}", format!("  {}", timer_path.display()).bright_black());
    println!();

    Ok(())
}

fn uninstall_systemd_service() -> Result<()> {
    let unit_path = systemd_unit_path()?;
    let timer_path = systemd_timer_path()?;
    let _ = run_systemctl_user(["disable", "--now", DEFAULT_SYSTEMD_TIMER]);
    if unit_path.exists() {
        fs::remove_file(unit_path)?;
    }
    if timer_path.exists() {
        fs::remove_file(timer_path)?;
    }
    let _ = run_systemctl_user(["daemon-reload"]);

    println!("\n  {}\n", "Removed systemd user service.".green());
    Ok(())
}

fn run_systemctl_user<const N: usize>(args: [&str; N]) -> Result<()> {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .with_context(|| format!("Failed to execute `systemctl --user {}`", args.join(" ")))?;
    if !output.status.success() {
        return Err(anyhow!(
            "systemctl --user {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

fn render_launchd_plist(command: &[String], schedule: &OmScheduleConfig) -> String {
    let program_args = command
        .iter()
        .map(|arg| format!("    <string>{}</string>\n", xml_escape(arg)))
        .collect::<String>();
    let schedule_xml = render_launchd_schedule(schedule);
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
            "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
            "<plist version=\"1.0\">\n",
            "<dict>\n",
            "  <key>Label</key>\n",
            "  <string>{label}</string>\n",
            "  <key>ProgramArguments</key>\n",
            "  <array>\n",
            "{program_args}",
            "  </array>\n",
            "{schedule_xml}",
            "  <key>StandardOutPath</key>\n",
            "  <string>{stdout_path}</string>\n",
            "  <key>StandardErrorPath</key>\n",
            "  <string>{stderr_path}</string>\n",
            "</dict>\n",
            "</plist>\n"
        ),
        label = DEFAULT_LAUNCHD_LABEL,
        program_args = program_args,
        schedule_xml = schedule_xml,
        stdout_path = config_root_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("daemon.log")
            .display(),
        stderr_path = config_root_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("daemon.error.log")
            .display(),
    )
}

fn render_systemd_unit(command: &[String]) -> String {
    format!(
        "[Unit]\nDescription=Open Mercato tokscale collector run\nAfter=network-online.target\n\n[Service]\nType=oneshot\nExecStart={}\nWorkingDirectory={}\n",
        shell_join(command),
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .display()
    )
}

fn render_systemd_timer(schedule: &OmScheduleConfig) -> String {
    format!(
        "[Unit]\nDescription=Open Mercato tokscale collector schedule\n\n[Timer]\nOnCalendar={}\nPersistent=true\nUnit={}\n\n[Install]\nWantedBy=timers.target\n",
        systemd_on_calendar(schedule),
        DEFAULT_SYSTEMD_UNIT
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&apos;")
}

fn render_launchd_schedule(schedule: &OmScheduleConfig) -> String {
    let weekdays: Vec<u8> = if schedule.weekdays_only {
        vec![1, 2, 3, 4, 5]
    } else {
        vec![0]
    };

    if weekdays == [0] {
        return format!(
            concat!(
                "  <key>StartCalendarInterval</key>\n",
                "  <dict>\n",
                "    <key>Hour</key>\n",
                "    <integer>{hour}</integer>\n",
                "    <key>Minute</key>\n",
                "    <integer>{minute}</integer>\n",
                "  </dict>\n"
            ),
            hour = schedule.daily_hour_local,
            minute = schedule.daily_minute_local,
        );
    }

    let items = weekdays
        .into_iter()
        .map(|weekday| {
            format!(
                concat!(
                    "    <dict>\n",
                    "      <key>Weekday</key>\n",
                    "      <integer>{weekday}</integer>\n",
                    "      <key>Hour</key>\n",
                    "      <integer>{hour}</integer>\n",
                    "      <key>Minute</key>\n",
                    "      <integer>{minute}</integer>\n",
                    "    </dict>\n"
                ),
                weekday = weekday,
                hour = schedule.daily_hour_local,
                minute = schedule.daily_minute_local,
            )
        })
        .collect::<String>();

    format!("  <key>StartCalendarInterval</key>\n  <array>\n{items}  </array>\n")
}

fn systemd_on_calendar(schedule: &OmScheduleConfig) -> String {
    let prefix = if schedule.weekdays_only {
        "Mon..Fri "
    } else {
        ""
    };
    format!(
        "{prefix}*-*-* {:02}:{:02}:00",
        schedule.daily_hour_local, schedule.daily_minute_local
    )
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg.contains(' ') {
                format!("\"{}\"", arg.replace('\"', "\\\""))
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn launchd_plist_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("Could not determine home directory")?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{DEFAULT_LAUNCHD_LABEL}.plist")))
}

fn systemd_unit_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("Could not determine home directory")?;
    Ok(home
        .join(".config")
        .join("systemd")
        .join("user")
        .join(DEFAULT_SYSTEMD_UNIT))
}

fn systemd_timer_path() -> Result<PathBuf> {
    let home = dirs::home_dir().context("Could not determine home directory")?;
    Ok(home
        .join(".config")
        .join("systemd")
        .join("user")
        .join(DEFAULT_SYSTEMD_TIMER))
}

#[cfg(unix)]
fn current_uid() -> Result<u32> {
    Ok(unsafe { libc::geteuid() })
}

#[cfg(not(unix))]
fn current_uid() -> Result<u32> {
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct EnvGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    fn sample_bucket(bucket_key: &str) -> OpenMercatoHourlyBucket {
        OpenMercatoHourlyBucket {
            bucket_key: bucket_key.to_string(),
            hour_start_utc: "2026-04-13T09:00:00+00:00".to_string(),
            source_client: "codex".to_string(),
            provider_id: "openai".to_string(),
            model_id: "gpt-5-codex".to_string(),
            agent_name: Some("builder".to_string()),
            workspace_fingerprint: "abc".repeat(21) + "a",
            workspace_label: Some("repo-a".to_string()),
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reasoning_tokens: 1,
            message_count: 2,
            turn_count: 1,
            source_session_count: 1,
            estimated_usd: 0.25,
        }
    }

    fn batch_state(batch_id: &str, bucket_keys: Vec<String>) -> UploadBatchState {
        UploadBatchState {
            collector_batch_id: batch_id.to_string(),
            created_at: "2026-04-13T10:00:00Z".to_string(),
            sent_at: None,
            ack_status: DEFAULT_BATCH_STATUS_PENDING.to_string(),
            server_ack_status: None,
            server_batch_id: None,
            retry_count: 0,
            bucket_keys,
            last_error: None,
            next_attempt_at: None,
        }
    }

    fn bucket_state(bucket_key: &str, acknowledged: bool) -> FinalizedBucketState {
        FinalizedBucketState {
            bucket_key: bucket_key.to_string(),
            payload_hash: format!("hash-{bucket_key}"),
            finalized_at: "2026-04-13T10:00:00Z".to_string(),
            acknowledged,
            blocked: false,
            acknowledged_at: acknowledged.then_some("2026-04-13T10:05:00Z".to_string()),
            blocked_at: None,
            batch_membership: None,
            server_ack_status: None,
            last_error: None,
            bucket: sample_bucket(bucket_key),
        }
    }

    #[test]
    #[serial]
    fn config_round_trip_migrates_token_out_of_config() {
        let temp = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set("TOKSCALE_OM_CONFIG_DIR", temp.path().to_str().unwrap());
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");

        let legacy = serde_json::json!({
            "serverUrl": "https://om.example",
            "deviceFingerprint": "device-123",
            "channel": "stable",
            "deviceToken": "secret"
        });
        write_secure_string(&temp.path().join("config.json"), &legacy.to_string()).unwrap();

        let loaded = load_config().unwrap();
        assert_eq!(loaded.server_url, "https://om.example");
        assert!(loaded.legacy_device_token.is_none());
        let stored_config: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(temp.path().join("config.json")).unwrap())
                .unwrap();
        assert!(stored_config.get("deviceToken").is_none());
        assert_eq!(load_device_token(&loaded).unwrap(), "secret");
    }

    #[test]
    #[serial]
    fn configure_persists_custom_daily_schedule() {
        let temp = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set("TOKSCALE_OM_CONFIG_DIR", temp.path().to_str().unwrap());
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");

        run_configure(
            "https://om.example".to_string(),
            "secret".to_string(),
            Some("stable".to_string()),
            Some("device-123".to_string()),
            Some(11),
            Some(30),
            true,
        )
        .unwrap();

        let loaded = load_config().unwrap();
        assert_eq!(loaded.schedule.daily_hour_local, 11);
        assert_eq!(loaded.schedule.daily_minute_local, 30);
        assert!(!loaded.schedule.weekdays_only);
    }

    #[test]
    #[serial]
    fn store_device_token_keeps_fallback_when_keyring_is_enabled() {
        let temp = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set("TOKSCALE_OM_CONFIG_DIR", temp.path().to_str().unwrap());
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "0");

        store_device_token("device-token:device-123", "secret").unwrap();

        let token_file = token_fallback_path("device-token:device-123").unwrap();
        assert_eq!(fs::read_to_string(token_file).unwrap(), "secret");
        assert_eq!(
            load_stored_device_token("device-token:device-123").unwrap(),
            "secret"
        );
    }

    #[test]
    #[serial]
    fn configure_rejects_invalid_daily_schedule() {
        let temp = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set("TOKSCALE_OM_CONFIG_DIR", temp.path().to_str().unwrap());
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");

        let error = run_configure(
            "https://om.example".to_string(),
            "secret".to_string(),
            Some("stable".to_string()),
            Some("device-123".to_string()),
            Some(24),
            Some(0),
            false,
        )
        .unwrap_err();

        assert!(error.to_string().contains("--daily-hour-local"));
    }

    #[test]
    fn stable_batch_id_is_deterministic_for_same_payload() {
        let buckets = vec![sample_bucket("bucket-1"), sample_bucket("bucket-2")];
        let first = stable_batch_id("device-123", &buckets);
        let second = stable_batch_id("device-123", &buckets);
        assert_eq!(first, second);
    }

    #[test]
    fn workspace_labels_are_omitted_by_default() {
        let buckets = apply_workspace_label_strategy(vec![sample_bucket("bucket-1")], None);
        assert!(buckets[0].workspace_label.is_none());
    }

    #[test]
    fn workspace_labels_can_be_preserved_explicitly() {
        let buckets =
            apply_workspace_label_strategy(vec![sample_bucket("bucket-1")], Some("preserve"));
        assert_eq!(buckets[0].workspace_label.as_deref(), Some("repo-a"));
    }

    #[test]
    fn batch_window_bounds_use_min_and_max_bucket_hours() {
        let mut first = sample_bucket("bucket-1");
        first.hour_start_utc = "2026-04-13T07:00:00Z".to_string();
        let mut second = sample_bucket("bucket-2");
        second.hour_start_utc = "2026-04-13T09:00:00Z".to_string();

        let (window_start_utc, window_end_utc) = batch_window_bounds(&[second, first]).unwrap();

        assert_eq!(window_start_utc, "2026-04-13T07:00:00Z");
        assert_eq!(window_end_utc, "2026-04-13T09:00:00Z");
    }

    #[test]
    fn create_pending_batches_splits_deterministically() {
        let mut state = CollectorState::default();
        for index in 0..251 {
            let key = format!("bucket-{index:03}");
            state
                .finalized_buckets
                .insert(key.clone(), bucket_state(&key, false));
        }

        let created = create_pending_batches(&mut state, "device-123", Utc::now());

        assert_eq!(created, 2);
        assert_eq!(state.upload_ledger.len(), 2);
        assert_eq!(
            state
                .upload_ledger
                .values()
                .map(|batch| batch.bucket_keys.len())
                .collect::<Vec<_>>(),
            vec![250, 1]
        );
    }

    #[test]
    fn prune_state_removes_old_acked_entries() {
        let mut state = CollectorState::default();
        let batch_id = "batch-1".to_string();
        let mut bucket = bucket_state("bucket-1", true);
        bucket.batch_membership = Some(batch_id.clone());
        bucket.acknowledged_at = Some("2026-02-01T10:05:00Z".to_string());
        state
            .finalized_buckets
            .insert("bucket-1".to_string(), bucket);
        let mut batch = batch_state(&batch_id, vec!["bucket-1".to_string()]);
        batch.ack_status = DEFAULT_BATCH_STATUS_ACKED.to_string();
        batch.sent_at = Some("2026-02-01T10:05:00Z".to_string());
        state.upload_ledger.insert(batch_id, batch);

        prune_state(
            &mut state,
            DateTime::parse_from_rfc3339("2026-04-13T10:15:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );

        assert!(state.finalized_buckets.is_empty());
        assert!(state.upload_ledger.is_empty());
    }

    #[test]
    fn normalize_state_migrates_old_batch_statuses() {
        let mut state = CollectorState {
            schema_version: 0,
            ..CollectorState::default()
        };
        state.upload_ledger.insert(
            "batch-1".to_string(),
            UploadBatchState {
                collector_batch_id: "batch-1".to_string(),
                created_at: "2026-04-13T10:00:00Z".to_string(),
                sent_at: Some("2026-04-13T10:01:00Z".to_string()),
                ack_status: "http-500".to_string(),
                server_ack_status: None,
                server_batch_id: None,
                retry_count: 1,
                bucket_keys: vec!["bucket-1".to_string()],
                last_error: Some("boom".to_string()),
                next_attempt_at: None,
            },
        );

        normalize_loaded_state(&mut state);

        assert_eq!(state.schema_version, CURRENT_STATE_SCHEMA_VERSION);
        assert_eq!(
            state.upload_ledger["batch-1"].ack_status,
            DEFAULT_BATCH_STATUS_RETRY_WAIT
        );
        assert!(state.upload_ledger["batch-1"].next_attempt_at.is_some());
    }

    #[test]
    #[serial]
    fn retry_blocked_resets_batch_and_bucket_state() {
        let mut state = CollectorState::default();
        state.finalized_buckets.insert(
            "bucket-1".to_string(),
            FinalizedBucketState {
                blocked: true,
                batch_membership: Some("batch-1".to_string()),
                blocked_at: Some("2026-04-13T10:10:00Z".to_string()),
                last_error: Some("bad auth".to_string()),
                ..bucket_state("bucket-1", false)
            },
        );
        state.upload_ledger.insert(
            "batch-1".to_string(),
            UploadBatchState {
                ack_status: DEFAULT_BATCH_STATUS_BLOCKED.to_string(),
                retry_count: 2,
                last_error: Some("bad auth".to_string()),
                ..batch_state("batch-1", vec!["bucket-1".to_string()])
            },
        );
        save_state_to_temp(&state, |temp| {
            let _dir = EnvGuard::set("TOKSCALE_OM_CONFIG_DIR", temp.path().to_str().unwrap());
            run_retry_blocked(Some("batch-1".to_string())).unwrap();
            let updated = load_state().unwrap();
            assert_eq!(
                updated.upload_ledger["batch-1"].ack_status,
                DEFAULT_BATCH_STATUS_PENDING
            );
            assert!(!updated.finalized_buckets["bucket-1"].blocked);
        });
    }

    #[test]
    fn render_service_units_include_daily_schedule() {
        let command = vec![
            "/usr/local/bin/tokscale-om".to_string(),
            "--no-spinner".to_string(),
            "om".to_string(),
            "sync".to_string(),
        ];
        let schedule = OmScheduleConfig::default();
        let launchd = render_launchd_plist(&command, &schedule);
        assert!(launchd.contains("tokscale-om"));
        assert!(launchd.contains("StartCalendarInterval"));
        assert!(launchd.contains("<integer>13</integer>"));
        assert!(launchd.contains("<integer>1</integer>"));
        assert!(render_systemd_unit(&command).contains("ExecStart=/usr/local/bin/tokscale-om"));
        assert!(render_systemd_timer(&schedule).contains("Mon..Fri *-*-* 13:00:00"));
    }

    #[test]
    fn serialized_state_and_request_do_not_leak_token_or_raw_path() {
        let mut state = CollectorState::default();
        state.sources.insert(
            "codex:hashed".to_string(),
            SourceState {
                source_type: Some("codex".to_string()),
                canonical_path_hash: Some("hashed".to_string()),
                last_consumed_offset: Some(42),
                parser_state: serde_json::json!({ "mode": "incremental-jsonl" }),
                last_successful_scan_time: Some("2026-04-13T10:00:00Z".to_string()),
            },
        );
        state
            .finalized_buckets
            .insert("bucket-1".to_string(), bucket_state("bucket-1", false));

        let request = UploadBatchRequest {
            schema_version: DEFAULT_SCHEMA_VERSION,
            collector_batch_id: "batch-1".to_string(),
            collector_version: "2.0.22".to_string(),
            device_fingerprint: "device-123".to_string(),
            generated_at_utc: "2026-04-13T10:00:00Z".to_string(),
            window_start_utc: "2026-04-13T09:00:00Z".to_string(),
            window_end_utc: "2026-04-13T09:00:00Z".to_string(),
            buckets: upload_bucket_payloads(vec![sample_bucket("bucket-1")]),
        };

        let state_json = serde_json::to_string(&state).unwrap();
        let request_json = serde_json::to_string(&request).unwrap();

        assert!(!state_json.contains("/Users/alice/company/repo"));
        assert!(!state_json.contains("secret-token"));
        assert!(!request_json.contains("/Users/alice/company/repo"));
        assert!(!request_json.contains("secret-token"));
        assert!(!request_json.contains("bucketKey"));
    }

    #[test]
    #[serial]
    fn daemon_loop_runs_multiple_dry_run_cycles() {
        let runtime = Runtime::new().unwrap();
        let config_dir = TempDir::new().unwrap();
        let home_dir = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set(
            "TOKSCALE_OM_CONFIG_DIR",
            config_dir.path().to_str().unwrap(),
        );
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");
        let _pricing_guard = EnvGuard::set("TOKSCALE_PRICING_CACHE_ONLY", "1");

        let codex_dir = home_dir.path().join(".codex").join("sessions").join("proj");
        fs::create_dir_all(&codex_dir).unwrap();
        fs::write(
            codex_dir.join("session.jsonl"),
            concat!(
                r#"{"type":"turn_context","payload":{"model":"gpt-5.4"}}"#,
                "\n",
                r#"{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3},"last_token_usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3}}}}"#,
                "\n"
            ),
        )
        .unwrap();
        run_configure(
            "https://om.example".to_string(),
            "secret".to_string(),
            None,
            Some("device-123".to_string()),
            None,
            None,
            false,
        )
        .unwrap();

        runtime
            .block_on(run_daemon(
                Some(home_dir.path().to_str().unwrap().to_string()),
                Some(vec!["codex".to_string()]),
                true,
                Some(2),
                Some(1),
            ))
            .unwrap();

        let state = load_state().unwrap();
        assert_eq!(state.sources.len(), 1);
        assert_eq!(state.finalized_buckets.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn sync_upload_marks_batches_acked_on_duplicate() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/ai-usage/collector/v1/ingest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "duplicate",
                "batchId": "server-1"
            })))
            .mount(&server)
            .await;

        let config_dir = TempDir::new().unwrap();
        let home_dir = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set(
            "TOKSCALE_OM_CONFIG_DIR",
            config_dir.path().to_str().unwrap(),
        );
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");
        let _pricing_guard = EnvGuard::set("TOKSCALE_PRICING_CACHE_ONLY", "1");

        let codex_dir = home_dir.path().join(".codex").join("sessions").join("proj");
        fs::create_dir_all(&codex_dir).unwrap();
        fs::write(
            codex_dir.join("session.jsonl"),
            concat!(
                r#"{"type":"turn_context","payload":{"model":"gpt-5.4"}}"#,
                "\n",
                r#"{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3},"last_token_usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3}}}}"#,
                "\n"
            ),
        )
        .unwrap();
        run_configure(
            server.uri(),
            "secret".to_string(),
            None,
            Some("device-123".to_string()),
            None,
            None,
            false,
        )
        .unwrap();

        let summary = sync_once(
            Some(home_dir.path().to_str().unwrap().to_string()),
            Some(vec!["codex".to_string()]),
            false,
            true,
        )
        .await
        .unwrap();

        assert_eq!(summary.uploaded_batch_count, 1);
        let state = load_state().unwrap();
        assert_eq!(
            count_batches_with_status(&state, DEFAULT_BATCH_STATUS_ACKED),
            1
        );
    }

    #[tokio::test]
    #[serial]
    async fn sync_upload_persists_blocked_state_on_rejected_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/ai-usage/collector/v1/ingest"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "status": "invalid",
                "error": "bad payload",
                "details": ["bucket rejected"]
            })))
            .mount(&server)
            .await;

        let config_dir = TempDir::new().unwrap();
        let home_dir = TempDir::new().unwrap();
        let _config_guard = EnvGuard::set(
            "TOKSCALE_OM_CONFIG_DIR",
            config_dir.path().to_str().unwrap(),
        );
        let _keyring_guard = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");
        let _pricing_guard = EnvGuard::set("TOKSCALE_PRICING_CACHE_ONLY", "1");

        let codex_dir = home_dir.path().join(".codex").join("sessions").join("proj");
        fs::create_dir_all(&codex_dir).unwrap();
        fs::write(
            codex_dir.join("session.jsonl"),
            concat!(
                r#"{"type":"turn_context","payload":{"model":"gpt-5.4"}}"#,
                "\n",
                r#"{"timestamp":"2026-01-01T00:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3},"last_token_usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3}}}}"#,
                "\n"
            ),
        )
        .unwrap();
        run_configure(
            server.uri(),
            "secret".to_string(),
            None,
            Some("device-123".to_string()),
            None,
            None,
            false,
        )
        .unwrap();

        let error = sync_once(
            Some(home_dir.path().to_str().unwrap().to_string()),
            Some(vec!["codex".to_string()]),
            false,
            true,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Upload rejected"));
        let state = load_state().unwrap();
        assert_eq!(
            count_batches_with_status(&state, DEFAULT_BATCH_STATUS_BLOCKED),
            1
        );
    }

    fn save_state_to_temp(state: &CollectorState, test: impl FnOnce(&TempDir)) {
        let temp = TempDir::new().unwrap();
        let _dir = EnvGuard::set("TOKSCALE_OM_CONFIG_DIR", temp.path().to_str().unwrap());
        let _keyring = EnvGuard::set("TOKSCALE_OM_DISABLE_KEYRING", "1");
        let state_file = temp.path().join("state.json");
        write_secure_json(&state_file, state).unwrap();
        let config_file = temp.path().join("config.json");
        write_secure_string(
            &config_file,
            &serde_json::json!({
                "serverUrl": "https://om.example",
                "deviceFingerprint": "device-123",
                "deviceTokenRef": "device-token:device-123",
                "channel": "stable"
            })
            .to_string(),
        )
        .unwrap();
        let token_file = token_fallback_path("device-token:device-123")
            .unwrap_or_else(|_| temp.path().join("device-token"));
        write_secure_string(&token_file, "secret").unwrap();
        test(&temp);
    }
}
