use crate::sessions::UnifiedMessage;
use crate::{clients::ClientId, scanner::ScanResult};
use chrono::{DateTime, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

const UNKNOWN_WORKSPACE_KEY: &str = "\0unknown-workspace";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenMercatoBucketKey {
    pub hour_start_utc: String,
    pub source_client: String,
    pub provider_id: String,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    pub workspace_fingerprint: String,
}

impl OpenMercatoBucketKey {
    pub fn as_stable_key(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.hour_start_utc,
            self.source_client,
            self.provider_id,
            self.model_id,
            self.agent_name.as_deref().unwrap_or(""),
            self.workspace_fingerprint
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenMercatoHourlyBucket {
    pub bucket_key: String,
    pub hour_start_utc: String,
    pub source_client: String,
    pub provider_id: String,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    pub workspace_fingerprint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_label: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub message_count: i32,
    pub turn_count: i32,
    pub source_session_count: i32,
    pub estimated_usd: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenMercatoSourceSnapshot {
    pub source_key: String,
    pub source_type: String,
    pub canonical_path_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_consumed_offset: Option<u64>,
    #[serde(default)]
    pub parser_state: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenMercatoLocalParseResult {
    pub messages: Vec<UnifiedMessage>,
    pub source_snapshots: Vec<OpenMercatoSourceSnapshot>,
}

#[derive(Default)]
struct BucketAccumulator {
    workspace_label: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_write_tokens: i64,
    reasoning_tokens: i64,
    message_count: i32,
    turn_count: i32,
    estimated_usd: f64,
    source_sessions: HashSet<String>,
}

pub fn workspace_fingerprint(workspace_key: Option<&str>) -> String {
    let raw = workspace_key
        .filter(|key| !key.trim().is_empty())
        .unwrap_or(UNKNOWN_WORKSPACE_KEY);
    hex_sha256(raw.as_bytes())
}

pub fn open_mercato_bucket_payload_hash(bucket: &OpenMercatoHourlyBucket) -> String {
    let json = serde_json::to_vec(bucket).unwrap_or_default();
    hex_sha256(&json)
}

pub fn build_open_mercato_hourly_buckets(
    messages: Vec<UnifiedMessage>,
    now_utc: DateTime<Utc>,
) -> Vec<OpenMercatoHourlyBucket> {
    let open_hour_start = truncate_to_hour(now_utc);
    let mut buckets: BTreeMap<OpenMercatoBucketKey, BucketAccumulator> = BTreeMap::new();

    for msg in messages {
        let Some(hour_start_utc) = message_hour_start_utc(&msg) else {
            continue;
        };
        if hour_start_utc >= open_hour_start {
            continue;
        }

        let agent_name = msg
            .agent
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let workspace_fingerprint = workspace_fingerprint(msg.workspace_key.as_deref());
        let key = OpenMercatoBucketKey {
            hour_start_utc: hour_start_utc.to_rfc3339(),
            source_client: msg.client.clone(),
            provider_id: msg.provider_id.clone(),
            model_id: msg.model_id.clone(),
            agent_name,
            workspace_fingerprint,
        };

        let entry = buckets.entry(key).or_default();
        if entry.workspace_label.is_none() {
            entry.workspace_label = msg
                .workspace_label
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
        }
        entry.input_tokens = entry.input_tokens.saturating_add(msg.tokens.input);
        entry.output_tokens = entry.output_tokens.saturating_add(msg.tokens.output);
        entry.cache_read_tokens = entry
            .cache_read_tokens
            .saturating_add(msg.tokens.cache_read);
        entry.cache_write_tokens = entry
            .cache_write_tokens
            .saturating_add(msg.tokens.cache_write);
        entry.reasoning_tokens = entry.reasoning_tokens.saturating_add(msg.tokens.reasoning);
        entry.message_count = entry.message_count.saturating_add(msg.message_count.max(0));
        if msg.is_turn_start {
            entry.turn_count = entry.turn_count.saturating_add(1);
        }
        entry.estimated_usd += msg.cost;

        let session_id = if !msg.session_id.trim().is_empty() {
            msg.session_id.clone()
        } else if let Some(dedup_key) = msg.dedup_key.as_deref().filter(|value| !value.is_empty()) {
            format!("dedup:{dedup_key}")
        } else {
            format!("fallback:{}:{}", msg.client, msg.timestamp)
        };
        entry.source_sessions.insert(session_id);
    }

    buckets
        .into_iter()
        .map(|(key, acc)| OpenMercatoHourlyBucket {
            bucket_key: key.as_stable_key(),
            hour_start_utc: key.hour_start_utc,
            source_client: key.source_client,
            provider_id: key.provider_id,
            model_id: key.model_id,
            agent_name: key.agent_name,
            workspace_fingerprint: key.workspace_fingerprint,
            workspace_label: acc.workspace_label,
            input_tokens: acc.input_tokens,
            output_tokens: acc.output_tokens,
            cache_read_tokens: acc.cache_read_tokens,
            cache_write_tokens: acc.cache_write_tokens,
            reasoning_tokens: acc.reasoning_tokens,
            message_count: acc.message_count,
            turn_count: acc.turn_count,
            source_session_count: acc.source_sessions.len() as i32,
            estimated_usd: acc.estimated_usd,
        })
        .collect()
}

fn truncate_to_hour(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_minute(0)
        .and_then(|dt| dt.with_second(0))
        .and_then(|dt| dt.with_nanosecond(0))
        .expect("valid hour truncation")
}

fn message_hour_start_utc(msg: &UnifiedMessage) -> Option<DateTime<Utc>> {
    if msg.timestamp > 0 {
        let dt = Utc.timestamp_millis_opt(msg.timestamp).single()?;
        return Some(truncate_to_hour(dt));
    }

    let fallback = format!("{}T00:00:00Z", msg.date);
    DateTime::parse_from_rfc3339(&fallback)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn build_open_mercato_source_snapshots(
    scan_result: &ScanResult,
) -> Vec<OpenMercatoSourceSnapshot> {
    let mut snapshots = Vec::new();

    snapshots.extend(
        scan_result
            .get(ClientId::Codex)
            .iter()
            .filter_map(|path| snapshot_from_path("codex", path)),
    );
    snapshots.extend(
        scan_result
            .get(ClientId::Claude)
            .iter()
            .filter_map(|path| snapshot_from_path("claude", path)),
    );

    snapshots.sort_by(|left, right| left.source_key.cmp(&right.source_key));
    snapshots
}

pub fn canonical_source_path_hash(path: &Path) -> String {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    hex_sha256(canonical.to_string_lossy().as_bytes())
}

fn snapshot_from_path(source_type: &str, path: &Path) -> Option<OpenMercatoSourceSnapshot> {
    if !path.is_file() {
        return None;
    }

    let canonical_path_hash = canonical_source_path_hash(path);
    let source_key = format!("{source_type}:{canonical_path_hash}");
    let metadata = fs::metadata(path).ok();
    let parser_state = match source_type {
        "codex" => serde_json::json!({
            "mode": "incremental-jsonl",
            "cache": "upstream-source-message-cache",
        }),
        "claude" => serde_json::json!({
            "mode": "full-file-cache",
            "fingerprint": "path+meta-sidecar",
        }),
        _ => serde_json::json!({
            "mode": "scan-only",
        }),
    };

    Some(OpenMercatoSourceSnapshot {
        source_key,
        source_type: source_type.to_string(),
        canonical_path_hash,
        last_consumed_offset: metadata.map(|value| value.len()),
        parser_state,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TokenBreakdown;

    fn make_message(
        session_id: &str,
        timestamp: i64,
        agent: Option<&str>,
        workspace_key: Option<&str>,
    ) -> UnifiedMessage {
        let mut message = UnifiedMessage::new_with_agent(
            "codex",
            "gpt-5-codex",
            "openai",
            session_id,
            timestamp,
            TokenBreakdown {
                input: 100,
                output: 40,
                cache_read: 5,
                cache_write: 2,
                reasoning: 7,
            },
            0.42,
            agent.map(str::to_string),
        );
        message.message_count = 3;
        message.is_turn_start = true;
        message.set_workspace(
            workspace_key.map(str::to_string),
            workspace_key
                .and_then(|path| path.rsplit('/').next())
                .map(str::to_string),
        );
        message
    }

    #[test]
    fn excludes_current_open_hour() {
        let now = DateTime::parse_from_rfc3339("2026-04-13T10:15:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let closed = make_message("s-1", now.timestamp_millis() - 60 * 60 * 1000, None, None);
        let open = make_message("s-2", now.timestamp_millis(), None, None);

        let buckets = build_open_mercato_hourly_buckets(vec![closed, open], now);

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].hour_start_utc, "2026-04-13T09:00:00+00:00");
    }

    #[test]
    fn splits_buckets_by_agent_and_workspace_fingerprint() {
        let now = DateTime::parse_from_rfc3339("2026-04-13T10:15:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let msg_a = make_message(
            "s-1",
            now.timestamp_millis() - 60 * 60 * 1000,
            Some("builder"),
            Some("/repo-a"),
        );
        let msg_b = make_message(
            "s-2",
            now.timestamp_millis() - 60 * 60 * 1000,
            Some("reviewer"),
            Some("/repo-b"),
        );

        let buckets = build_open_mercato_hourly_buckets(vec![msg_a, msg_b], now);

        assert_eq!(buckets.len(), 2);
        assert_ne!(
            buckets[0].workspace_fingerprint,
            buckets[1].workspace_fingerprint
        );
        assert_ne!(buckets[0].agent_name, buckets[1].agent_name);
        assert!(
            buckets
                .iter()
                .all(|bucket| bucket.workspace_label.is_some()),
            "workspace label may be sent, but not the raw workspace path"
        );
    }

    #[test]
    fn aggregates_metrics_and_counts_distinct_sessions() {
        let now = DateTime::parse_from_rfc3339("2026-04-13T10:15:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let base_ts = now.timestamp_millis() - 60 * 60 * 1000;
        let msg_a = make_message("s-1", base_ts, Some("builder"), Some("/repo-a"));
        let msg_b = make_message("s-2", base_ts + 5_000, Some("builder"), Some("/repo-a"));

        let buckets = build_open_mercato_hourly_buckets(vec![msg_a, msg_b], now);

        assert_eq!(buckets.len(), 1);
        let bucket = &buckets[0];
        assert_eq!(bucket.input_tokens, 200);
        assert_eq!(bucket.output_tokens, 80);
        assert_eq!(bucket.cache_read_tokens, 10);
        assert_eq!(bucket.cache_write_tokens, 4);
        assert_eq!(bucket.reasoning_tokens, 14);
        assert_eq!(bucket.message_count, 6);
        assert_eq!(bucket.turn_count, 2);
        assert_eq!(bucket.source_session_count, 2);
        assert!((bucket.estimated_usd - 0.84).abs() < 1e-9);
    }

    #[test]
    fn workspace_fingerprint_does_not_leak_raw_path() {
        let fingerprint = workspace_fingerprint(Some("/Users/alice/company/repo"));
        assert_eq!(fingerprint.len(), 64);
        assert!(!fingerprint.contains("/Users/alice/company/repo"));
        assert_eq!(
            workspace_fingerprint(Some("/Users/alice/company/repo")),
            fingerprint
        );
    }

    #[test]
    fn canonical_source_path_hash_does_not_leak_raw_path() {
        let path = Path::new("/Users/alice/company/.codex/sessions/example.jsonl");
        let fingerprint = canonical_source_path_hash(path);
        assert_eq!(fingerprint.len(), 64);
        assert!(!fingerprint.contains("/Users/alice/company"));
    }
}
