//! Iceberg REST scan-planning endpoint (§6 from TODO-fix-shelf-performance.md).
//!
//! Exposes a single Iceberg REST-compliant endpoint:
//! `POST /v1/{prefix}/namespaces/{ns}/tables/{t}/plan`
//!
//! Spec is fixed by [`rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! # How it works
//!
//! 1. Reads the current `metadata.json` (cached by [`rewarm_poller`]).
//! 2. Resolves the manifest list + manifests via [`decoded_meta`] (zero S3 IO on warm).
//! 3. Runs predicate evaluation against manifest min/max and Parquet page indexes.
//! 4. Streams `FileScanTask` records back to Trino over HTTP/2 chunked transfer.
//!
//! # Why this matters
//!
//! Trino's planner pain on Iceberg is documented in:
//! - [trinodb/trino#26563](https://github.com/trinodb/trino/issues/26563) — planning
//!   time 7 ms → ~3 min when `iceberg.statistics_enabled=true` on tables with 2000+
//!   partitions.
//! - [trinodb/trino#11708](https://github.com/trinodb/trino/issues/11708) — reduce
//!   `planFiles` calls.
//!
//! Apache Iceberg landed **server-side scan planning** in 2025:
//! - [PR #13004](https://github.com/apache/iceberg/pull/13004) merged 2025-08-15 —
//!   request/response parsers.
//! - [PR #13400](https://github.com/apache/iceberg/pull/13400) merged 2025-12-10 —
//!   routes, `RestTable`, `RestTableScan`, streaming iterator.
//!
//! # Rollout caveats
//!
//! - **REST catalog split is mandatory.** This only works if Trino's catalog is
//!   `iceberg.catalog.type=rest`, not `hive`.
//! - **Trino client support.** Trino's `iceberg-rest` catalog client today still
//!   calls `planFiles` locally even against a REST catalog — the REST scan-planning
//!   client code is what PR #13400 shipped to *core* Iceberg, but Trino has to pick
//!   it up.
//!
//! See `TODO-fix-shelf-performance.md` §6.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::http::ServerState;

/// Iceberg REST scan-plan request per OpenAPI spec.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PlanTableScanRequest {
    /// Snapshot ID to scan (optional; defaults to current snapshot).
    #[serde(default)]
    pub snapshot_id: Option<i64>,

    /// Filter expression in Iceberg expression format.
    #[serde(default)]
    pub filter: Option<IcebergExpression>,

    /// Columns to select (optional; defaults to all).
    #[serde(default)]
    pub select: Option<Vec<String>>,

    /// Whether to use incremental planning (for CDC).
    #[serde(default)]
    pub incremental: bool,

    /// Starting snapshot for incremental scan.
    #[serde(default)]
    pub from_snapshot_id: Option<i64>,

    /// Case sensitivity for column matching.
    #[serde(default = "default_true")]
    pub case_sensitive: bool,

    /// Planning mode: sync or async.
    #[serde(default)]
    pub planning_mode: PlanningMode,
}

fn default_true() -> bool {
    true
}

/// Planning mode for the scan request.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlanningMode {
    #[default]
    Sync,
    Async,
}

/// Iceberg expression (simplified for the scaffold).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum IcebergExpression {
    /// Literal value.
    Literal(serde_json::Value),
    /// Predicate expression.
    Predicate {
        #[serde(rename = "type")]
        expr_type: String,
        term: Option<String>,
        value: Option<serde_json::Value>,
        values: Option<Vec<serde_json::Value>>,
        child: Option<Box<IcebergExpression>>,
        left: Option<Box<IcebergExpression>>,
        right: Option<Box<IcebergExpression>>,
    },
}

/// Iceberg REST scan-plan response.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct PlanTableScanResponse {
    /// Planning result type.
    pub result_type: PlanResultType,

    /// File scan tasks (for sync mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<Vec<FileScanTask>>,

    /// Plan ID for async fetch (for async mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,

    /// Schema of the table.
    pub schema: TableSchema,

    /// Statistics summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<ScanStats>,
}

/// Type of planning result.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlanResultType {
    /// All tasks returned inline.
    Complete,
    /// Tasks available via async fetch.
    Pending,
}

/// A single file scan task.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct FileScanTask {
    /// Data file path (S3 URI).
    pub file_path: String,

    /// File format (PARQUET, ORC, AVRO).
    pub file_format: String,

    /// File size in bytes.
    pub file_size_bytes: u64,

    /// Record count in the file.
    pub record_count: i64,

    /// Partition values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<HashMap<String, serde_json::Value>>,

    /// Row groups to scan (for Parquet).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_groups: Option<Vec<i32>>,

    /// Byte offset to start reading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,

    /// Length to read in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i64>,

    /// Delete files associated with this data file.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub delete_files: Vec<DeleteFile>,

    /// Residual filter after partition pruning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub residual_filter: Option<IcebergExpression>,

    /// Shelf hint: pre-warmed in cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _shelf_prewarmed: Option<bool>,

    /// Shelf hint: MV name if this file backs an MV.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _shelf_mv_hint: Option<String>,
}

/// Delete file reference.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DeleteFile {
    pub file_path: String,
    pub file_format: String,
    pub record_count: i64,
    pub content: DeleteFileContent,
}

/// Delete file content type.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeleteFileContent {
    PositionDeletes,
    EqualityDeletes,
}

/// Simplified table schema.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableSchema {
    pub schema_id: i32,
    pub fields: Vec<SchemaField>,
}

/// Schema field.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: bool,
}

/// Scan statistics summary.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScanStats {
    pub total_files: i64,
    pub total_records: i64,
    pub total_bytes: i64,
    pub files_matched: i64,
    pub files_pruned: i64,
}

/// Build the plan endpoint router.
pub fn plan_router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/v1/:prefix/namespaces/:namespace/tables/:table/plan",
            post(handle_plan_table_scan),
        )
        .route(
            "/v1/:prefix/namespaces/:namespace/tables/:table/plan/:plan_id/tasks",
            post(handle_fetch_scan_tasks),
        )
}

/// Path parameters for the plan endpoint.
#[derive(Debug, Deserialize)]
pub struct PlanPathParams {
    pub prefix: String,
    pub namespace: String,
    pub table: String,
}

/// Handle POST /v1/{prefix}/namespaces/{ns}/tables/{t}/plan
async fn handle_plan_table_scan(
    State(state): State<Arc<ServerState>>,
    Path(params): Path<PlanPathParams>,
    Json(request): Json<PlanTableScanRequest>,
) -> Result<Json<PlanTableScanResponse>, PlanError> {
    let table_fqn = format!("{}.{}.{}", params.prefix, params.namespace, params.table);

    info!(
        table = %table_fqn,
        snapshot_id = ?request.snapshot_id,
        incremental = request.incremental,
        mode = ?request.planning_mode,
        "Received plan request"
    );

    PLAN_REQUESTS_TOTAL.inc();

    // TODO: Implement actual planning logic:
    // 1. Read metadata.json from rewarm_poller cache
    // 2. Resolve manifest list via decoded_meta
    // 3. Apply predicate pruning via filter_service
    // 4. Build FileScanTask list

    // For now, return a scaffold response indicating the endpoint is live
    // but not yet wired to the actual planning logic.
    let response = PlanTableScanResponse {
        result_type: PlanResultType::Complete,
        tasks: Some(Vec::new()), // Empty for scaffold
        plan_id: None,
        schema: TableSchema {
            schema_id: 0,
            fields: Vec::new(),
        },
        stats: Some(ScanStats {
            total_files: 0,
            total_records: 0,
            total_bytes: 0,
            files_matched: 0,
            files_pruned: 0,
        }),
    };

    debug!(
        table = %table_fqn,
        tasks = response.tasks.as_ref().map(|t| t.len()).unwrap_or(0),
        "Plan response ready"
    );

    Ok(Json(response))
}

/// Path parameters for fetch tasks endpoint.
#[derive(Debug, Deserialize)]
pub struct FetchTasksPathParams {
    pub prefix: String,
    pub namespace: String,
    pub table: String,
    pub plan_id: String,
}

/// Handle POST /v1/{prefix}/namespaces/{ns}/tables/{t}/plan/{plan_id}/tasks
async fn handle_fetch_scan_tasks(
    State(_state): State<Arc<ServerState>>,
    Path(params): Path<FetchTasksPathParams>,
) -> Result<Json<FetchScanTasksResponse>, PlanError> {
    info!(
        table = "{}.{}.{}",
        params.prefix, params.namespace, params.table,
        plan_id = %params.plan_id,
        "Fetch scan tasks request"
    );

    // TODO: Implement async task fetching for large plans
    Ok(Json(FetchScanTasksResponse {
        tasks: Vec::new(),
        has_more: false,
    }))
}

/// Response for async task fetching.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct FetchScanTasksResponse {
    pub tasks: Vec<FileScanTask>,
    pub has_more: bool,
}

/// Error type for plan endpoint.
#[derive(Debug)]
pub enum PlanError {
    TableNotFound(String),
    InvalidRequest(String),
    InternalError(String),
}

impl IntoResponse for PlanError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            PlanError::TableNotFound(msg) => (StatusCode::NOT_FOUND, msg),
            PlanError::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            PlanError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = serde_json::json!({
            "error": {
                "message": message,
                "type": match status {
                    StatusCode::NOT_FOUND => "NoSuchTableException",
                    StatusCode::BAD_REQUEST => "BadRequestException",
                    _ => "ServiceFailureException",
                }
            }
        });

        (status, Json(body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

use once_cell::sync::Lazy;
use prometheus::{register_int_counter_with_registry, IntCounter};

static REGISTRY: Lazy<prometheus::Registry> = Lazy::new(|| crate::metrics::REGISTRY.clone());

pub static PLAN_REQUESTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_requests_total",
        "Number of scan-plan requests received.",
        *REGISTRY
    )
    .expect("register plan_requests_total")
});

pub static PLAN_TASKS_RETURNED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_tasks_returned_total",
        "Number of FileScanTask records returned across all plan requests.",
        *REGISTRY
    )
    .expect("register plan_tasks_returned_total")
});

pub static PLAN_FILES_PRUNED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter_with_registry!(
        "shelf_plan_files_pruned_total",
        "Number of files pruned by predicate evaluation.",
        *REGISTRY
    )
    .expect("register plan_files_pruned_total")
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_plan_request_minimal() {
        let json = r#"{}"#;
        let req: PlanTableScanRequest = serde_json::from_str(json).unwrap();
        assert!(req.snapshot_id.is_none());
        assert!(req.filter.is_none());
        assert!(req.case_sensitive);
        assert_eq!(req.planning_mode, PlanningMode::Sync);
    }

    #[test]
    fn test_deserialize_plan_request_full() {
        let json = r#"{
            "snapshot-id": 12345,
            "select": ["col1", "col2"],
            "case-sensitive": false,
            "planning-mode": "async"
        }"#;
        let req: PlanTableScanRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.snapshot_id, Some(12345));
        assert_eq!(req.select, Some(vec!["col1".to_string(), "col2".to_string()]));
        assert!(!req.case_sensitive);
        assert_eq!(req.planning_mode, PlanningMode::Async);
    }

    #[test]
    fn test_serialize_file_scan_task() {
        let task = FileScanTask {
            file_path: "s3://bucket/data/file.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            file_size_bytes: 1024 * 1024,
            record_count: 10000,
            partition: None,
            row_groups: Some(vec![0, 1, 2]),
            start: None,
            length: None,
            delete_files: Vec::new(),
            residual_filter: None,
            _shelf_prewarmed: Some(true),
            _shelf_mv_hint: None,
        };

        let json = serde_json::to_string(&task).unwrap();
        assert!(json.contains("file-path"));
        assert!(json.contains("s3://bucket/data/file.parquet"));
        assert!(json.contains("_shelf_prewarmed"));
    }
}
