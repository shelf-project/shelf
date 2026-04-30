//! Span-hygiene regression tests for SHELF-08.
//!
//! These tests do NOT require a running OTLP collector, MinIO, or
//! docker — they install a custom `tracing_subscriber::Layer` that
//! records span `(name, parent_id)` into a shared vector and then
//! drive a narrow slice of the code path to prove the expected spans
//! open in the expected parent → child relationship.
//!
//! Covered SHELF-08 acceptance properties:
//!
//! 1. A `GET /cache/*` request opens at least two spans —
//!    `http.get_cache` (server) and `s3.get_object` (client) — with
//!    the former as the parent.
//! 2. `FoyerStore::get_or_fetch` emits a `shelfd.singleflight` event
//!    labeled `role = leader` (once) and `role = follower` (≥ once)
//!    when multiple callers race on the same cold key.
//!
//! ### Runtime note
//!
//! The tests build their own **current-thread** Tokio runtime so that
//! `tracing::subscriber::set_default` (thread-local, `!Send`) covers
//! every task spawned during the test. `#[tokio::test]` would expand
//! into a `Send` future that cannot hold a `DefaultGuard` across
//! `.await`.

#![cfg(test)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use shelfd::admission::SizeThresholdPolicy;
use shelfd::config::{
    AdmissionConfig, MetadataPoolConfig, OriginConfig, PoolsConfig, RowGroupPoolConfig,
};
use shelfd::origin::{Origin, S3Origin};
use shelfd::store::{key_from_tuple, FoyerStore, Pool};
use tracing::{field::Visit, span, Event, Instrument, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Debug, Clone)]
struct SpanSample {
    id: u64,
    name: &'static str,
    parent: Option<u64>,
}

#[derive(Debug, Clone)]
struct EventSample {
    fields: HashMap<String, String>,
}

#[derive(Default)]
struct Captured {
    spans: Mutex<Vec<SpanSample>>,
    events: Mutex<Vec<EventSample>>,
}

struct CapturingLayer(Arc<Captured>);

impl<S> Layer<S> for CapturingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let name = attrs.metadata().name();
        let parent_id = attrs
            .parent()
            .cloned()
            .or_else(|| ctx.current_span().id().cloned())
            .map(|id| id.into_u64());
        self.0.spans.lock().unwrap().push(SpanSample {
            id: id.into_u64(),
            name,
            parent: parent_id,
        });
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        let mut visitor = FieldCollector(&mut fields);
        event.record(&mut visitor);
        self.0.events.lock().unwrap().push(EventSample { fields });
    }
}

struct FieldCollector<'a>(&'a mut HashMap<String, String>);

impl Visit for FieldCollector<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime")
}

#[test]
fn get_range_emits_s3_span_under_parent_handler_span() {
    let rt = current_thread_runtime();
    let captured = Arc::new(Captured::default());

    let subscriber = tracing_subscriber::registry().with(CapturingLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    rt.block_on(async {
        // Point the origin at a closed TCP port so `get_range` fails
        // *after* opening its `s3.get_object` span. The SDK builds the
        // client synchronously; span capture is what matters.
        let origin = S3Origin::new(&OriginConfig {
            bucket: "shelf-trace-it".into(),
            endpoint_url: Some("http://127.0.0.1:1".into()),
            region: Some("us-east-1".into()),
            max_inflight: 4,
        })
        .await
        .expect("S3Origin::new");

        let parent = tracing::info_span!("http.get_cache", route = "/cache/:pool/:key/:range");
        async {
            let _ = origin
                .get_range("shelf-trace-it", "missing-key", 0, 16)
                .await;
        }
        .instrument(parent)
        .await;
    });

    let spans = captured.spans.lock().unwrap().clone();
    let by_id: HashMap<u64, SpanSample> = spans.iter().map(|s| (s.id, s.clone())).collect();

    let handler = spans
        .iter()
        .find(|s| s.name == "http.get_cache")
        .cloned()
        .unwrap_or_else(|| {
            panic!("http.get_cache span missing; captured: {spans:?}");
        });
    let s3 = spans
        .iter()
        .find(|s| s.name == "s3.get_object")
        .cloned()
        .unwrap_or_else(|| {
            panic!("s3.get_object span missing; captured: {spans:?}");
        });

    assert!(
        spans.len() >= 2,
        "expected ≥ 2 spans, got {} ({:?})",
        spans.len(),
        spans.iter().map(|s| s.name).collect::<Vec<_>>(),
    );

    let mut cur = s3.parent;
    let mut found_handler_ancestor = false;
    while let Some(pid) = cur {
        if pid == handler.id {
            found_handler_ancestor = true;
            break;
        }
        cur = by_id.get(&pid).and_then(|p| p.parent);
    }
    assert!(
        found_handler_ancestor,
        "s3.get_object (id={}) must chain up to http.get_cache (id={}); captured: {:?}",
        s3.id, handler.id, spans,
    );
}

#[test]
fn singleflight_emits_leader_and_follower_events() {
    let rt = current_thread_runtime();
    let captured = Arc::new(Captured::default());

    let subscriber = tracing_subscriber::registry().with(CapturingLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    rt.block_on(async {
        let pools = PoolsConfig {
            metadata: MetadataPoolConfig {
                dram_bytes: 1 << 20,
            },
            rowgroup: RowGroupPoolConfig {
                dram_bytes: 1 << 20,
                nvme_dir: PathBuf::from("/tmp/shelfd-it-trace-unused"),
                nvme_bytes: 0,
                eviction_policy: shelfd::config::EvictionPolicy::default(),
                disk_cache: shelfd::config::RowGroupDiskCacheConfig::default(),
                compression: shelfd::config::CompressionConfig::default(),
            },
        };
        let store = Arc::new(FoyerStore::open(&pools).await.expect("open"));
        let admission = SizeThresholdPolicy::from_config(&AdmissionConfig {
            size_threshold_bytes: 1 << 30,
            pinned_bypass: true,
        });
        let key = key_from_tuple(b"singleflight-etag", 0, 1, 0).unwrap();

        let mk_fut = |payload: &'static [u8]| {
            let store = store.clone();
            let admission = admission.clone();
            let key = key.clone();
            async move {
                store
                    .get_or_fetch(Pool::RowGroup, key, &admission, async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, shelfd::Error>(Bytes::from_static(payload))
                    })
                    .await
            }
        };

        let (a, b) = tokio::join!(mk_fut(b"payload"), mk_fut(b"payload"));
        a.unwrap();
        b.unwrap();
    });

    let events = captured.events.lock().unwrap().clone();
    let singleflight_events: Vec<_> = events
        .iter()
        .filter(|e| {
            // The event body is a bare message "shelfd.singleflight";
            // the tracing crate stores it under the `message` field.
            e.fields.get("message").map(|v| v.as_str()) == Some("shelfd.singleflight")
                || e.fields.values().any(|v| v == "shelfd.singleflight")
        })
        .collect();
    let leaders = singleflight_events
        .iter()
        .filter(|e| e.fields.get("role").map(|s| s.as_str()) == Some("leader"))
        .count();
    let followers = singleflight_events
        .iter()
        .filter(|e| e.fields.get("role").map(|s| s.as_str()) == Some("follower"))
        .count();
    assert_eq!(
        leaders, 1,
        "expected exactly 1 leader event, got {leaders}; events: {events:?}",
    );
    assert!(
        followers >= 1,
        "expected ≥ 1 follower event, got {followers}; events: {events:?}",
    );
}
