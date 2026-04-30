//! SHELF-30 — row-group range coalescing for the s3_shim hot path.
//!
//! The store-level [`crate::store::FoyerStore::get_or_fetch`] already
//! dedupes callers that present the **same** content-addressed key
//! (sha256(etag || offset || length || rg_ordinal), per ADR-0011). It
//! does **not** help the case where two concurrent splits ask for
//! overlapping-but-not-identical byte ranges of the same Iceberg
//! snapshot — each presents a different content key, and each fires
//! its own origin GET. SHELF-30 closes that gap.
//!
//! The strategy in v1 is intentionally narrow:
//!
//! * Maintain a per-`(bucket, key, etag)` map of in-flight requests
//!   keyed on `[offset, offset+length)`.
//! * The first caller for a triple becomes the **leader** and is
//!   registered with its range.
//! * A subsequent caller whose range is **fully contained** inside an
//!   in-flight leader's range becomes a **follower**: it awaits the
//!   leader's `tokio::sync::watch` slot, slices the leader's bytes,
//!   and returns without touching origin or the Foyer pool.
//! * Other overlap shapes (partial overlap, leader is a strict subset
//!   of the new request, different etag) fall through to a fresh
//!   leader registration of their own.
//!
//! Footer-aware row-group quantization (so leader + follower share an
//! identical content-addressed key and the follower's slot is also
//! cached for next time) is deliberately punted to **SHELF-30b**,
//! which is gated on SHELF-34's footer parser landing in production.
//! See `agents/out/adr/0013-row-group-range-coalescing.md` § "Why
//! subsumption only in v1".
//!
//! Liveness / safety:
//!
//! * The leader holds a [`LeaderGuard`] for the duration of its fetch.
//!   On `complete`, followers wake up and observe `Some(Ok(bytes))`.
//! * On guard `Drop` without `complete` (panic, error path that forgot
//!   to call complete), followers observe `Some(Err(...))` and **fall
//!   through to their own fetch path** — there is no deadlock and no
//!   silent stall.
//! * `etag = None` (or empty) returns a no-op leader — without a
//!   content-version anchor we cannot serve a follower's bytes
//!   safely if origin has been updated mid-flight.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::watch;

/// Coalescer state for the s3_shim hot path. One instance is held on
/// `ServerState` and shared across every concurrent GET.
#[derive(Debug, Default)]
pub struct RangeCoalescer {
    inner: Mutex<HashMap<RangeKey, Vec<InflightEntry>>>,
}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct RangeKey {
    bucket: String,
    object: String,
    /// `Some(etag)` is the only shape we register. `None` callers are
    /// short-circuited at [`RangeCoalescer::try_join_or_register`]
    /// because joining a leader without a content-version anchor risks
    /// serving stale bytes.
    etag: String,
}

#[derive(Debug)]
struct InflightEntry {
    offset: u64,
    length: u64,
    receiver: watch::Receiver<Option<FetchResult>>,
}

/// Result published by the leader to all subsumed followers. `Err`
/// surfaces as a fall-through to the standard fetch path on the
/// follower side; the string is logged at DEBUG and otherwise opaque.
pub type FetchResult = Result<Bytes, String>;

/// What a call to [`RangeCoalescer::try_join_or_register`] resolved to.
#[derive(Debug)]
pub enum Outcome {
    /// The caller is the leader. It must run the fetch and call
    /// [`LeaderGuard::complete`] (or let the guard drop on the error
    /// path, which publishes an `Err` to followers automatically).
    Leader(LeaderGuard),
    /// A subsuming leader is already in flight. Await `receiver` then
    /// slice the leader's bytes at
    /// `[offset_in_leader, offset_in_leader+length)`.
    Follower {
        receiver: watch::Receiver<Option<FetchResult>>,
        offset_in_leader: u64,
        length: u64,
    },
}

/// Guard the leader holds for the duration of its fetch.
///
/// Calling [`LeaderGuard::complete`] publishes the bytes to followers
/// and deregisters the entry from the coalescer. Dropping the guard
/// without calling `complete` (panic, early return on an error path)
/// publishes [`LEADER_DROPPED_ERR`] to followers so they fall through
/// rather than wait forever.
#[derive(Debug)]
pub struct LeaderGuard {
    state: Option<LeaderState>,
}

#[derive(Debug)]
struct LeaderState {
    coalescer: Arc<RangeCoalescer>,
    range_key: RangeKey,
    sender: watch::Sender<Option<FetchResult>>,
    offset: u64,
    length: u64,
}

/// Sentinel string published when the leader's [`LeaderGuard`] is
/// dropped without [`LeaderGuard::complete`] being called. Public so
/// callers can match on it for telemetry without a string-compare to
/// a literal scattered across the codebase.
pub const LEADER_DROPPED_ERR: &str = "coalesce leader dropped without completing — falling through";

impl LeaderGuard {
    /// Publish the fetch result to all followers and deregister.
    ///
    /// This is the happy path. `complete(Ok(bytes))` wakes every
    /// follower with the leader's payload; `complete(Err(msg))` wakes
    /// them with an error so they fall through to their own fetch.
    pub fn complete(mut self, result: FetchResult) {
        if let Some(state) = self.state.take() {
            // `watch::Sender::send` only fails when every receiver has
            // been dropped — that is benign (no follower waiting); the
            // bytes still got cached upstream by the leader's normal
            // fetch path.
            let _ = state.sender.send(Some(result));
            state
                .coalescer
                .deregister(&state.range_key, state.offset, state.length);
        }
    }

    /// `true` if this guard is a no-op (etag-less callers, length-0
    /// requests). Exposed for the s3_shim caller's metric bookkeeping.
    pub fn is_noop(&self) -> bool {
        self.state.is_none()
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let _ = state.sender.send(Some(Err(LEADER_DROPPED_ERR.to_string())));
            state
                .coalescer
                .deregister(&state.range_key, state.offset, state.length);
        }
    }
}

impl RangeCoalescer {
    /// Build an empty coalescer. Wrapped in `Arc` because every leader
    /// guard needs a strong handle to deregister itself in `Drop`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Try to join an in-flight leader whose range subsumes
    /// `[offset, offset+length)`. If none exists, register the caller
    /// as a fresh leader.
    ///
    /// `etag = None` (or empty) returns a no-op `Leader` outcome
    /// without registering. The caller's control flow stays uniform
    /// — they still own a `LeaderGuard`, but it has no followers and
    /// `complete` is a no-op.
    pub fn try_join_or_register(
        self: &Arc<Self>,
        bucket: &str,
        object: &str,
        etag: Option<&str>,
        offset: u64,
        length: u64,
    ) -> Outcome {
        if length == 0 {
            return Outcome::Leader(LeaderGuard { state: None });
        }
        let etag = match etag.filter(|e| !e.is_empty()) {
            Some(e) => e,
            None => return Outcome::Leader(LeaderGuard { state: None }),
        };

        let range_key = RangeKey {
            bucket: bucket.to_string(),
            object: object.to_string(),
            etag: etag.to_string(),
        };
        let mut guard = self.inner.lock();
        if let Some(list) = guard.get(&range_key) {
            for entry in list {
                let leader_end = entry.offset.saturating_add(entry.length);
                let req_end = offset.saturating_add(length);
                if entry.offset <= offset && req_end <= leader_end {
                    return Outcome::Follower {
                        receiver: entry.receiver.clone(),
                        offset_in_leader: offset - entry.offset,
                        length,
                    };
                }
            }
        }
        let (tx, rx) = watch::channel(None);
        guard
            .entry(range_key.clone())
            .or_default()
            .push(InflightEntry {
                offset,
                length,
                receiver: rx,
            });
        Outcome::Leader(LeaderGuard {
            state: Some(LeaderState {
                coalescer: self.clone(),
                range_key,
                sender: tx,
                offset,
                length,
            }),
        })
    }

    fn deregister(&self, range_key: &RangeKey, offset: u64, length: u64) {
        let mut guard = self.inner.lock();
        if let Some(list) = guard.get_mut(range_key) {
            list.retain(|e| !(e.offset == offset && e.length == length));
            if list.is_empty() {
                guard.remove(range_key);
            }
        }
    }

    /// Best-effort observability hook — total in-flight leaders across
    /// all keys. Cheap-but-not-free (acquires the mutex). Exposed for
    /// tests and for a future `/admin/coalesce` stats endpoint.
    pub fn inflight_len(&self) -> usize {
        self.inner.lock().values().map(Vec::len).sum()
    }
}

/// Slice `leader_bytes` to satisfy a follower waiting on
/// `[offset_in_leader, offset_in_leader+length)`.
///
/// Returns `None` if the leader's payload was truncated below the
/// follower's expected window (origin returned fewer bytes than the
/// leader asked for, or some other invariant violation). Callers
/// surface this as an internal error rather than a partial response —
/// see `s3_shim::handle_get_object`'s SHELF-30 fall-through handling.
pub fn slice_for_follower(
    leader_bytes: &Bytes,
    offset_in_leader: u64,
    length: u64,
) -> Option<Bytes> {
    let start = usize::try_from(offset_in_leader).ok()?;
    let len = usize::try_from(length).ok()?;
    let end = start.checked_add(len)?;
    if end > leader_bytes.len() {
        return None;
    }
    Some(leader_bytes.slice(start..end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[tokio::test]
    async fn first_caller_is_leader() {
        let c = RangeCoalescer::new();
        let outcome = c.try_join_or_register("buk", "k", Some("etag-1"), 0, 100);
        assert!(matches!(outcome, Outcome::Leader(_)));
        assert_eq!(c.inflight_len(), 1);
    }

    #[tokio::test]
    async fn empty_etag_is_noop_leader_and_does_not_register() {
        let c = RangeCoalescer::new();
        let outcome = c.try_join_or_register("buk", "k", None, 0, 100);
        match outcome {
            Outcome::Leader(g) => assert!(g.is_noop()),
            _ => panic!("expected noop leader"),
        }
        assert_eq!(c.inflight_len(), 0, "no etag must not register");

        let outcome = c.try_join_or_register("buk", "k", Some(""), 0, 100);
        match outcome {
            Outcome::Leader(g) => assert!(g.is_noop()),
            _ => panic!("expected noop leader"),
        }
        assert_eq!(c.inflight_len(), 0, "empty etag must not register");
    }

    #[tokio::test]
    async fn zero_length_is_noop_leader_and_does_not_register() {
        let c = RangeCoalescer::new();
        let outcome = c.try_join_or_register("buk", "k", Some("etag-1"), 0, 0);
        match outcome {
            Outcome::Leader(g) => assert!(g.is_noop()),
            _ => panic!("expected noop leader"),
        }
        assert_eq!(c.inflight_len(), 0);
    }

    #[tokio::test]
    async fn subsumed_caller_becomes_follower_and_slices() {
        let c = RangeCoalescer::new();
        let leader = c.try_join_or_register("buk", "k", Some("etag-1"), 0, 100);
        let leader_guard = match leader {
            Outcome::Leader(g) => g,
            _ => panic!(),
        };
        // Follower request 20..40 — strictly inside leader 0..100.
        let follower = c.try_join_or_register("buk", "k", Some("etag-1"), 20, 20);
        let (mut rx, off, len) = match follower {
            Outcome::Follower {
                receiver,
                offset_in_leader,
                length,
            } => (receiver, offset_in_leader, length),
            _ => panic!("expected follower"),
        };
        assert_eq!(off, 20);
        assert_eq!(len, 20);

        // Leader publishes; follower reads.
        let payload = Bytes::from((0u8..100).collect::<Vec<u8>>());
        leader_guard.complete(Ok(payload.clone()));
        rx.changed().await.unwrap();
        let v = rx.borrow().clone().unwrap();
        let bytes = v.unwrap();
        let sliced = slice_for_follower(&bytes, off, len).unwrap();
        assert_eq!(sliced.len(), 20);
        assert_eq!(sliced[0], 20u8);
        assert_eq!(sliced[19], 39u8);

        // Leader entry is deregistered after complete.
        assert_eq!(c.inflight_len(), 0);
    }

    #[tokio::test]
    async fn partial_overlap_is_not_coalesced() {
        let c = RangeCoalescer::new();
        let _leader = c.try_join_or_register("buk", "k", Some("e"), 0, 50);
        // Request 30..80 partially overlaps leader 0..50 but is not subsumed.
        let outcome = c.try_join_or_register("buk", "k", Some("e"), 30, 50);
        assert!(matches!(outcome, Outcome::Leader(_)));
        assert_eq!(c.inflight_len(), 2);
    }

    #[tokio::test]
    async fn superset_request_is_not_coalesced() {
        let c = RangeCoalescer::new();
        let _leader = c.try_join_or_register("buk", "k", Some("e"), 10, 20);
        // Request 0..100 is a superset of the in-flight leader.
        let outcome = c.try_join_or_register("buk", "k", Some("e"), 0, 100);
        assert!(matches!(outcome, Outcome::Leader(_)));
        assert_eq!(c.inflight_len(), 2);
    }

    #[tokio::test]
    async fn different_etag_does_not_coalesce() {
        let c = RangeCoalescer::new();
        let _leader = c.try_join_or_register("buk", "k", Some("etag-1"), 0, 100);
        let outcome = c.try_join_or_register("buk", "k", Some("etag-2"), 20, 20);
        assert!(matches!(outcome, Outcome::Leader(_)));
        assert_eq!(c.inflight_len(), 2);
    }

    #[tokio::test]
    async fn different_object_does_not_coalesce() {
        let c = RangeCoalescer::new();
        let _leader = c.try_join_or_register("buk", "obj-a", Some("e"), 0, 100);
        let outcome = c.try_join_or_register("buk", "obj-b", Some("e"), 20, 20);
        assert!(matches!(outcome, Outcome::Leader(_)));
        assert_eq!(c.inflight_len(), 2);
    }

    #[tokio::test]
    async fn dropped_leader_publishes_error_to_followers() {
        let c = RangeCoalescer::new();
        let leader = match c.try_join_or_register("buk", "k", Some("e"), 0, 100) {
            Outcome::Leader(g) => g,
            _ => panic!(),
        };
        let mut rx = match c.try_join_or_register("buk", "k", Some("e"), 10, 10) {
            Outcome::Follower { receiver, .. } => receiver,
            _ => panic!(),
        };
        drop(leader);
        rx.changed().await.unwrap();
        let v = rx.borrow().clone().unwrap();
        assert!(matches!(v, Err(ref s) if s == LEADER_DROPPED_ERR));
        assert_eq!(c.inflight_len(), 0);
    }

    #[tokio::test]
    async fn complete_with_err_propagates() {
        let c = RangeCoalescer::new();
        let leader = match c.try_join_or_register("buk", "k", Some("e"), 0, 100) {
            Outcome::Leader(g) => g,
            _ => panic!(),
        };
        let mut rx = match c.try_join_or_register("buk", "k", Some("e"), 10, 10) {
            Outcome::Follower { receiver, .. } => receiver,
            _ => panic!(),
        };
        leader.complete(Err("origin 503".into()));
        rx.changed().await.unwrap();
        let v = rx.borrow().clone().unwrap();
        assert!(matches!(v, Err(ref s) if s == "origin 503"));
        assert_eq!(c.inflight_len(), 0);
    }

    #[tokio::test]
    async fn slice_for_follower_truncated_payload_returns_none() {
        let leader = b("0123456789");
        // Leader returned only 10 bytes; follower wanted 5..20 (length 15).
        assert!(slice_for_follower(&leader, 5, 15).is_none());
    }

    #[tokio::test]
    async fn slice_for_follower_exact_match() {
        let leader = b("0123456789");
        let s = slice_for_follower(&leader, 0, 10).unwrap();
        assert_eq!(s, b("0123456789"));
    }

    #[tokio::test]
    async fn many_followers_one_leader() {
        let c = RangeCoalescer::new();
        let leader = match c.try_join_or_register("buk", "k", Some("e"), 0, 100) {
            Outcome::Leader(g) => g,
            _ => panic!(),
        };
        let mut receivers = Vec::new();
        for off in [10u64, 20, 30, 40, 50] {
            match c.try_join_or_register("buk", "k", Some("e"), off, 5) {
                Outcome::Follower {
                    receiver,
                    offset_in_leader,
                    length,
                } => receivers.push((receiver, offset_in_leader, length)),
                other => panic!("expected follower for offset={off}, got {other:?}"),
            }
        }
        let payload = Bytes::from((0u8..100).collect::<Vec<u8>>());
        leader.complete(Ok(payload));
        // Pump every follower.
        for (mut rx, off, len) in receivers {
            tokio::time::timeout(Duration::from_secs(1), rx.changed())
                .await
                .unwrap()
                .unwrap();
            let bytes = rx.borrow().clone().unwrap().unwrap();
            let s = slice_for_follower(&bytes, off, len).unwrap();
            assert_eq!(s.len(), 5);
            assert_eq!(s[0], off as u8);
        }
        assert_eq!(c.inflight_len(), 0);
    }
}
