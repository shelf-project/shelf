//! SHELF-42 — A/B query tagging.
//!
//! This module owns the receive side of the
//! [`X-Shelf-Tag`](../../docs/contracts/ab-tag.md) propagation
//! contract. The Trino plugin attaches a URL-encoded JSON tag set to
//! every shelf-bound HTTP request; shelfd parses it here, enforces the
//! per-pod cardinality cap, and exposes a per-request accessor that
//! downstream metric / observability hooks consume.
//!
//! Design constraints (from the v1 contract):
//!
//! - **Lifetime**: tag values belong to a single request. We do **not**
//!   stash them on a [`crate::http::ServerState`] field, do **not**
//!   thread them through a `tokio::task_local` global, and do **not**
//!   include them in any cache key. The tag is built once per request
//!   in `s3_shim`, attached to a [`TaggedContext`], and dropped when
//!   the response future completes.
//! - **Fail-open**: a malformed `X-Shelf-Tag` header behaves identically
//!   to an absent one. The cache must keep serving reads — the tag is a
//!   labelling concern, never a control-plane decision.
//! - **Cardinality safety**: even a *valid* tag is replaced by the
//!   sentinel [`OTHER_TAG_LABEL`] once the per-pod cap is reached
//!   within a scrape window. This mirrors the `table_label` "other"
//!   sentinel in `s3_shim::table_label` (see `metrics.rs` rationale).
//! - **Default-off**: the chart's `cache.abTag.enabled=false` default
//!   means a freshly deployed pod ignores the header entirely. Operators
//!   opt in via Helm (`enabled: true`) or per-pod env override
//!   (`SHELFD_AB_TAG=on`).
//!
//! See `docs/contracts/ab-tag.md` for the wire-level contract and
//! `shelfd/docs/design-notes/SHELF-42-ab-query-tagging.md` for the
//! lifecycle diagram.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS};
use serde::{Deserialize, Serialize};

/// Header name carried by the Trino plugin; the receive site below in
/// [`extract_from_headers`] is the only producer that should look at
/// this constant. Keep the casing as documented; HTTP header names are
/// case-insensitive on the wire but the documentation example uses
/// this canonical form.
pub const HEADER_NAME: &str = "x-shelf-tag";

/// Sentinel label used in place of a per-tag value once the
/// cardinality cap is reached. Mirrors `s3_shim::table_label` "other".
pub const OTHER_TAG_LABEL: &str = "other";

/// Empty-tag label used on metrics when a request had no `X-Shelf-Tag`
/// header. Distinct from [`OTHER_TAG_LABEL`] so dashboards can split
/// "tag absent" from "tag dropped due to cap".
pub const NO_TAG_LABEL: &str = "none";

/// Maximum decoded payload size of an `X-Shelf-Tag` header, in bytes.
/// Larger payloads are treated as if the header were absent.
pub const MAX_DECODED_BYTES: usize = 4096;

/// Maximum number of `{key: value}` pairs in a single tag set.
pub const MAX_KEYS: usize = 8;

/// Maximum length, in UTF-8 bytes, of a single tag value.
pub const MAX_VALUE_BYTES: usize = 128;

/// Default per-pod cardinality cap. Operators override via
/// `cache.abTag.maxDistinctTags` (Helm value → `Config::ab_tag`).
pub const DEFAULT_MAX_DISTINCT_TAGS: usize = 16;

/// Default scrape window over which the cap is enforced. Prometheus
/// scrapes shelfd every 30 s in production; the window deliberately
/// exceeds that so a long Prometheus pause does not cause us to
/// re-warn on the same offending tag every scrape.
pub const DEFAULT_SCRAPE_WINDOW: Duration = Duration::from_secs(60);

/// RFC 3986 percent-encoding set we apply when normalising tag values
/// for the wire form. Matches `application/x-www-form-urlencoded` for
/// the JSON characters we need to escape (`{`, `}`, `"`, `,`, `:`).
const TAG_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'#')
    .add(b'?')
    .add(b'{')
    .add(b'}')
    .add(b':')
    .add(b',')
    .add(b'/')
    .add(b'%')
    .add(b'+')
    .add(b'=')
    .add(b'&')
    .add(b';');

/// Validation rule for a tag-set key. The contract requires
/// `[A-Za-z_][A-Za-z0-9_]{0,63}`.
fn is_valid_key(s: &str) -> bool {
    if s.is_empty() || s.len() > 64 {
        return false;
    }
    let mut chars = s.chars();
    let head = chars.next().unwrap();
    if !(head.is_ascii_alphabetic() || head == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A fully-validated tag set. Sorted by key so equality and hashing
/// (used by [`AbTagState::tag_label_for`]) are deterministic across
/// requests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagSet {
    /// Sorted-by-key entries. Empty == "tag absent".
    entries: Vec<(String, String)>,
}

impl TagSet {
    /// Empty tag set.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// True iff the tag set carries no entries (header was absent or
    /// invalid). Avoid serialising an empty tag onto a metric.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Sorted iter over `(key, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Look up the value for a single key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.entries[i].1.as_str())
    }

    /// Build a tag set from already-validated `(key, value)` pairs.
    /// The constructor takes care of sorting + dedup-by-key (last
    /// write wins).
    pub fn from_pairs<I, K, V>(pairs: I) -> Result<Self, ParseError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut entries: Vec<(String, String)> = pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        if entries.is_empty() {
            return Ok(Self { entries });
        }
        if entries.len() > MAX_KEYS {
            return Err(ParseError::TooManyKeys {
                got: entries.len(),
                cap: MAX_KEYS,
            });
        }
        for (k, v) in &entries {
            if !is_valid_key(k) {
                return Err(ParseError::BadKey(k.clone()));
            }
            if v.len() > MAX_VALUE_BYTES {
                return Err(ParseError::ValueTooLong {
                    key: k.clone(),
                    bytes: v.len(),
                });
            }
        }
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        // Dedup-by-key: if the caller passed duplicate keys, keep the
        // last one (matches `HashMap::insert` semantics).
        let mut deduped: Vec<(String, String)> = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            if let Some(last) = deduped.last_mut() {
                if last.0 == k {
                    last.1 = v;
                    continue;
                }
            }
            deduped.push((k, v));
        }
        Ok(Self { entries: deduped })
    }

    /// Render the canonical wire form: URL-encoded JSON with keys in
    /// lexicographic order. Returns `None` for an empty tag (so the
    /// caller can omit the metric label rather than emit an empty
    /// string label, which Prometheus collapses).
    pub fn to_wire(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        let mut json = String::with_capacity(32 + self.entries.len() * 24);
        json.push('{');
        for (i, (k, v)) in self.entries.iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            json.push('"');
            json.push_str(&json_escape(k));
            json.push_str("\":\"");
            json.push_str(&json_escape(v));
            json.push('"');
        }
        json.push('}');
        Some(utf8_percent_encode(&json, TAG_ENCODE_SET).to_string())
    }
}

/// Minimal JSON string-escape (Bourne-shell-style; covers the small
/// alphabet our keys/values can carry per `is_valid_key` and the
/// 128-byte value cap). Avoids pulling `serde_json::to_string` for a
/// 16-key map on the hot path.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// Errors surfaced by [`TagSet::from_wire`] / [`TagSet::from_pairs`].
/// Per the contract, callers map any of these to "header absent" — the
/// error is logged at debug level rather than surfaced to Trino.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Empty header value or empty post-decode payload.
    Empty,
    /// Decoded payload exceeded [`MAX_DECODED_BYTES`].
    Oversized { bytes: usize },
    /// Header value carried bytes that did not URL-decode to valid UTF-8.
    BadEncoding,
    /// Decoded payload was not a JSON object literal.
    NotObject,
    /// JSON contained nested objects, arrays, or `null`.
    UnsupportedShape,
    /// JSON parse error.
    Json,
    /// Map exceeded [`MAX_KEYS`] entries.
    TooManyKeys { got: usize, cap: usize },
    /// Key did not match `[A-Za-z_][A-Za-z0-9_]{0,63}`.
    BadKey(String),
    /// Coerced value exceeded [`MAX_VALUE_BYTES`].
    ValueTooLong { key: String, bytes: usize },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Empty => f.write_str("empty X-Shelf-Tag payload"),
            ParseError::Oversized { bytes } => {
                write!(
                    f,
                    "X-Shelf-Tag payload {bytes} B exceeds {MAX_DECODED_BYTES} B cap"
                )
            }
            ParseError::BadEncoding => f.write_str("X-Shelf-Tag URL-decoded to invalid UTF-8"),
            ParseError::NotObject => f.write_str("X-Shelf-Tag is not a JSON object"),
            ParseError::UnsupportedShape => {
                f.write_str("X-Shelf-Tag JSON contained nested object/array/null")
            }
            ParseError::Json => f.write_str("X-Shelf-Tag JSON parse error"),
            ParseError::TooManyKeys { got, cap } => {
                write!(f, "X-Shelf-Tag has {got} keys; cap is {cap}")
            }
            ParseError::BadKey(k) => write!(f, "X-Shelf-Tag rejected key {k:?}"),
            ParseError::ValueTooLong { key, bytes } => write!(
                f,
                "X-Shelf-Tag value for {key:?} is {bytes} B; cap is {MAX_VALUE_BYTES}"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

impl TagSet {
    /// Parse the URL-encoded JSON wire form (the `X-Shelf-Tag` header
    /// value) into a validated tag set. Empty header value is
    /// [`ParseError::Empty`]; callers map either Empty or any other
    /// `Err` to "tag absent".
    pub fn from_wire(raw: &str) -> Result<Self, ParseError> {
        if raw.is_empty() {
            return Err(ParseError::Empty);
        }
        // Cheap pre-check: the encoded form is at least as long as the
        // decoded form, so anything wildly over the cap is rejected
        // without allocating a decode buffer.
        if raw.len() > MAX_DECODED_BYTES * 4 {
            return Err(ParseError::Oversized { bytes: raw.len() });
        }
        let decoded = percent_decode_str(raw)
            .decode_utf8()
            .map_err(|_| ParseError::BadEncoding)?;
        if decoded.is_empty() {
            return Err(ParseError::Empty);
        }
        if decoded.len() > MAX_DECODED_BYTES {
            return Err(ParseError::Oversized {
                bytes: decoded.len(),
            });
        }

        let value: serde_json::Value =
            serde_json::from_str(&decoded).map_err(|_| ParseError::Json)?;
        let obj = match value {
            serde_json::Value::Object(map) => map,
            _ => return Err(ParseError::NotObject),
        };
        if obj.len() > MAX_KEYS {
            return Err(ParseError::TooManyKeys {
                got: obj.len(),
                cap: MAX_KEYS,
            });
        }

        let mut pairs: Vec<(String, String)> = Vec::with_capacity(obj.len());
        for (k, v) in obj {
            let coerced = match v {
                serde_json::Value::String(s) => s,
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Null
                | serde_json::Value::Array(_)
                | serde_json::Value::Object(_) => return Err(ParseError::UnsupportedShape),
            };
            pairs.push((k, coerced));
        }
        Self::from_pairs(pairs)
    }
}

/// Per-request context attached to a shim handler. Built by
/// [`extract_from_headers`] and dropped at the end of the request.
///
/// Holding the resolved label as a `String` directly (rather than a
/// callback into [`AbTagState`]) keeps the hot path lock-free for
/// readers — the cap-check + interning happened once at request entry.
#[derive(Debug, Clone)]
pub struct TaggedContext {
    /// The validated tag set. Empty when the header was absent or
    /// rejected.
    pub tag: TagSet,
    /// Pre-resolved metric-label string. `None` means "do not attach a
    /// `tag` label to this request's metrics" (header absent, ab-tag
    /// disabled, or empty tag set).
    label: Option<String>,
}

impl TaggedContext {
    /// Empty context — header absent or feature disabled.
    pub fn empty() -> Self {
        Self {
            tag: TagSet::empty(),
            label: None,
        }
    }

    /// Returns the metric-label string callers should attach to
    /// per-tag metrics. `None` means "no `tag` dimension on this
    /// request's metrics" — either the feature is disabled or the
    /// request had no tag.
    pub fn metric_label(&self) -> Option<&str> {
        self.label.as_deref()
    }
}

/// Per-pod state that owns the cardinality cap and the cap-violation
/// bookkeeping. One instance lives on `ServerState`, set up at
/// `main` boot.
#[derive(Debug)]
pub struct AbTagState {
    enabled: AtomicBool,
    /// Maximum number of distinct *non-sentinel* tag wire forms we
    /// will publish as a `tag` label within a scrape window.
    max_distinct_tags: AtomicU64,
    /// Length of one scrape window, in milliseconds. The window
    /// rotates by zeroing the live set; we treat the operator's
    /// Prometheus scrape interval as the canonical window.
    scrape_window: AtomicU64,
    /// Cap-violation-warning gate. We log + bump
    /// `shelf_ab_tag_cap_violations_total` exactly once per (window,
    /// distinct-offending-tag).
    inner: RwLock<AbTagInner>,
}

#[derive(Debug)]
struct AbTagInner {
    /// Wall-clock start of the current window. Resets whenever
    /// `Instant::now() - window_start > scrape_window`.
    window_start: Instant,
    /// Tag wire forms admitted under the cap during the current
    /// window.
    admitted: std::collections::HashSet<String>,
    /// Tag wire forms that have already been counted as a violation
    /// in the current window — used so we don't re-warn on every
    /// request that lands the same offending tag.
    warned: std::collections::HashSet<String>,
}

impl AbTagState {
    /// Construct an `AbTagState` with operator-configured limits.
    pub fn new(enabled: bool, max_distinct_tags: usize, scrape_window: Duration) -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(enabled),
            max_distinct_tags: AtomicU64::new(max_distinct_tags as u64),
            scrape_window: AtomicU64::new(scrape_window.as_millis() as u64),
            inner: RwLock::new(AbTagInner {
                window_start: Instant::now(),
                admitted: std::collections::HashSet::new(),
                warned: std::collections::HashSet::new(),
            }),
        })
    }

    /// Disabled instance — used when the operator has not opted in.
    pub fn disabled() -> Arc<Self> {
        Self::new(false, DEFAULT_MAX_DISTINCT_TAGS, DEFAULT_SCRAPE_WINDOW)
    }

    /// Returns `true` if the receive path is active. The Trino-side
    /// forwarding has no kill-switch, but the daemon side does so an
    /// operator can dial cap-driven cardinality back in an incident.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Toggle the receive path at runtime. Returns the prior value.
    pub fn set_enabled(&self, enabled: bool) -> bool {
        self.enabled.swap(enabled, Ordering::Release)
    }

    fn cap(&self) -> usize {
        self.max_distinct_tags.load(Ordering::Relaxed) as usize
    }

    fn window(&self) -> Duration {
        Duration::from_millis(self.scrape_window.load(Ordering::Relaxed))
    }

    /// Resolve a (validated) tag set to the metric-label string
    /// callers should attach. Bumps the cap-violation counter exactly
    /// once per (window, distinct-offending-tag).
    ///
    /// Returns `None` when:
    /// - the receive path is disabled,
    /// - the tag is empty.
    ///
    /// Returns `Some(OTHER_TAG_LABEL)` when the cap would be exceeded;
    /// otherwise returns the canonical wire form.
    pub fn tag_label_for(&self, tag: &TagSet) -> Option<String> {
        if !self.is_enabled() || tag.is_empty() {
            return None;
        }
        let wire = tag.to_wire()?;
        let cap = self.cap();
        let window = self.window();

        let mut inner = self.inner.write();
        if inner.window_start.elapsed() > window {
            inner.window_start = Instant::now();
            inner.admitted.clear();
            inner.warned.clear();
        }
        if inner.admitted.contains(&wire) || inner.admitted.len() < cap {
            inner.admitted.insert(wire.clone());
            return Some(wire);
        }
        // Over the cap — fall back to the sentinel and (once per
        // window per offending tag) bump the cap-violation counter.
        if inner.warned.insert(wire.clone()) {
            crate::metrics::AB_TAG_CAP_VIOLATIONS_TOTAL
                .with_label_values(&["cardinality"])
                .inc();
            tracing::warn!(
                offending_tag = %wire,
                cap = cap,
                window_ms = window.as_millis() as u64,
                "shelf_ab_tag: cardinality cap reached; folding into 'other' sentinel for the rest of the window",
            );
        }
        Some(OTHER_TAG_LABEL.to_owned())
    }
}

/// Extract a [`TaggedContext`] from a request's headers using the
/// supplied cap state. The caller wires the resulting context into the
/// per-request flow; nothing in this module reads it back via global
/// state.
pub fn extract_from_headers<'h, I, V>(headers: I, state: &AbTagState) -> TaggedContext
where
    I: IntoIterator<Item = (&'h str, V)>,
    V: AsRef<[u8]>,
{
    if !state.is_enabled() {
        return TaggedContext::empty();
    }
    // Two-pass over the iterator is impossible (it is consumed), so we
    // pull the matching header bytes into a local Vec first; that lets
    // us reject duplicates and parse without fighting borrow lifetimes
    // tied to the iterator's borrowed `value`.
    let mut matched: Vec<Vec<u8>> = Vec::with_capacity(1);
    for (name, value) in headers {
        if !name.eq_ignore_ascii_case(HEADER_NAME) {
            continue;
        }
        matched.push(value.as_ref().to_vec());
        if matched.len() > 1 {
            // Multiple X-Shelf-Tag headers ⇒ reject (per contract). We
            // do not pick "first wins" because the contract is explicit
            // that >1 instance is malformed.
            tracing::debug!("shelf_ab_tag: rejected request carrying >1 X-Shelf-Tag header");
            return TaggedContext::empty();
        }
    }
    let bytes = match matched.first() {
        Some(b) => b.as_slice(),
        None => return TaggedContext::empty(),
    };
    if bytes.iter().any(|b| !b.is_ascii()) {
        return TaggedContext::empty();
    }
    let raw = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return TaggedContext::empty(),
    };
    match TagSet::from_wire(raw) {
        Ok(tag) => {
            let label = state.tag_label_for(&tag);
            TaggedContext { tag, label }
        }
        Err(e) => {
            tracing::debug!(error = %e, "shelf_ab_tag: header rejected; treating as absent");
            TaggedContext::empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread::sleep;

    fn parse(raw: &str) -> Result<TagSet, ParseError> {
        TagSet::from_wire(raw)
    }

    #[test]
    fn empty_or_missing_yields_empty_tag() {
        let tag = TagSet::empty();
        assert!(tag.is_empty());
        assert!(tag.to_wire().is_none());
        assert_eq!(parse(""), Err(ParseError::Empty));
    }

    #[test]
    fn valid_single_pair_round_trips() {
        let raw = "%7B%22experiment%22%3A%22b1_compression_on%22%7D";
        let tag = parse(raw).expect("valid wire form");
        assert_eq!(tag.get("experiment"), Some("b1_compression_on"));
        assert_eq!(tag.iter().count(), 1);
        assert_eq!(tag.to_wire().as_deref(), Some(raw));
    }

    #[test]
    fn keys_are_sorted_lexicographically_in_wire_form() {
        let unsorted_input = "%7B%22experiment%22%3A%22b1%22%2C%22cohort%22%3A%22rep1%22%7D";
        let tag = parse(unsorted_input).expect("valid");
        let sorted_wire = tag.to_wire().expect("non-empty");
        // After normalisation, "cohort" must appear before "experiment".
        let decoded = percent_decode_str(&sorted_wire).decode_utf8().unwrap();
        let cohort_idx = decoded.find("cohort").unwrap();
        let experiment_idx = decoded.find("experiment").unwrap();
        assert!(cohort_idx < experiment_idx, "decoded={decoded}");
    }

    #[test]
    fn coerces_int_and_bool_values() {
        let raw = "%7B%22epoch%22%3A123%2C%22on%22%3Atrue%7D";
        let tag = parse(raw).expect("valid wire form");
        assert_eq!(tag.get("epoch"), Some("123"));
        assert_eq!(tag.get("on"), Some("true"));
    }

    #[test]
    fn rejects_nested_array() {
        let raw = "%7B%22a%22%3A%5B1%5D%7D";
        assert_eq!(parse(raw), Err(ParseError::UnsupportedShape));
    }

    #[test]
    fn rejects_nested_object() {
        let raw = "%7B%22a%22%3A%7B%22b%22%3A1%7D%7D";
        assert_eq!(parse(raw), Err(ParseError::UnsupportedShape));
    }

    #[test]
    fn rejects_null_value() {
        let raw = "%7B%22a%22%3Anull%7D";
        assert_eq!(parse(raw), Err(ParseError::UnsupportedShape));
    }

    #[test]
    fn rejects_non_object_root() {
        let raw = "%5B1%2C2%5D";
        assert_eq!(parse(raw), Err(ParseError::NotObject));
    }

    #[test]
    fn rejects_oversized_payload() {
        let big_value = "x".repeat(MAX_DECODED_BYTES + 50);
        let json = format!("{{\"a\":\"{}\"}}", big_value);
        let wire = utf8_percent_encode(&json, TAG_ENCODE_SET).to_string();
        match parse(&wire) {
            Err(ParseError::Oversized { .. }) => {}
            other => panic!("expected Oversized, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_key() {
        let json = "{\"1bad\":\"v\"}";
        let wire = utf8_percent_encode(json, TAG_ENCODE_SET).to_string();
        match parse(&wire) {
            Err(ParseError::BadKey(k)) => assert_eq!(k, "1bad"),
            other => panic!("expected BadKey, got {other:?}"),
        }
    }

    #[test]
    fn rejects_value_too_long() {
        let val = "x".repeat(MAX_VALUE_BYTES + 1);
        let json = format!("{{\"a\":\"{}\"}}", val);
        let wire = utf8_percent_encode(&json, TAG_ENCODE_SET).to_string();
        match parse(&wire) {
            Err(ParseError::ValueTooLong { key, bytes }) => {
                assert_eq!(key, "a");
                assert_eq!(bytes, MAX_VALUE_BYTES + 1);
            }
            other => panic!("expected ValueTooLong, got {other:?}"),
        }
    }

    #[test]
    fn rejects_too_many_keys() {
        let mut json = String::from("{");
        for i in 0..(MAX_KEYS + 1) {
            if i > 0 {
                json.push(',');
            }
            json.push_str(&format!("\"k{i}\":\"v\""));
        }
        json.push('}');
        let wire = utf8_percent_encode(&json, TAG_ENCODE_SET).to_string();
        match parse(&wire) {
            Err(ParseError::TooManyKeys { got, cap }) => {
                assert_eq!(got, MAX_KEYS + 1);
                assert_eq!(cap, MAX_KEYS);
            }
            other => panic!("expected TooManyKeys, got {other:?}"),
        }
    }

    #[test]
    fn cardinality_cap_folds_excess_into_other_sentinel() {
        // The cap-violation counter is a `Lazy<IntCounterVec>`; it
        // self-initialises on first `with_label_values` call. Do NOT
        // call `crate::metrics::Registry::init()` here — that would
        // double-register collectors on the shared `REGISTRY` and
        // poison `metrics::tests`' own initialisation barrier.
        let cap = 16usize;
        let state = AbTagState::new(true, cap, Duration::from_secs(60));
        let mut admitted = 0usize;
        let mut other_count = 0usize;
        for i in 0..32 {
            let json = format!("{{\"experiment\":\"t{i}\"}}");
            let wire = utf8_percent_encode(&json, TAG_ENCODE_SET).to_string();
            let tag = TagSet::from_wire(&wire).expect("valid");
            match state.tag_label_for(&tag) {
                Some(l) if l == OTHER_TAG_LABEL => other_count += 1,
                Some(_) => admitted += 1,
                None => panic!("disabled state inside test"),
            }
        }
        assert_eq!(admitted, cap, "admitted should equal cap");
        assert_eq!(other_count, 32 - cap, "remainder should fold into 'other'");
        // A single bump per scrape window per offending tag — for 16
        // distinct over-cap tags, the violation counter must be 16
        // (one bump each, not one per request). See contract §4.
        let scraped = crate::metrics::AB_TAG_CAP_VIOLATIONS_TOTAL
            .with_label_values(&["cardinality"])
            .get();
        assert!(
            scraped >= 16,
            "expected >= 16 cap violations counted; got {scraped}"
        );
    }

    #[test]
    fn cardinality_cap_resets_after_window() {
        // Same Lazy-static rationale as `cardinality_cap_folds_*`: do
        // NOT call `Registry::init()` from here.
        // Tiny window so the test stays fast.
        let state = AbTagState::new(true, 1, Duration::from_millis(50));
        let json_a = "{\"experiment\":\"a\"}";
        let json_b = "{\"experiment\":\"b\"}";
        let wire_a = utf8_percent_encode(json_a, TAG_ENCODE_SET).to_string();
        let wire_b = utf8_percent_encode(json_b, TAG_ENCODE_SET).to_string();
        let tag_a = TagSet::from_wire(&wire_a).unwrap();
        let tag_b = TagSet::from_wire(&wire_b).unwrap();

        // First admit succeeds, second is over the cap of 1.
        assert_ne!(state.tag_label_for(&tag_a).unwrap(), OTHER_TAG_LABEL);
        assert_eq!(state.tag_label_for(&tag_b).unwrap(), OTHER_TAG_LABEL);

        // After the scrape window rolls over, both new tags admit again.
        sleep(Duration::from_millis(80));
        // First call after the rollover triggers the window reset.
        let label_b_after = state.tag_label_for(&tag_b).unwrap();
        assert_ne!(
            label_b_after, OTHER_TAG_LABEL,
            "tag B should admit fresh in the new window; got {label_b_after}"
        );
    }

    #[test]
    fn extract_from_headers_skips_when_disabled() {
        let state = AbTagState::disabled();
        let raw = "%7B%22experiment%22%3A%22b1_on%22%7D";
        let ctx = extract_from_headers(vec![("X-Shelf-Tag", raw.as_bytes())], &state);
        assert!(ctx.metric_label().is_none());
        assert!(ctx.tag.is_empty());
    }

    #[test]
    fn extract_from_headers_returns_none_when_header_absent() {
        let state = AbTagState::new(true, 16, Duration::from_secs(60));
        let ctx: TaggedContext = extract_from_headers::<Vec<(&str, &[u8])>, &[u8]>(vec![], &state);
        assert!(ctx.metric_label().is_none());
        assert!(ctx.tag.is_empty());
    }

    #[test]
    fn extract_from_headers_rejects_duplicate_header_instances() {
        let state = AbTagState::new(true, 16, Duration::from_secs(60));
        let raw = "%7B%22experiment%22%3A%22b1_on%22%7D";
        let ctx = extract_from_headers(
            vec![
                ("X-Shelf-Tag", raw.as_bytes()),
                ("X-Shelf-Tag", raw.as_bytes()),
            ],
            &state,
        );
        assert!(ctx.tag.is_empty());
        assert!(ctx.metric_label().is_none());
    }

    #[test]
    fn extract_from_headers_treats_malformed_header_as_absent() {
        let state = AbTagState::new(true, 16, Duration::from_secs(60));
        let ctx = extract_from_headers(vec![("x-shelf-tag", b"not-a-json".as_slice())], &state);
        assert!(ctx.tag.is_empty());
        assert!(ctx.metric_label().is_none());
    }

    #[test]
    fn extract_from_headers_returns_label_on_valid_request() {
        let state = AbTagState::new(true, 16, Duration::from_secs(60));
        let raw = "%7B%22experiment%22%3A%22b1_on%22%7D";
        let ctx = extract_from_headers(vec![("X-Shelf-Tag", raw.as_bytes())], &state);
        assert!(!ctx.tag.is_empty());
        let label = ctx.metric_label().unwrap();
        assert!(
            label.contains("experiment") && label.contains("b1_on"),
            "expected wire-form label, got {label}"
        );
    }

    #[derive(serde::Deserialize)]
    struct GoldenVectors {
        vectors: Vec<GoldenVector>,
    }

    #[derive(serde::Deserialize)]
    struct GoldenVector {
        name: String,
        session_props: HashMap<String, String>,
        normalized: Option<HashMap<String, String>>,
        wire: Option<String>,
    }

    /// Parity test against the JSON fixture shared with the Java side.
    /// Regenerate the fixture on both sides whenever the canonical
    /// shape changes; do NOT diverge.
    #[test]
    fn parses_shared_golden_vectors_from_repo_fixture() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/fixtures/ab-tag-vectors.json");
        let raw = std::fs::read_to_string(&path).expect("read ab-tag-vectors.json");
        let parsed: GoldenVectors =
            serde_json::from_str(&raw).expect("ab-tag-vectors.json is valid JSON");
        for v in parsed.vectors {
            // Build a TagSet from the session-prop-derived `normalized`
            // map (the Java side does the same).
            let derived = match v.normalized.clone() {
                None => TagSet::empty(),
                Some(pairs) => TagSet::from_pairs(pairs)
                    .unwrap_or_else(|e| panic!("vector {} parse error: {e:?}", v.name)),
            };
            assert_eq!(
                derived.to_wire(),
                v.wire,
                "vector {}: wire form mismatch",
                v.name
            );
            // Round-trip the wire form back through the parser when present.
            if let Some(wire) = v.wire.as_deref() {
                let parsed_again = TagSet::from_wire(wire)
                    .unwrap_or_else(|e| panic!("vector {}: parse error {:?}", v.name, e));
                assert_eq!(
                    parsed_again.to_wire().as_deref(),
                    Some(wire),
                    "vector {}: re-parse round-trip",
                    v.name
                );
            }
            // Derived map must equal the normalized map.
            if let Some(normalized) = v.normalized {
                let actual: HashMap<String, String> = derived
                    .iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect();
                assert_eq!(
                    actual, normalized,
                    "vector {}: derived map mismatch",
                    v.name
                );
            } else {
                assert!(derived.is_empty(), "vector {}: expected empty", v.name);
            }
            let _ = v.session_props; // referenced by Java side; ignored here.
        }
    }
}
