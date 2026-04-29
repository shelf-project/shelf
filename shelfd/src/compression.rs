//! Pool-agnostic zstd cache compression.
//!
//! Originally introduced as **SHELF-E2** for the metadata pool —
//! `metadata.json`, manifest lists, manifests, footers — which
//! compress ~5× because the field names repeat. **B1** widened the
//! scope to the row-group hybrid pool's NVMe tier: real Iceberg /
//! Parquet rowgroup payloads compress ~1.4–2.5× because the columnar
//! data is already dictionary-encoded but the per-page headers and
//! repeated string values still win meaningfully under zstd-3. At
//! constant pod count this gives ~+5–10 pp hit ratio; at constant
//! hit ratio it makes the StatefulSet shrinkable by one pod.
//!
//! This module is **deliberately small** and pure:
//!
//! - [`encode`] / [`decode`] are pure functions — no `Store`
//!   entanglement — so they unit-test in isolation.
//! - [`CompressionPipeline`] is the per-pool integration helper:
//!   it owns the configured level + min-size threshold, reports
//!   outcomes onto Prometheus counters, and is wired through
//!   [`crate::store::FoyerStore`] for any pool whose
//!   `compression.enabled` is `true`.
//! - The legacy `zstd_metadata` Cargo feature flag is retained so
//!   downstream consumers that built against it keep compiling, but
//!   B1's wiring is **runtime-toggled** via
//!   `cache.pools.<name>.compression.enabled` in the chart values
//!   so operators can flip per pool without a recompile.
//!
//! ### On-disk safety contract
//!
//! Encoded frames are **header-tagged** so the same byte stream can
//! distinguish "uncompressed (legacy or below-threshold)" from
//! "zstd". That handles a single config epoch cleanly. **It does
//! not** handle a hot-flip from compression-off → compression-on
//! against an already-populated NVMe ring: pre-flip raw row-group
//! bytes were written without any header byte, so byte 0 is
//! arbitrary Parquet content, and the post-flip decoder cannot
//! tell those apart from a header byte. The store layer therefore
//! gates each hybrid pool with a `.shelf-compression.json` marker
//! file (see `FoyerStore::ensure_compression_marker`) and aborts at
//! boot rather than corrupt reads silently.

use std::time::Instant;

use bytes::Bytes;

/// Default zstd level — `3` is the library default and the one every
/// serious benchmark publishes for "balanced". Bumping higher gains
/// <5% on JSON at the cache sizes we target but costs 2-4× CPU.
pub const ZSTD_LEVEL: i32 = 3;

/// Default inline compressibility floor, in bytes. Anything smaller
/// than this is stored uncompressed because the zstd frame overhead
/// (~13 bytes) dominates any savings. Tunable per pool via
/// `cache.pools.<name>.compression.minSizeBytes`.
pub const MIN_COMPRESS_BYTES: usize = 256;

/// A header byte prepended to cached bytes so the store can
/// distinguish "never compressed" (legacy) entries from new
/// compressed entries without scanning for a zstd magic number.
pub const FRAME_VERSION_UNCOMPRESSED: u8 = 0x00;
/// Magic value for a compressed frame: ASCII 'Z'. The body is a
/// standard zstd frame — no custom container, no checksum (zstd has
/// its own).
pub const FRAME_VERSION_ZSTD: u8 = 0x5A;

/// Error surface for the compression layer.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("zstd encode error: {0}")]
    Encode(String),
    #[error("zstd decode error: {0}")]
    Decode(String),
    #[error("cached frame header corrupted: byte 0 = 0x{0:02x}")]
    CorruptHeader(u8),
}

/// Encode `src` at the default zstd level / minimum size. Equivalent
/// to `encode_with(src, ZSTD_LEVEL, MIN_COMPRESS_BYTES)`.
pub fn encode(src: &Bytes) -> Result<Bytes, CompressionError> {
    encode_with(src, ZSTD_LEVEL, MIN_COMPRESS_BYTES)
}

/// Encode `src` at the given zstd level, bypassing zstd entirely
/// when the input is below `min_compress_bytes`. Returns the
/// tag-prefixed bytes ready to be stored.
pub fn encode_with(
    src: &Bytes,
    level: i32,
    min_compress_bytes: usize,
) -> Result<Bytes, CompressionError> {
    if src.len() < min_compress_bytes {
        let mut out = Vec::with_capacity(src.len() + 1);
        out.push(FRAME_VERSION_UNCOMPRESSED);
        out.extend_from_slice(src);
        return Ok(Bytes::from(out));
    }
    let compressed = zstd::stream::encode_all(src.as_ref(), level)
        .map_err(|e| CompressionError::Encode(e.to_string()))?;
    // Refuse to "compress" something that got bigger. This is common
    // for already-compressed payloads (gzipped JSON, Snappy-encoded
    // Avro) and we should not inflate them.
    if compressed.len() + 1 >= src.len() + 1 {
        let mut out = Vec::with_capacity(src.len() + 1);
        out.push(FRAME_VERSION_UNCOMPRESSED);
        out.extend_from_slice(src);
        return Ok(Bytes::from(out));
    }
    let mut out = Vec::with_capacity(compressed.len() + 1);
    out.push(FRAME_VERSION_ZSTD);
    out.extend_from_slice(&compressed);
    Ok(Bytes::from(out))
}

/// Decode the reverse of [`encode`]. Callers hand in bytes that came
/// out of the metadata pool. Returns the original payload.
pub fn decode(src: &Bytes) -> Result<Bytes, CompressionError> {
    let Some((&tag, rest)) = src.split_first() else {
        return Ok(Bytes::new());
    };
    match tag {
        FRAME_VERSION_UNCOMPRESSED => Ok(Bytes::copy_from_slice(rest)),
        FRAME_VERSION_ZSTD => {
            let decompressed = zstd::stream::decode_all(rest)
                .map_err(|e| CompressionError::Decode(e.to_string()))?;
            Ok(Bytes::from(decompressed))
        }
        other => Err(CompressionError::CorruptHeader(other)),
    }
}

/// Metrics-friendly descriptor of a compression outcome so the
/// caller can attribute admission-weight to the right bucket.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CompressionOutcome {
    /// Input was smaller than `MIN_COMPRESS_BYTES`; no zstd.
    SkippedSmall,
    /// zstd made the payload bigger; we fell back to uncompressed.
    SkippedIncompressible,
    /// Normal path — stored compressed.
    Compressed { ratio_x100: u32 },
}

/// Helper for benchmarks + observability. Reports how much smaller
/// the zstd-encoded frame is as an integer 0..=100. A ratio of 80
/// means the encoded frame is 20% of the original.
pub fn inspect(src: &Bytes) -> Result<CompressionOutcome, CompressionError> {
    let encoded = encode(src)?;
    Ok(classify(src.len(), encoded.len()))
}

fn classify(original: usize, encoded: usize) -> CompressionOutcome {
    if original < MIN_COMPRESS_BYTES {
        CompressionOutcome::SkippedSmall
    } else if encoded + 1 >= original + 1 && encoded > original {
        CompressionOutcome::SkippedIncompressible
    } else if encoded >= original {
        // Encoded includes the 1-byte tag — if it equals original we
        // also classify as "not worth it".
        CompressionOutcome::SkippedIncompressible
    } else {
        let ratio_x100 =
            ((original.saturating_sub(encoded) as u64) * 100 / original.max(1) as u64) as u32;
        CompressionOutcome::Compressed { ratio_x100 }
    }
}

/// Per-pool integration helper.
///
/// One [`CompressionPipeline`] is constructed per Foyer pool whose
/// `compression.enabled` is `true` and is held inside
/// [`crate::store::FoyerStore`]. The pipeline owns the configured
/// zstd level + min-size threshold and pushes its outcomes onto the
/// pool-labelled Prometheus families in [`crate::metrics`].
///
/// Cheap to clone (only `Copy`-ish state).
#[derive(Debug, Clone)]
pub struct CompressionPipeline {
    pool_label: &'static str,
    level: i32,
    min_size_bytes: usize,
}

impl CompressionPipeline {
    /// Build a new pipeline for `pool_label` (the same `"metadata"` /
    /// `"rowgroup"` strings used by the rest of the metric registry).
    pub fn new(pool_label: &'static str, level: i32, min_size_bytes: usize) -> Self {
        Self {
            pool_label,
            level,
            min_size_bytes,
        }
    }

    /// `<algo>@<level>` descriptor for marker files / `/stats`. The
    /// shape is stable so the marker file can refuse a config that
    /// would change either dimension on a populated NVMe ring.
    pub fn descriptor(&self) -> String {
        format!("zstd@{}", self.level)
    }

    /// Encode `src` for storage. Pushes outcome counters and an
    /// `encode` latency histogram observation onto the pool's
    /// metric family. Returns the tag-prefixed bytes ready to hand
    /// to Foyer.
    pub fn encode_for_store(&self, src: &Bytes) -> Result<Bytes, CompressionError> {
        let started = Instant::now();
        let original = src.len();
        let encoded = encode_with(src, self.level, self.min_size_bytes)?;
        let elapsed = started.elapsed();

        crate::metrics::COMPRESS_BYTES_IN_TOTAL
            .with_label_values(&[self.pool_label])
            .inc_by(original as u64);
        crate::metrics::COMPRESS_BYTES_OUT_TOTAL
            .with_label_values(&[self.pool_label])
            .inc_by(encoded.len() as u64);

        let outcome_label = match classify(original, encoded.len()) {
            CompressionOutcome::SkippedSmall => "skipped_small",
            CompressionOutcome::SkippedIncompressible => "skipped_incompressible",
            CompressionOutcome::Compressed { .. } => "compressed",
        };
        crate::metrics::COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&[self.pool_label, outcome_label])
            .inc();
        crate::metrics::COMPRESS_SECONDS
            .with_label_values(&[self.pool_label, "encode"])
            .observe(elapsed.as_secs_f64());

        Ok(encoded)
    }

    /// Decode `stored` into the original payload. Pushes a
    /// `decode` latency observation and an `outcome` counter so
    /// dashboards see the split between "actually decompressed" and
    /// "frame was tagged uncompressed".
    pub fn decode_from_store(&self, stored: &Bytes) -> Result<Bytes, CompressionError> {
        let started = Instant::now();
        // Empty stored frame → empty payload (matches `decode`'s
        // contract). Bump the latency histogram even on this path
        // so dashboards see the no-op cost is non-zero.
        let outcome_label = match stored.first() {
            Some(&FRAME_VERSION_UNCOMPRESSED) => "decompressed_uncompressed",
            Some(&FRAME_VERSION_ZSTD) => "decompressed_ok",
            Some(_) => "decompress_error",
            None => "decompressed_uncompressed",
        };
        let result = decode(stored);
        let elapsed = started.elapsed();
        crate::metrics::COMPRESS_OUTCOMES_TOTAL
            .with_label_values(&[self.pool_label, outcome_label])
            .inc();
        crate::metrics::COMPRESS_SECONDS
            .with_label_values(&[self.pool_label, "decode"])
            .observe(elapsed.as_secs_f64());
        result
    }

    /// Pre-touch every metric child this pipeline can ever bump so
    /// dashboards never have to special-case "metric not yet present"
    /// vs "value is genuinely zero". Same pattern as
    /// `FoyerStore::open`'s pool-pretouch loop.
    pub fn pre_touch_metrics(&self) {
        for outcome in [
            "compressed",
            "skipped_small",
            "skipped_incompressible",
            "decompressed_ok",
            "decompressed_uncompressed",
            "decompress_error",
        ] {
            crate::metrics::COMPRESS_OUTCOMES_TOTAL
                .with_label_values(&[self.pool_label, outcome])
                .inc_by(0);
        }
        crate::metrics::COMPRESS_BYTES_IN_TOTAL
            .with_label_values(&[self.pool_label])
            .inc_by(0);
        crate::metrics::COMPRESS_BYTES_OUT_TOTAL
            .with_label_values(&[self.pool_label])
            .inc_by(0);
    }

    pub fn pool_label(&self) -> &'static str {
        self.pool_label
    }

    pub fn level(&self) -> i32 {
        self.level
    }

    pub fn min_size_bytes(&self) -> usize {
        self.min_size_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_inputs_are_tagged_uncompressed() {
        let payload = Bytes::from_static(b"hi");
        let encoded = encode(&payload).unwrap();
        assert_eq!(encoded[0], FRAME_VERSION_UNCOMPRESSED);
        assert_eq!(decode(&encoded).unwrap(), payload);
    }

    #[test]
    fn json_like_inputs_compress_and_round_trip() {
        // 8 KiB of synthetic manifest JSON; should compress
        // comfortably above 4× since the field names repeat.
        let unit = br#"{"file":"s3://bucket/foo/bar.parquet","length":12345,"partition":null},"#;
        let mut payload = Vec::with_capacity(8 * 1024);
        while payload.len() < 8 * 1024 {
            payload.extend_from_slice(unit);
        }
        let payload = Bytes::from(payload);
        let encoded = encode(&payload).unwrap();
        assert_eq!(encoded[0], FRAME_VERSION_ZSTD);
        assert!(
            encoded.len() * 4 < payload.len(),
            "expected ≥ 4x on JSON: {} → {}",
            payload.len(),
            encoded.len()
        );
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn incompressible_inputs_fall_back_to_uncompressed() {
        // Random bytes don't compress. We expect the encoder to
        // fall back to FRAME_VERSION_UNCOMPRESSED so we never
        // inflate.
        let mut payload = vec![0u8; 4096];
        // Deterministic high-entropy fill via splitmix64. Good
        // enough to defeat zstd level 3 on 4 KiB.
        let mut x: u64 = 0xDEADBEEF_CAFEBABE;
        for chunk in payload.chunks_mut(8) {
            x = x.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            let bytes = z.to_le_bytes();
            for (i, b) in chunk.iter_mut().enumerate() {
                *b = bytes[i];
            }
        }
        let payload = Bytes::from(payload);
        let encoded = encode(&payload).unwrap();
        assert_eq!(
            encoded[0], FRAME_VERSION_UNCOMPRESSED,
            "random bytes must not inflate"
        );
        assert_eq!(encoded.len(), payload.len() + 1);
        assert_eq!(decode(&encoded).unwrap(), payload);
    }

    #[test]
    fn corrupt_header_is_rejected() {
        let bogus = Bytes::from_static(&[0xAB, 0x01, 0x02]);
        assert!(matches!(
            decode(&bogus),
            Err(CompressionError::CorruptHeader(0xAB))
        ));
    }

    #[test]
    fn inspect_reports_compressed_outcome_for_json() {
        let payload = Bytes::from(
            br#"{"snapshot":1234567890,"manifests":["a","a","a","a","a","a","a","a","a","a","a","a","a","a","a","a"]}"#
                .repeat(32),
        );
        match inspect(&payload).unwrap() {
            CompressionOutcome::Compressed { ratio_x100 } => {
                assert!(ratio_x100 > 30, "expected >30% saved, got {ratio_x100}");
            }
            other => panic!("expected Compressed, got {other:?}"),
        }
    }

    #[test]
    fn inspect_reports_small_outcome() {
        let payload = Bytes::from_static(b"tiny");
        assert_eq!(inspect(&payload).unwrap(), CompressionOutcome::SkippedSmall);
    }

    #[test]
    fn empty_roundtrip() {
        let payload = Bytes::new();
        let encoded = encode(&payload).unwrap();
        assert_eq!(encoded[0], FRAME_VERSION_UNCOMPRESSED);
        assert_eq!(decode(&encoded).unwrap(), payload);
    }

    #[test]
    fn pipeline_descriptor_is_stable() {
        let pipe = CompressionPipeline::new("rowgroup", 3, 256);
        assert_eq!(pipe.descriptor(), "zstd@3");
        assert_eq!(pipe.level(), 3);
        assert_eq!(pipe.min_size_bytes(), 256);
        assert_eq!(pipe.pool_label(), "rowgroup");
    }

    #[test]
    fn pipeline_round_trip_via_helper() {
        let pipe = CompressionPipeline::new("rowgroup", 3, 256);
        let unit = br#"{"row":"abcdefghijklmnopqrstuvwxyz","val":1234567890}"#;
        let mut payload = Vec::with_capacity(8 * 1024);
        while payload.len() < 8 * 1024 {
            payload.extend_from_slice(unit);
        }
        let payload = Bytes::from(payload);
        let encoded = pipe.encode_for_store(&payload).unwrap();
        // We rely on the underlying `encode_with` here; the pipeline
        // adds telemetry but must not alter the on-the-wire shape.
        assert_eq!(encoded[0], FRAME_VERSION_ZSTD);
        let decoded = pipe.decode_from_store(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn pipeline_passes_through_uncompressed_frame() {
        let pipe = CompressionPipeline::new("rowgroup", 3, 256);
        // Below threshold: encoded as `[0x00, ..raw..]`.
        let payload = Bytes::from_static(b"hello world (under threshold)");
        let encoded = pipe.encode_for_store(&payload).unwrap();
        assert_eq!(encoded[0], FRAME_VERSION_UNCOMPRESSED);
        let decoded = pipe.decode_from_store(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }
}
