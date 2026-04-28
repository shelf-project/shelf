//! SHELF E2 — zstd wrapper for Pool::Metadata compression.
//!
//! Metadata pool entries are almost always plain-text JSON
//! (`metadata.json`, Avro manifest summaries) or uncompressed Puffin
//! stats. They compress at roughly 5× on real rep-2 traces. Without
//! any in-cache compression, a 4 GiB metadata DRAM pool effectively
//! holds ~4 GiB of JSON; at the observed compression ratio the same
//! DRAM could hold ~20 GiB of entries, directly lifting hit-rate on
//! the tail of cold manifests.
//!
//! This module is **deliberately small** and feature-gated:
//!
//! - `encode` / `decode` are pure functions — no `Store` entanglement
//!   — so they can be unit-tested in isolation.
//! - The `zstd_metadata` feature flag (defined in
//!   `shelfd/Cargo.toml`) gates whether the store wraps metadata
//!   entries in `CompressedBytes` before admission.
//! - Compression level is fixed at 3 (zstd default). Higher levels
//!   cost CPU without material ratio gain on JSON at the sizes we
//!   cache; lower levels lose ratio.
//!
//! The store wiring is *not* in this file: it will live in
//! `store.rs` behind `#[cfg(feature = "zstd_metadata")]` so the
//! non-feature build continues to compile and run unchanged. That
//! wiring step is described in `docs/design-notes/SHELF-E2-zstd-metadata.md`.

use bytes::Bytes;

/// zstd level — `3` is the library default and the one every serious
/// benchmark publishes for "balanced". Bumping higher gains <5% on
/// JSON at the cache sizes we target but costs 2-4× CPU.
pub const ZSTD_LEVEL: i32 = 3;

/// Inline compressibility floor, in bytes. Anything smaller than
/// this is stored uncompressed because the zstd frame overhead
/// (~13 bytes) dominates any savings. Metadata objects under this
/// threshold are already effectively free to cache.
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

/// Encode `src`. Small inputs are returned tagged-uncompressed so we
/// never pay zstd overhead on sub-frame-sized entries. Returns the
/// tag-prefixed bytes ready to be stored.
pub fn encode(src: &Bytes) -> Result<Bytes, CompressionError> {
    if src.len() < MIN_COMPRESS_BYTES {
        let mut out = Vec::with_capacity(src.len() + 1);
        out.push(FRAME_VERSION_UNCOMPRESSED);
        out.extend_from_slice(src);
        return Ok(Bytes::from(out));
    }
    let compressed = zstd::stream::encode_all(src.as_ref(), ZSTD_LEVEL)
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
}
