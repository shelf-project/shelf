//! SHELF-25 — decoder for AWS SigV4 streaming chunked transfer
//! encoding (`Content-Encoding: aws-chunked`).
//!
//! ## Why this exists
//!
//! The AWS SDK's "streaming-signed" PUT path (the default for any
//! body large enough to bother streaming, including every Iceberg
//! `metadata.json` Trino's native S3 client writes) wraps the
//! request body in a chunk-framed envelope:
//!
//! ```text
//! <size-hex>;chunk-signature=<sig>\r\n
//! <size bytes of payload>\r\n
//! <size-hex>;chunk-signature=<sig>\r\n
//! <size bytes of payload>\r\n
//! ...
//! 0;chunk-signature=<sig>\r\n
//! \r\n
//! ```
//!
//! Real S3 unwraps this transparently when it observes the headers
//! `Content-Encoding: aws-chunked` plus
//! `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
//! (or any `STREAMING-*` variant). The chunk-signature embedded in
//! each header is part of the signing chain S3 validates.
//!
//! shelfd's shim re-uploads via the SDK's `PutObject`/`UploadPart`
//! and the SDK applies its own (regular, non-streaming) SigV4
//! signing to the body bytes we hand it. We therefore have to
//! **strip** the `aws-chunked` envelope before the SDK sees the
//! body — otherwise the chunk-size hex + `chunk-signature=…\r\n`
//! lines are persisted to S3 verbatim, corrupting every metadata.json
//! Trino writes through the shim. (See `docs/rollout-v1/rca-stage0bc.md`,
//! H4: 50 MiB sample at ETag `"0b30b19205ec71d2b31e8bac15a61830-2"`.)
//!
//! ## Trust model
//!
//! We do **not** validate the chunk-signature ourselves. The signer
//! (AWS SDK on the Trino worker) already ran SigV4 over the original
//! request; the only consumer that could re-validate the streaming
//! chain is real S3 itself, and we are explicitly the unwrapper, not
//! a proxy. The signature is parsed-and-discarded.
//!
//! ## Robustness
//!
//! - Hex size is parsed strictly: only `0-9 a-f A-F`, no leading
//!   `0x`, no whitespace, no Unicode digits.
//! - Trailing `\r\n` after each chunk body is required.
//! - The terminating `0`-sized chunk is required; trailers (after
//!   the final `\r\n`) are accepted and discarded — AWS's v4
//!   streaming spec does not sign chunk trailers, so any bytes
//!   that follow the final `\r\n` are advisory.
//! - Returns a typed [`AwsChunkedError`] on malformed input rather
//!   than panicking; callers map it to a 400 `InvalidRequest`
//!   response.

use bytes::{Bytes, BytesMut};

/// Errors surfaced by [`decode_aws_chunked`].
#[derive(Debug, thiserror::Error)]
pub enum AwsChunkedError {
    /// Stream ended before the terminating zero-sized chunk.
    #[error("aws-chunked: stream truncated at offset {offset}")]
    Truncated { offset: usize },

    /// Chunk header was missing the required `\r\n` terminator
    /// (after the optional `;chunk-signature=…` extension).
    #[error("aws-chunked: chunk header missing CRLF at offset {offset}")]
    MissingHeaderCrlf { offset: usize },

    /// Chunk-size token was empty or contained non-hex characters.
    #[error("aws-chunked: invalid hex size {token:?} at offset {offset}")]
    InvalidHexSize { offset: usize, token: String },

    /// Chunk size declared N bytes but the stream had fewer than N
    /// remaining (or the trailing `\r\n` was absent).
    #[error(
        "aws-chunked: chunk body short by {missing} bytes \
         (declared {declared}) at offset {offset}"
    )]
    BodyTooShort {
        offset: usize,
        declared: usize,
        missing: usize,
    },

    /// Chunk body was correctly sized but was not followed by the
    /// required `\r\n` separator before the next header.
    #[error("aws-chunked: chunk body missing trailing CRLF at offset {offset}")]
    MissingBodyCrlf { offset: usize },
}

/// Decode an `aws-chunked` body into the raw payload bytes.
///
/// Allocates a single `BytesMut` sized to `bytes.len()` (a strict
/// upper bound on the decoded length, since the wire form only adds
/// headers — never compresses). Returns the freezed `Bytes`.
///
/// See module docs for the wire grammar.
pub fn decode_aws_chunked(bytes: &[u8]) -> Result<Bytes, AwsChunkedError> {
    let mut out = BytesMut::with_capacity(bytes.len());
    let mut cursor = 0usize;

    loop {
        let header_start = cursor;

        // Locate the first `\r\n` that terminates this chunk's header.
        let crlf_rel = match find_crlf(&bytes[cursor..]) {
            Some(i) => i,
            None => {
                return Err(AwsChunkedError::MissingHeaderCrlf {
                    offset: header_start,
                });
            }
        };
        let header = &bytes[cursor..cursor + crlf_rel];
        cursor += crlf_rel + 2; // skip "\r\n"

        // The size is everything up to the first `;` (which starts
        // the chunk extensions, e.g. `;chunk-signature=...`). If
        // there is no `;`, the entire header is the size.
        let size_bytes = match header.iter().position(|&b| b == b';') {
            Some(i) => &header[..i],
            None => header,
        };

        let size_token = std::str::from_utf8(size_bytes).map_err(|_| {
            AwsChunkedError::InvalidHexSize {
                offset: header_start,
                token: format!("{:?}", size_bytes),
            }
        })?;

        if size_token.is_empty() {
            return Err(AwsChunkedError::InvalidHexSize {
                offset: header_start,
                token: size_token.to_owned(),
            });
        }
        // Reject any whitespace / `0x` prefix / Unicode digits.
        if !size_token.bytes().all(is_ascii_hex_digit) {
            return Err(AwsChunkedError::InvalidHexSize {
                offset: header_start,
                token: size_token.to_owned(),
            });
        }
        let size = usize::from_str_radix(size_token, 16).map_err(|_| {
            AwsChunkedError::InvalidHexSize {
                offset: header_start,
                token: size_token.to_owned(),
            }
        })?;

        if size == 0 {
            // Terminating chunk. Per RFC 7230 §4.1.2 / AWS streaming
            // spec, optional trailers may follow, terminated by an
            // empty line. We don't validate them — AWS doesn't sign
            // them in v4 streaming.
            return Ok(out.freeze());
        }

        let body_start = cursor;
        let body_end =
            body_start
                .checked_add(size)
                .ok_or(AwsChunkedError::BodyTooShort {
                    offset: header_start,
                    declared: size,
                    missing: usize::MAX,
                })?;

        if body_end > bytes.len() {
            return Err(AwsChunkedError::BodyTooShort {
                offset: header_start,
                declared: size,
                missing: body_end - bytes.len(),
            });
        }

        out.extend_from_slice(&bytes[body_start..body_end]);
        cursor = body_end;

        // Each chunk body is followed by a literal `\r\n` separator
        // (it is NOT counted in the size). Reject anything else
        // explicitly so a one-off off-by-one in an upstream signer
        // surfaces here, not as silent data corruption downstream.
        if bytes.get(cursor..cursor + 2) != Some(b"\r\n") {
            return Err(AwsChunkedError::MissingBodyCrlf { offset: cursor });
        }
        cursor += 2;
    }
}

/// Return the byte index of the first `\r\n` in `buf`, or `None` if
/// it doesn't appear. We scan manually rather than pulling
/// `memchr::memmem` — chunk headers are tens of bytes, the linear
/// scan is identical to memchr on this size class, and we avoid an
/// extra dep.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    (0..buf.len() - 1).find(|&i| buf[i] == b'\r' && buf[i + 1] == b'\n')
}

fn is_ascii_hex_digit(b: u8) -> bool {
    b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || (b'A'..=b'F').contains(&b)
}

/// Build a single-chunk `aws-chunked` body around `payload`. Test
/// helper kept module-private; integration tests reach for it via
/// `crate::aws_chunked::test_support` (re-exported under
/// `#[cfg(test)]` below).
#[cfg(test)]
pub(crate) fn build_single_chunk(payload: &[u8], signature: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 128);
    let header = format!("{:x};chunk-signature={signature}\r\n", payload.len());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
    let trailer = format!("0;chunk-signature={signature}\r\n\r\n");
    out.extend_from_slice(trailer.as_bytes());
    out
}

#[cfg(test)]
pub(crate) fn build_multi_chunk(chunks: &[&[u8]], signature: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for c in chunks {
        let header = format!("{:x};chunk-signature={signature}\r\n", c.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(c);
        out.extend_from_slice(b"\r\n");
    }
    let trailer = format!("0;chunk-signature={signature}\r\n\r\n");
    out.extend_from_slice(trailer.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIG: &str = "cd7cb30d08ae28c059835a12c33ace18cca48a52cd6e94afa8fa999c6215e866";

    #[test]
    fn single_chunk_round_trips() {
        let payload = b"{\"format-version\":2,\"table-uuid\":\"abc\"}";
        let wire = build_single_chunk(payload, SIG);
        let decoded = decode_aws_chunked(&wire).expect("decode");
        assert_eq!(decoded.as_ref(), payload);
    }

    #[test]
    fn multi_chunk_concatenates() {
        // Non-trivial sizes that exercise the hex-size code: 0x1F (31),
        // 0x100 (256), 0x4 (4). Hex sizes mid-stream forced lowercase
        // by `format!("{:x}", _)`.
        let a = vec![b'A'; 0x1F];
        let b = vec![b'B'; 0x100];
        let c = vec![b'C'; 4];
        let wire = build_multi_chunk(&[&a, &b, &c], SIG);
        let decoded = decode_aws_chunked(&wire).expect("decode");
        let mut expect = Vec::new();
        expect.extend_from_slice(&a);
        expect.extend_from_slice(&b);
        expect.extend_from_slice(&c);
        assert_eq!(decoded.as_ref(), expect.as_slice());
    }

    #[test]
    fn empty_body_decodes_to_empty() {
        // Just the terminating chunk — what the AWS SDK emits for a
        // zero-byte object.
        let wire = format!("0;chunk-signature={SIG}\r\n\r\n");
        let decoded = decode_aws_chunked(wire.as_bytes()).expect("decode");
        assert!(decoded.is_empty());
    }

    #[test]
    fn upper_case_hex_size_accepted() {
        let payload = b"hello world";
        // Hand-build with upper-case hex, since `format!("{:X}", _)`
        // is what some signers emit.
        let header = format!("{:X};chunk-signature={SIG}\r\n", payload.len());
        let mut wire = Vec::new();
        wire.extend_from_slice(header.as_bytes());
        wire.extend_from_slice(payload);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(format!("0;chunk-signature={SIG}\r\n\r\n").as_bytes());
        let decoded = decode_aws_chunked(&wire).expect("decode");
        assert_eq!(decoded.as_ref(), payload);
    }

    #[test]
    fn header_without_extension_accepted() {
        // RFC 7230 chunk grammar: chunk-ext is optional. AWS always
        // includes one in v4 streaming, but a hand-rolled signer
        // might omit it for the terminating chunk; accept that.
        let payload = b"abcd";
        let mut wire = Vec::new();
        wire.extend_from_slice(format!("{:x}\r\n", payload.len()).as_bytes());
        wire.extend_from_slice(payload);
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(b"0\r\n\r\n");
        let decoded = decode_aws_chunked(&wire).expect("decode");
        assert_eq!(decoded.as_ref(), payload);
    }

    #[test]
    fn malformed_hex_size_rejected() {
        let wire = b"zzzz;chunk-signature=abc\r\nbody\r\n0;chunk-signature=abc\r\n\r\n";
        let err = decode_aws_chunked(wire).expect_err("must reject");
        assert!(matches!(err, AwsChunkedError::InvalidHexSize { .. }), "got {err:?}");
    }

    #[test]
    fn empty_size_token_rejected() {
        // Header begins with `;` (no size at all). Some buggy
        // signers have produced this; refuse to interpret as 0.
        let wire = b";chunk-signature=abc\r\n\r\n";
        let err = decode_aws_chunked(wire).expect_err("must reject");
        assert!(matches!(err, AwsChunkedError::InvalidHexSize { .. }), "got {err:?}");
    }

    #[test]
    fn missing_header_crlf_rejected() {
        // No `\r\n` anywhere — decoder must error rather than spin.
        let wire = b"100;chunk-signature=abc";
        let err = decode_aws_chunked(wire).expect_err("must reject");
        assert!(
            matches!(err, AwsChunkedError::MissingHeaderCrlf { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn declared_size_exceeds_body_rejected() {
        // Header says 100 bytes; body has 50.
        let body = vec![b'X'; 50];
        let mut wire = Vec::new();
        wire.extend_from_slice(b"64;chunk-signature=abc\r\n"); // 0x64 = 100
        wire.extend_from_slice(&body);
        wire.extend_from_slice(b"\r\n0;chunk-signature=abc\r\n\r\n");
        let err = decode_aws_chunked(&wire).expect_err("must reject");
        assert!(matches!(err, AwsChunkedError::BodyTooShort { .. }), "got {err:?}");
    }

    #[test]
    fn missing_body_crlf_rejected() {
        // Body is correctly 4 bytes, but no `\r\n` separator follows.
        let mut wire = Vec::new();
        wire.extend_from_slice(b"4;chunk-signature=abc\r\n");
        wire.extend_from_slice(b"abcd"); // 4 bytes, no CRLF
        wire.extend_from_slice(b"0;chunk-signature=abc\r\n\r\n");
        let err = decode_aws_chunked(&wire).expect_err("must reject");
        assert!(matches!(err, AwsChunkedError::MissingBodyCrlf { .. }), "got {err:?}");
    }

    #[test]
    fn truncated_before_terminator_rejected() {
        // Single 4-byte chunk, then EOF (no terminating zero chunk).
        let mut wire = Vec::new();
        wire.extend_from_slice(b"4;chunk-signature=abc\r\nabcd\r\n");
        let err = decode_aws_chunked(&wire).expect_err("must reject");
        // Either MissingHeaderCrlf (loop tries to parse one more
        // header from empty input) — both are valid error shapes
        // for "stream truncated".
        assert!(
            matches!(
                err,
                AwsChunkedError::MissingHeaderCrlf { .. } | AwsChunkedError::Truncated { .. }
            ),
            "got {err:?}"
        );
    }

    /// Regression — exact rendering from rca-stage0bc.md §H4. The
    /// first 64 wire bytes of the corrupt 50 MiB metadata-25455
    /// object started:
    ///
    /// ```text
    /// 20000;chunk-signature=cd7cb30d08ae28c059835a12c33ace18cca48a52cd6e94afa8fa999c6215e866\r\n
    /// {"format-version":2,...
    /// ```
    ///
    /// This test reconstructs that envelope around a plausible JSON
    /// body and asserts the decoder strips it cleanly — i.e. that
    /// the bug we're fixing would not have shipped corrupt bytes
    /// under preview-9.
    #[test]
    fn rca_h4_iceberg_metadata_shape() {
        let json = br#"{"format-version":2,"table-uuid":"16aa64b8-1779-4477-a290-fc1f07f65f8e"}"#;
        // Pad to 0x20000 (128 KiB), the exact first-chunk size the
        // SDK uses (matches `20000;` prefix in the RCA hex dump).
        let chunk1_size = 0x20000usize;
        assert!(json.len() <= chunk1_size);
        let mut chunk1 = json.to_vec();
        chunk1.resize(chunk1_size, b' ');
        let wire = build_multi_chunk(&[&chunk1], SIG);
        // The wire form *must* contain the `20000;` prefix the RCA
        // forensic hex dump captured.
        assert!(wire.starts_with(b"20000;chunk-signature="), "wire prefix mismatch");
        let decoded = decode_aws_chunked(&wire).expect("decode");
        assert_eq!(decoded.len(), chunk1_size);
        assert!(decoded.starts_with(json));
        // And — the canary — the decoded body must NOT contain any
        // chunk-signature= literal.
        let needle = b"chunk-signature=";
        assert!(
            !decoded
                .windows(needle.len())
                .any(|w| w == needle),
            "decoded body must not retain envelope bytes"
        );
    }
}
