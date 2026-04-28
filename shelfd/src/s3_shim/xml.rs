//! Tiny hand-rolled XML codec for the SHELF-21b shim verbs.
//!
//! S3's multipart + bulk-delete + ListObjectsV2 envelopes are stable
//! and small (≤ 10 unique elements each). Pulling in `quick-xml`
//! just to parse `<Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>`
//! would add a dependency the workspace doesn't otherwise need, so we
//! hand-roll a single-pass tag scanner that's good enough for this
//! schema set and rejects anything outside it.
//!
//! Scope:
//!
//! Parsers (request bodies):
//! - `parse_complete_multipart_upload(&str) -> Vec<CompletedPart>`
//! - `parse_delete_objects(&str) -> Vec<String>`
//!
//! Renderers (response bodies):
//! - `render_initiate_multipart_upload(bucket, key, upload_id)`
//! - `render_complete_multipart_upload(bucket, key, etag, location)`
//! - `render_list_bucket_v2(bucket, request, page)`
//! - `render_delete_result(outcomes)`
//!
//! Anything outside that surface — Last-Modified deltas, owner
//! blocks, request-charge — is intentionally not modelled. AWS-SDK
//! clients (boto3, aws-sdk-s3, Trino's S3 filesystem) tolerate
//! missing optional fields silently; emitting empty placeholders
//! would only encourage drift.

use std::fmt::Write;

use crate::origin::{BulkDeleteOutcome, CompletedPart, ListObjectsV2Page};

/// Very small XML escape that matches what S3 itself emits — `&`
/// `<` `>` `"` `'`. Keep in sync with `super::xml_escape` (we don't
/// re-export the parent helper to avoid a circular module surface,
/// but the implementation is byte-identical).
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// Reverse of [`esc`]. Only handles the entity set S3 emits. Bare
/// ampersands without a recognised entity are passed through —
/// matches S3 server-side leniency.
fn unesc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '&' {
            out.push(c);
            continue;
        }
        let mut entity = String::new();
        let mut closed = false;
        while let Some(&p) = chars.peek() {
            chars.next();
            if p == ';' {
                closed = true;
                break;
            }
            entity.push(p);
            if entity.len() > 6 {
                break;
            }
        }
        if !closed {
            out.push('&');
            out.push_str(&entity);
            continue;
        }
        match entity.as_str() {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            other => {
                out.push('&');
                out.push_str(other);
                out.push(';');
            }
        }
    }
    out
}

/// Pull the inner-text of the first `<tag>...</tag>` occurrence
/// inside `slice`. Returns `None` when the tag isn't present.
///
/// **Not a full XML parser.** Crucially: it does not handle
/// attributes (`<tag foo="bar">`), comments, or CDATA. The S3
/// schemas this module models have none of those, so the simpler
/// scanner is correct *for our inputs* and cheaper than dragging
/// in `quick-xml`.
fn first_inner<'a>(slice: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let s = slice.find(&open)? + open.len();
    let e = slice[s..].find(&close)?;
    Some(&slice[s..s + e])
}

/// Iterate over every `<tag>...</tag>` block inside `slice` (in
/// document order). Each yielded slice is the inner text.
fn iter_blocks<'a>(slice: &'a str, tag: &'a str) -> impl Iterator<Item = &'a str> + 'a {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut cursor = 0usize;
    std::iter::from_fn(move || {
        let rest = slice.get(cursor..)?;
        let rel_start = rest.find(&open)? + open.len();
        let abs_start = cursor + rel_start;
        let rel_end = slice.get(abs_start..)?.find(&close)?;
        let abs_end = abs_start + rel_end;
        cursor = abs_end + close.len();
        slice.get(abs_start..abs_end)
    })
}

// ---- Parsers --------------------------------------------------------------

/// Parse the body Trino's S3 client (and any AWS-SDK consumer) sends
/// to `POST /:bucket/*key?uploadId=...`:
///
/// ```xml
/// <CompleteMultipartUpload>
///   <Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>
///   <Part><PartNumber>2</PartNumber><ETag>"def"</ETag></Part>
/// </CompleteMultipartUpload>
/// ```
///
/// Returns the parts in document order; the caller must NOT re-sort.
/// Out-of-order parts are a client bug AWS rejects with 400 —
/// silently sorting would mask it.
pub(super) fn parse_complete_multipart_upload(body: &str) -> Result<Vec<CompletedPart>, String> {
    let mut parts = Vec::new();
    for part_block in iter_blocks(body, "Part") {
        let pn_str = first_inner(part_block, "PartNumber")
            .ok_or_else(|| "missing <PartNumber>".to_owned())?;
        let etag = first_inner(part_block, "ETag").ok_or_else(|| "missing <ETag>".to_owned())?;
        let part_number: i32 = pn_str
            .trim()
            .parse()
            .map_err(|_| format!("invalid <PartNumber>: {pn_str}"))?;
        if part_number < 1 {
            return Err(format!("<PartNumber> must be >= 1, got {part_number}"));
        }
        parts.push(CompletedPart {
            part_number,
            etag: unesc(etag.trim()),
        });
    }
    if parts.is_empty() {
        return Err("CompleteMultipartUpload: no <Part> entries".to_owned());
    }
    Ok(parts)
}

/// Parse the body of `POST /:bucket?delete`:
///
/// ```xml
/// <Delete>
///   <Object><Key>foo/a</Key></Object>
///   <Object><Key>foo/b</Key></Object>
/// </Delete>
/// ```
///
/// `<Quiet>true</Quiet>` is observed but currently ignored — the
/// `render_delete_result` caller decides whether to elide
/// `<Deleted>` rows. We keep the `quiet` decision in the handler
/// rather than threading it through here.
pub(super) fn parse_delete_objects(body: &str) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    for obj_block in iter_blocks(body, "Object") {
        let key = first_inner(obj_block, "Key").ok_or_else(|| "missing <Key>".to_owned())?;
        keys.push(unesc(key.trim()));
    }
    if keys.is_empty() {
        return Err("Delete: no <Object> entries".to_owned());
    }
    Ok(keys)
}

/// Detect the `<Quiet>true</Quiet>` flag without round-tripping the
/// rest of the body. Anything other than the literal `true` (case-
/// insensitive) is treated as the verbose default — matches S3.
pub(super) fn parse_delete_quiet(body: &str) -> bool {
    first_inner(body, "Quiet")
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ---- Renderers ------------------------------------------------------------

const PROLOGUE: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

pub(super) fn render_initiate_multipart_upload(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "{PROLOGUE}<InitiateMultipartUploadResult>\
<Bucket>{}</Bucket>\
<Key>{}</Key>\
<UploadId>{}</UploadId>\
</InitiateMultipartUploadResult>",
        esc(bucket),
        esc(key),
        esc(upload_id),
    )
}

pub(super) fn render_complete_multipart_upload(
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    location: Option<&str>,
) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(PROLOGUE);
    out.push_str("<CompleteMultipartUploadResult>");
    if let Some(loc) = location {
        let _ = write!(out, "<Location>{}</Location>", esc(loc));
    }
    let _ = write!(out, "<Bucket>{}</Bucket>", esc(bucket));
    let _ = write!(out, "<Key>{}</Key>", esc(key));
    if let Some(e) = etag {
        // Always emit ETag in S3's quoted form — clients (Trino's
        // S3 client included) re-quote on parse, so leaving it
        // unquoted breaks round-trip.
        let trimmed = e.trim_matches('"');
        let _ = write!(out, "<ETag>&quot;{}&quot;</ETag>", esc(trimmed));
    }
    out.push_str("</CompleteMultipartUploadResult>");
    out
}

pub(super) struct ListBucketRequestEcho<'a> {
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub continuation_token: Option<&'a str>,
    pub start_after: Option<&'a str>,
    pub max_keys: i32,
}

pub(super) fn render_list_bucket_v2(
    bucket: &str,
    req: &ListBucketRequestEcho<'_>,
    page: &ListObjectsV2Page,
) -> String {
    let mut out = String::with_capacity(512 + page.contents.len() * 256);
    out.push_str(PROLOGUE);
    out.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    let _ = write!(out, "<Name>{}</Name>", esc(bucket));
    if let Some(p) = req.prefix {
        let _ = write!(out, "<Prefix>{}</Prefix>", esc(p));
    } else {
        out.push_str("<Prefix></Prefix>");
    }
    if let Some(d) = req.delimiter {
        let _ = write!(out, "<Delimiter>{}</Delimiter>", esc(d));
    }
    let _ = write!(out, "<MaxKeys>{}</MaxKeys>", req.max_keys);
    let _ = write!(
        out,
        "<KeyCount>{}</KeyCount>",
        page.key_count.max(page.contents.len() as u32)
    );
    let _ = write!(
        out,
        "<IsTruncated>{}</IsTruncated>",
        if page.is_truncated { "true" } else { "false" },
    );
    if let Some(t) = req.continuation_token {
        let _ = write!(out, "<ContinuationToken>{}</ContinuationToken>", esc(t));
    }
    if let Some(t) = page.next_continuation_token.as_deref() {
        let _ = write!(
            out,
            "<NextContinuationToken>{}</NextContinuationToken>",
            esc(t)
        );
    }
    if let Some(s) = req.start_after {
        let _ = write!(out, "<StartAfter>{}</StartAfter>", esc(s));
    }
    for obj in &page.contents {
        out.push_str("<Contents>");
        let _ = write!(out, "<Key>{}</Key>", esc(&obj.key));
        if let Some(lm) = obj.last_modified.as_deref() {
            let _ = write!(out, "<LastModified>{}</LastModified>", esc(lm));
        }
        if let Some(et) = obj.etag.as_deref() {
            // S3's ListObjectsV2 returns ETags **already** quoted —
            // pass through verbatim with safe escaping.
            let _ = write!(out, "<ETag>{}</ETag>", esc(et));
        }
        let _ = write!(out, "<Size>{}</Size>", obj.size);
        out.push_str("<StorageClass>STANDARD</StorageClass>");
        out.push_str("</Contents>");
    }
    for cp in &page.common_prefixes {
        let _ = write!(
            out,
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            esc(cp),
        );
    }
    out.push_str("</ListBucketResult>");
    out
}

pub(super) fn render_delete_result(outcomes: &[BulkDeleteOutcome], quiet: bool) -> String {
    let mut out = String::with_capacity(128 + outcomes.len() * 64);
    out.push_str(PROLOGUE);
    out.push_str("<DeleteResult>");
    for o in outcomes {
        match &o.error {
            None if !quiet => {
                let _ = write!(out, "<Deleted><Key>{}</Key></Deleted>", esc(&o.key));
            }
            None => {} // Quiet mode hides successful rows entirely.
            Some((code, message)) => {
                let _ = write!(
                    out,
                    "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
                    esc(&o.key),
                    esc(code),
                    esc(message),
                );
            }
        }
    }
    out.push_str("</DeleteResult>");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_complete_multipart_upload_round_trip() {
        let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>&quot;a1&quot;</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>&quot;b2&quot;</ETag></Part>
</CompleteMultipartUpload>"#;
        let parts = parse_complete_multipart_upload(body).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag, "\"a1\"");
        assert_eq!(parts[1].part_number, 2);
        assert_eq!(parts[1].etag, "\"b2\"");
    }

    #[test]
    fn parse_complete_multipart_upload_rejects_empty_part_list() {
        let err =
            parse_complete_multipart_upload("<CompleteMultipartUpload></CompleteMultipartUpload>")
                .expect_err("empty body must be rejected");
        assert!(err.contains("no <Part>"));
    }

    #[test]
    fn parse_complete_multipart_upload_rejects_zero_part_number() {
        let body = "<CompleteMultipartUpload><Part><PartNumber>0</PartNumber><ETag>x</ETag></Part></CompleteMultipartUpload>";
        let err = parse_complete_multipart_upload(body).expect_err("part 0 invalid");
        assert!(err.contains(">= 1"));
    }

    #[test]
    fn parse_complete_multipart_upload_preserves_caller_order() {
        // Anti-regression: a re-sort would mask client bugs and
        // produce a "valid" complete with the wrong ETag composition.
        let body = "<CompleteMultipartUpload>\
<Part><PartNumber>3</PartNumber><ETag>c</ETag></Part>\
<Part><PartNumber>1</PartNumber><ETag>a</ETag></Part>\
</CompleteMultipartUpload>";
        let parts = parse_complete_multipart_upload(body).unwrap();
        assert_eq!(parts[0].part_number, 3);
        assert_eq!(parts[1].part_number, 1);
    }

    #[test]
    fn parse_delete_objects_round_trip() {
        let body = "<Delete>\
<Object><Key>a/b</Key></Object>\
<Object><Key>c%2Fd</Key></Object>\
</Delete>";
        let keys = parse_delete_objects(body).unwrap();
        assert_eq!(keys, vec!["a/b", "c%2Fd"]);
    }

    #[test]
    fn parse_delete_objects_rejects_empty() {
        let err = parse_delete_objects("<Delete></Delete>").expect_err("empty body invalid");
        assert!(err.contains("no <Object>"));
    }

    #[test]
    fn parse_delete_quiet_handles_both_modes() {
        assert!(parse_delete_quiet("<Delete><Quiet>true</Quiet></Delete>"));
        assert!(parse_delete_quiet("<Delete><Quiet>TRUE</Quiet></Delete>"));
        assert!(!parse_delete_quiet("<Delete><Quiet>false</Quiet></Delete>"));
        assert!(!parse_delete_quiet("<Delete></Delete>"));
    }

    #[test]
    fn render_initiate_multipart_upload_emits_all_three_fields() {
        let xml = render_initiate_multipart_upload("buck", "key/with space", "uid-xyz");
        assert!(xml.starts_with("<?xml"));
        assert!(xml.contains("<Bucket>buck</Bucket>"));
        assert!(xml.contains("<Key>key/with space</Key>"));
        assert!(xml.contains("<UploadId>uid-xyz</UploadId>"));
    }

    #[test]
    fn render_complete_multipart_upload_quotes_etag() {
        let xml = render_complete_multipart_upload("b", "k", Some("abc-1"), None);
        assert!(xml.contains("<ETag>&quot;abc-1&quot;</ETag>"));
    }

    #[test]
    fn render_complete_strips_existing_quotes_then_re_quotes() {
        let xml = render_complete_multipart_upload("b", "k", Some("\"abc-1\""), None);
        assert!(xml.contains("<ETag>&quot;abc-1&quot;</ETag>"));
        assert!(!xml.contains("&quot;&quot;abc-1&quot;&quot;"));
    }

    #[test]
    fn render_list_bucket_v2_emits_truncation_and_token() {
        let page = ListObjectsV2Page {
            contents: vec![crate::origin::ListedObject {
                key: "a.parquet".into(),
                size: 1234,
                etag: Some("\"deadbeef\"".into()),
                last_modified: Some("2026-04-27T10:00:00Z".into()),
            }],
            common_prefixes: vec!["data/".into()],
            is_truncated: true,
            next_continuation_token: Some("opaque-token".into()),
            key_count: 1,
        };
        let req = ListBucketRequestEcho {
            prefix: Some("data/"),
            delimiter: Some("/"),
            continuation_token: None,
            start_after: None,
            max_keys: 1000,
        };
        let xml = render_list_bucket_v2("buck", &req, &page);
        assert!(xml.contains("<Name>buck</Name>"));
        assert!(xml.contains("<Prefix>data/</Prefix>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextContinuationToken>opaque-token</NextContinuationToken>"));
        assert!(xml.contains("<Contents>"));
        assert!(xml.contains("<Key>a.parquet</Key>"));
        assert!(xml.contains("<Size>1234</Size>"));
        assert!(xml.contains("<CommonPrefixes><Prefix>data/</Prefix></CommonPrefixes>"));
    }

    #[test]
    fn render_delete_result_verbose_mode_lists_successes() {
        let outcomes = vec![
            BulkDeleteOutcome {
                key: "a".into(),
                error: None,
            },
            BulkDeleteOutcome {
                key: "b".into(),
                error: Some(("AccessDenied".into(), "no perm".into())),
            },
        ];
        let xml = render_delete_result(&outcomes, false);
        assert!(xml.contains("<Deleted><Key>a</Key></Deleted>"));
        assert!(xml.contains(
            "<Error><Key>b</Key><Code>AccessDenied</Code><Message>no perm</Message></Error>"
        ));
    }

    #[test]
    fn render_delete_result_quiet_mode_hides_successes_keeps_errors() {
        let outcomes = vec![
            BulkDeleteOutcome {
                key: "a".into(),
                error: None,
            },
            BulkDeleteOutcome {
                key: "b".into(),
                error: Some(("AccessDenied".into(), "no perm".into())),
            },
        ];
        let xml = render_delete_result(&outcomes, true);
        assert!(!xml.contains("<Deleted>"));
        assert!(xml.contains("<Error><Key>b</Key>"));
    }

    #[test]
    fn xml_escape_round_trip_through_unesc() {
        for s in ["no specials", "amp & ersand", "<tag>", "quote\"and'apos"] {
            let escaped = esc(s);
            let back = unesc(&escaped);
            assert_eq!(back, s, "round-trip failed for {s:?}: escaped={escaped:?}");
        }
    }
}
