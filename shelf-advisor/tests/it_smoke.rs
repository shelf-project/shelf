//! Integration smoke test for the `shelf-advisor` CLI.
//!
//! Drives the compiled binary end-to-end with `assert_cmd`, asserts
//! that `analyze` exits 0 and writes a syntactically-valid empty
//! JSON array. This is the contract the downstream CI/CD consumers
//! depend on — even when no recommendations exist, the output file
//! must be a parseable JSON array (`[]`), not a missing file or a
//! non-JSON string.

use std::fs;

use assert_cmd::Command;
use tempfile::TempDir;

#[test]
fn analyze_writes_empty_json_array() {
    let tmp = TempDir::new().expect("tempdir");
    let out = tmp.path().join("recs.json");

    let mut cmd = Command::cargo_bin("shelf-advisor").expect("binary built");
    cmd.arg("analyze")
        .arg("--output")
        .arg(&out)
        .arg("--window")
        .arg("1d");

    let assert = cmd.assert();
    assert.success();

    let body = fs::read_to_string(&out).expect("output file written");
    let trimmed = body.trim();
    assert_eq!(trimmed, "[]", "expected empty JSON array, got {body:?}");

    let parsed: serde_json::Value = serde_json::from_str(&body).expect("output must be valid JSON");
    assert!(parsed.is_array(), "top-level JSON must be an array");
    assert_eq!(parsed.as_array().unwrap().len(), 0);
}

#[test]
fn version_flag_prints_crate_version() {
    let mut cmd = Command::cargo_bin("shelf-advisor").expect("binary built");
    cmd.arg("--version");
    let assert = cmd.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("utf8");
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "expected crate version in --version output, got {stdout:?}"
    );
}
