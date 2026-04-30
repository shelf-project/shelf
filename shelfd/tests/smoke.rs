//! Integration test scaffold.
//!
//! Ticket ownership:
//! - SHELF-12 — docker-compose-driven end-to-end smoke test. The real
//!   version spins up Trino 480 + MinIO + shelfd and runs the 10
//!   canonical queries.
//!
//! Each test here is `#[ignore]` so `cargo test --all` stays fast on
//! developer laptops. CI jobs unignore them by running
//! `cargo test -- --ignored`.

use shelfd::{config, store};

#[test]
#[ignore = "SHELF-12: pending docker-compose harness"]
fn smoke_read_through_against_minio() {
    // Target shape after SHELF-06 + SHELF-12:
    //   1. start MinIO + shelfd via testcontainers
    //   2. upload a 1 MiB Parquet file
    //   3. GET /cache/<key>/0-1048576 → 200 with bytes
    //   4. GET again → same bytes from DRAM hit (assert metric delta)
    panic!("SHELF-12: smoke test not implemented yet; see 03-plan.md §4 SHELF-12");
}

#[test]
fn config_types_link() {
    // This test exists only to prove the public surface links.
    // Remove once SHELF-02 lands a meaningful Config::from_path test.
    let _pool_kind = store::Pool::Metadata;
    let _ = std::mem::size_of::<config::Config>();
}
