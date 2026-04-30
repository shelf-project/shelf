//! Criterion bench scaffold for the HRW router hot path.
//!
//! Ticket ownership:
//! - SHELF-19 — the real bench measures owner() throughput under
//!   N = {3, 5, 10, 20} pods on weighted + unweighted input.
//!
//! Run with:
//!   cargo bench -p shelfd --bench hashring
//!
//! The benchmark below is a stub that records a no-op baseline. It
//! exists so `cargo bench --list` is non-empty and the benches
//! directory is wired into CI.

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_owner_lookup_stub(c: &mut Criterion) {
    c.bench_function("router::owner (SHELF-19 stub)", |b| {
        b.iter(|| {
            // SHELF-19: replace with a real `router.owner(key)` call
            // once the function body lands. Today we time a trivial
            // operation so the harness compiles.
            let _ = std::hint::black_box(0u64.wrapping_add(1));
        })
    });
}

criterion_group!(benches, bench_owner_lookup_stub);
criterion_main!(benches);
