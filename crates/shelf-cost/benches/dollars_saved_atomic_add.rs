//! Hot-path micro-benchmark for SHELF-40.
//!
//! The acceptance criterion is "wiring overhead must be one atomic
//! add per request — verified by < 5 ns/call". This benchmark
//! exercises three call shapes the s3_shim hot path actually emits:
//!
//! 1. Memory hit, same-AZ — the dominant variant (DRAM serves most
//!    cache hits in steady state, and the requesting Trino worker
//!    is co-located with shelfd most of the time on the dedicated
//!    NodePool).
//! 2. Disk hit, same-AZ — the "DRAM cold but NVMe warm" variant.
//! 3. Peer hit, cross-AZ — the only realistic cross-AZ variant; we
//!    intentionally bench the worst case so the < 5 ns claim
//!    bounds the *whole* matrix, not just the easy case.
//!
//! Run with `cargo bench -p shelf-cost`. CI does not gate on the
//! number — the bench is a "you broke the hot path" leading
//! indicator, not a regression alert.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use shelf_cost::{CostModel, HitEvent, PeerAz};

fn bench_memory_same_az(c: &mut Criterion) {
    let model = CostModel::for_region("ap-south-1").unwrap();
    let event = HitEvent::Memory {
        bytes_returned: 1 << 20,
        peer_az: PeerAz::SameAz,
    };
    c.bench_function("dollars_saved/memory_same_az", |b| {
        b.iter(|| {
            let _ = model.dollars_saved(black_box(event));
        });
    });
}

fn bench_disk_same_az(c: &mut Criterion) {
    let model = CostModel::for_region("ap-south-1").unwrap();
    let event = HitEvent::Disk {
        bytes_returned: 4 << 20,
        peer_az: PeerAz::SameAz,
    };
    c.bench_function("dollars_saved/disk_same_az", |b| {
        b.iter(|| {
            let _ = model.dollars_saved(black_box(event));
        });
    });
}

fn bench_peer_cross_az(c: &mut Criterion) {
    let model = CostModel::for_region("ap-south-1").unwrap();
    let event = HitEvent::Peer {
        bytes_returned: 32 << 20,
        peer_az: PeerAz::CrossAz,
    };
    c.bench_function("dollars_saved/peer_cross_az", |b| {
        b.iter(|| {
            let _ = model.dollars_saved(black_box(event));
        });
    });
}

criterion_group!(
    benches,
    bench_memory_same_az,
    bench_disk_same_az,
    bench_peer_cross_az,
);
criterion_main!(benches);
