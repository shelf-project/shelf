# Shelf chaos drills

Five scripted drills, each runnable in staging with a single command.
All are bash, `set -euo pipefail`, with assertion points marked
`TODO_SHELF-NN` where the real threshold plugs in after the Phase-0
benchmarks land.

| Drill                           | Script                 | Proves                                              |
| ------------------------------- | ---------------------- | --------------------------------------------------- |
| Pod kill                        | `pod-kill.sh`          | HRW re-elects; hit rate ≥ 80% of baseline           |
| Network partition               | `network-partition.sh` | Circuit breaker + fall-through per BLUEPRINT §9.5   |
| NVMe fill                       | `nvme-fill.sh`         | Admission refuses; existing keys still serve        |
| Block corruption                | `block-corruption.sh`  | Key mismatch → re-fetch                             |
| "Leader kill" (DNS membership)  | `leader-kill-dns.sh`   | No-op; ring still routes reads (ADR-0001)           |

## Running

```bash
# Prerequisites:
#   - kubectl context points at the staging cluster
#   - shelf-staging namespace has a 3-pod StatefulSet live
#   - The canonical workload generator is installed (benchmarks/replay)

export SHELF_NAMESPACE=shelf-staging
export TRINO_NAMESPACE=trino-db-staging

./chaos/pod-kill.sh
./chaos/network-partition.sh
./chaos/nvme-fill.sh
./chaos/block-corruption.sh
./chaos/leader-kill-dns.sh
```

Each script emits a PASS / FAIL line on exit and exits non-zero on
assertion failure so CI / weekly drill automation picks it up.

## Scope

These are *skeletons*. They codify the structure of the drill and the
assertion points; the actual thresholds (e.g. "hit rate ≥ 80% of
baseline") are placeholders until Phase-0 benchmarks E3 / E7 give us
numbers. See plan §2 experiments table.
