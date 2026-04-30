# Quickstart — Shelf + Trino + MinIO in one `docker compose up`

**Goal**: go from a fresh clone to seeing Shelf cache-hits climb on a
Trino query over Iceberg, on a laptop, in **under 2 minutes** once the
images are pulled. This is the same harness our CI runs on every PR
(`.github/workflows/smoke.yml`).

## Prerequisites

- Docker Desktop 4.28+ (or any Docker Engine ≥ 24 with Compose v2)
- `curl`, `bash` — already on every Linux and macOS
- ~4 GiB free RAM and ~2 GiB free disk for the images
- Apple Silicon is supported (all images are multi-arch)

You do **not** need Rust, Java, Maven, or a Trino install locally. The
harness runs everything in containers.

## Run it

```bash
git clone https://github.com/shelf-project/shelf.git
cd shelf/benchmarks/smoke
./run-smoke.sh
```

First run: ~2 min to pull MinIO + Trino 480 + iceberg-rest + the
locally-built `shelfd` image, then ~20 s for the actual test. Subsequent
runs: ~20 s cold + ~5 s warm.

### What the harness does

1. `docker compose up -d` — starts MinIO, `iceberg-rest`, `shelfd`, Trino
2. seeds 4 Iceberg tables (`nation`, `region`, `orders_small`,
   `customer_small`) into MinIO through the REST catalog
3. runs 10 canonical queries against Trino → `results/cold/NN.txt`
4. scrapes `shelfd:9091/metrics` → `results/metrics-after-cold.txt`
5. runs the same queries again → `results/warm/NN.txt`
6. scrapes metrics again → `results/metrics-after-warm.txt`
7. asserts cold ≡ warm byte-for-byte per query
8. asserts `shelf_hits_total` climbed on at least one pool between
   cold and warm

Green exit = all three properties hold (correctness, hit-climb,
byte-identity). Red exit = something regressed.

### What "working" looks like

```
[smoke] waiting up to 90s for services to report healthy
[smoke] cold run complete
[smoke] warm run complete
[smoke] metadata: cold=84 warm=112  (+28)
[smoke] rowgroup: cold=30 warm=40   (+10)
[smoke] all 10 queries byte-identical between cold and warm
smoke run PASS in 20s
```

If you open `results/cold/04-join-nation-region.txt` you'll see the
actual query output. It's meant to be a small, readable artefact you
can diff against your own changes — not a big benchmark number.

## Poke around

While the harness is up (before `docker compose down`):

```bash
# Trino UI — http://127.0.0.1:8080   (no auth; default "admin")
# MinIO console — http://127.0.0.1:9001   (user: minioadmin / minioadmin)
# shelfd metrics — http://127.0.0.1:9091/metrics
# shelfd S3 shim — http://127.0.0.1:9092
# shelfd admin —  http://127.0.0.1:9090  (health, pin list, stats)
```

You can issue your own query through the Trino UI, or pipe one through
`trino-cli` (an official image exists). Every `GetObject`/`HeadObject`
against `s3://iceberg-warehouse/*` goes through `shelfd` and will show
up in its metrics.

## Tearing it down

```bash
docker compose down -v     # also wipes MinIO data + Foyer cache
```

## Common issues

- **`smoke run FAIL: warm hits did not climb`** — usually means Trino's
  JVM-local Iceberg cache absorbed the warm run. The smoke config pins
  `iceberg.metadata-cache.enabled=false` to prevent that, so if you
  see this after editing the Iceberg catalog properties, double-check
  `benchmarks/smoke/config/trino/etc/catalog/iceberg.properties`.
- **Port conflict on 8080 / 9000 / 9001 / 9090–9092** — the harness
  binds loopback-only by default. Free the ports or edit
  `docker-compose.yml`.
- **Apple Silicon, MinIO x86 emulation warnings** — harmless; the
  harness uses the MinIO multi-arch `RELEASE.2024-*` tag.

## Next steps

- **Add a query of your own**: drop a `.sql` file into
  `benchmarks/smoke/seed/queries/` and re-run.
- **Point Trino somewhere else**: change `s3.endpoint` in
  `config/trino/etc/catalog/iceberg.properties` to `http://shelfd:9092`
  (already set) or direct-to-MinIO to baseline.
- **Try from your own Trino**: follow the same `s3.endpoint` swap
  pattern against any reachable `shelfd:9092`. See
  [`shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md`](../../shelfd/docs/design-notes/SHELF-22-s3-compat-shim.md)
  for the protocol subset Shelf speaks.
- **Production deploy**: see the Helm chart in `charts/shelfd/` — the
  CI `helm-lint` job keeps it release-clean. A full EKS playbook
  lives in [`docs/cluster-handoff.md`](../cluster-handoff.md) (ops
  handoff packet, not a formal user guide yet — tracked under
  SHELF-13).
