# Contributing a new engine example

This directory ships seven reference stacks (Spark, DuckDB, Polars,
Daft, ClickHouse, StarRocks, PyIceberg) that each prove the same
thing on a different engine: pointing an OSS query engine's S3
client at `shelfd:9092` produces a measurable cold→warm speedup with
zero engine-side patches. New engines welcome — same shape, same
contract.

## What to put in `examples/<engine>/`

Every example must contain at least these five artifacts. The CI
matrix in [`.github/workflows/multi-engine.yml`](../.github/workflows/multi-engine.yml)
keys off them by name.

| Path | Required content |
| --- | --- |
| `docker-compose.yml` | MinIO + an Iceberg catalog (`tabulario/iceberg-rest` is the default; `apache/iceberg-rest-fixture` if you need a SQLite-backed REST) + `shelfd` (built from this repo's `shelfd/Dockerfile`) + the engine + a one-shot `seed` service. Host ports must not collide with the other examples (each engine grabs a unique 4-digit range — pick the next free one). |
| `README.md` | A 5-minute walkthrough: what it shows, prerequisites, `bash run.sh` instructions, expected sample output, the engine's S3-client property names, the engine's specific gotcha (every engine has had one — see `examples/README.md` for the running list), and a `docker compose down -v` cleanup line. |
| `run.sh` | One-command end-to-end driver. **Must** print a single `cold=… warm=… speedup=…` line on stdout and exit 0 only when the warm pass produced more `shelf_hits_total` than the cold pass. Must work in a fresh checkout with no manual setup. CI invokes it as `bash examples/<engine>/run.sh` with a 15-minute timeout. |
| Seed script (`init/seed.py`, `init/seed.sh`, etc.) | Writes a small Iceberg table (10k–1M rows is the band; smaller is fine if cold/warm is still measurable). The seed **must** point directly at MinIO (`s3.endpoint=http://minio:9000`), not at shelfd — we want to measure the *read* path through Shelf, not double-count writes. |
| Bench script (`bench.py`, `bench.sql`, etc.) | Runs the same query twice, scrapes shelfd `/metrics` between runs (`shelf_hits_total`, `shelf_misses_total`, `shelf_origin_bytes_total`), and prints the deltas. Must be deterministic on the seeded data. |

Optional but encouraged:

- `VALIDATION_NOTES.md` — captured output from one or two real
  end-to-end runs on a real machine. The other examples cite the
  date and the host (typically `m1` MacBook, Docker Desktop) so
  reviewers can reproduce. CI tells you it doesn't crash; this
  tells the next reader what to expect to see.
- `Dockerfile.runner` — if the engine ships a Python or other
  client that needs extras pinned (`daft`, `polars`, `pyiceberg`).
- `config/<engine>/...` — engine-side config snippets that disable
  any engine-local cache that would mask shelfd on the warm pass
  (see the *Caveats by engine* section of `examples/README.md`).

## The contract `run.sh` must honour

CI is the strictest reader of `run.sh`. Specifically:

1. **Exit non-zero on any failure.** Use `set -euo pipefail` at the
   top. The CI step pipes through `tee run.log`; the
   `set -o pipefail` setting on the workflow side preserves the
   inner exit code.
2. **Print a `cold→warm` summary on stdout.** Both human-readable
   ("`cold: 1.24s | warm: 143ms | speedup: 8.68x`") and the raw
   numbers are fine; the existing examples vary. The single
   non-negotiable is that the summary appears *unconditionally*,
   even when the speedup is below 1.0×, so reviewers can see the
   regression.
3. **Scrape `/metrics`.** Hit `http://127.0.0.1:<host-port>/metrics`
   between the cold and warm runs and report
   `shelf_hits_total` and `shelf_misses_total` deltas. The
   warm pass should produce hits ≫ 0; if it doesn't, something
   engine-side is masking shelfd (a metadata cache, an in-process
   range cache) and the example needs to disable it explicitly.
4. **Tear down on every exit path.** Wire a `trap … EXIT` (see
   `examples/duckdb/run.sh` and `examples/pyiceberg/run.sh` for
   the canonical shape) so an interrupted run does not leak
   containers and volumes onto the runner. The CI workflow also
   runs `docker compose -f examples/<engine>/docker-compose.yml
   down -v --timeout 30 --remove-orphans` in an `if: always()`
   step as belt-and-suspenders, but `run.sh` should not depend on
   that for local correctness.
5. **Build shelfd from source by default.** Use this repo's
   `shelfd/Dockerfile` (or a thin Dockerfile that wraps it) so the
   example is runnable from any checkout, with no GHCR auth and
   no flipped-public package required. Honour
   `SHELFD_IMAGE=ghcr.io/<owner>/shelfd:<tag>` as an opt-in
   override for users who already have a published image they
   want to pin to.

## Style and host-port discipline

- Host ports go in a unique 4-digit range per example so two
  stacks can run side by side on a developer laptop. Grep the
  existing `docker-compose.yml` files for `:9` and pick a band
  no other example uses.
- Service names start with `shelf-<engine>-` so `docker ps`
  output remains legible when several stacks are up at once.
- Iceberg table name is `demo.events` (or `default.events` for
  catalogs that do not allow a `demo` namespace by default — see
  the StarRocks example). Keep it consistent so the bench
  scripts can be skimmed in parallel.

## Reviewing your example

Run the same gate CI runs:

```bash
cd shelf
bash examples/<engine>/run.sh                              # full e2e
docker compose -f examples/<engine>/docker-compose.yml \
    down -v --timeout 30 --remove-orphans                  # clean up
```

Then add the engine to the matrix in
`.github/workflows/multi-engine.yml`, the table in
`examples/README.md` (cold/warm timing, engine-specific knob), and
— if you found a sharp edge that hides whether shelfd is on the
read path — the *Caveats by engine* section of the same README.
