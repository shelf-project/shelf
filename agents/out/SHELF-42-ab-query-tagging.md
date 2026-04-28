# SHELF-42: A/B query tagging in event listener

**Status:** Draft
**Tier:** B
**Estimated effort:** S
**Depends on:** SHELF-37
**Blocks:** none

## Problem (OSS-cited)

The original idea was an in-plugin per-query A/B coin-flip toggling Shelf vs direct S3. Verified against Trino 480: `TrinoFileSystem.newInputFile(Location)` does not carry a `QueryId`, so a true *runtime* per-query toggle is **infeasible without an upstream SPI change** ([trinodb/trino #29184](https://github.com/trinodb/trino/issues/29184) — blob-cache SPI — is the umbrella). A genuine runtime A/B requires the catalog to be split (the existing `cdp_shelf` parallel pattern). What the listener *can* do today is record an arm assignment that downstream `GROUP BY shelf_arm` makes the analysis honest.

## Goal

Every `QueryCompletedEvent` row in the SHELF-37 Iceberg log table carries a `shelf_arm ∈ {A, B}` tag, deterministically derived from the `query_id` so reruns are stable.

## Approach

Extend the SHELF-37 listener (no new jar) with a small `ArmAssigner` component: on `queryCreated` (or at projection time on `queryCompleted`), compute `arm = if (sha256(query_id)[0] & 1) == 0 then "A" else "B"`. Configurable via `event-listener.properties`:

```
shelf.arm.enabled=true
shelf.arm.method=hash    # hash | always_a | always_b
shelf.arm.salt=<random>  # rotates per-week if desired
```

Default `enabled=false` for clean OSS deployments; flipping `true` is a one-line config change. `salt` defaults to empty; operators set a per-window salt to randomise cohorts across weeks.

The `shelf_arm` column already exists in the SHELF-37 schema (placeholder column). When `enabled=false`, the column is `null`. Implementation lives in `clients/trino/event-listener-iceberg/src/main/java/io/shelf/eventlistener/ArmAssigner.java`. Documentation under `clients/trino/event-listener-iceberg/docs/AB.md` covers (a) how to interpret the resulting `GROUP BY shelf_arm` analysis, (b) why the tag is *post-hoc analytic*, not runtime routing, (c) the upstream TIP filed against trinodb/trino#29184 requesting `QueryId` propagation.

The companion analysis SQL templates live under `shelfctl/sql/ab/` so SHELF-39 / SHELF-43 / `shelfctl tune` can consume them.

## Acceptance criteria

- [ ] `shelf_arm` is non-null on every emitted row when `shelf.arm.enabled=true`; null when `false`.
- [ ] Across 100 K synthetic `query_id`s, the A:B ratio is within 1 % of 50:50.
- [ ] Two runs of the same `query_id` and salt produce the same arm (determinism).
- [ ] Changing the salt redistributes ≥ 49 % of queries across arms.
- [ ] Performance: arm assignment adds < 10 µs per `queryCompleted` event (benchmarked).
- [ ] Documentation under `clients/trino/event-listener-iceberg/docs/AB.md` explains analytic-vs-runtime scope and links the upstream TIP.

## Out of scope

- Runtime A/B routing (impossible without upstream SPI; covered by the TIP, not this ticket).
- Tagging based on user / catalog / table predicates (v1.x).
- Multi-arm (3+ cohorts).
- Arm assignment via the read path (`s3_shim` / `/cache/*`).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Operators interpret `shelf_arm` as "Shelf was disabled for arm B" — false | Doc + the analysis SQL templates clarify it is a *cohort label* over identical infrastructure. |
| Hash bias on small N | Use SHA-256 first byte (256 buckets); document that small windows (<10 K queries) may show > 1 % imbalance and is expected. |
| Salt rotation breaks longitudinal analysis | Salt history committed to the log table as a separate `shelf_arm_salt` column (or carried as a session property echoed by the listener). |

## Test plan

- Unit tests: deterministic-hash assignment, salt sensitivity, disabled-mode null behaviour, distribution test on 100 K `query_id`s.
- Integration tests: extends the SHELF-37 smoke harness with `shelf.arm.enabled=true` and asserts `SELECT shelf_arm, count(*) FROM <log> GROUP BY shelf_arm` returns 2 rows with ratio ∈ [0.49, 0.51].
- (If applicable) docker compose smoke: green-in-CI assertion under `make ab-smoke`.

## Open questions

- Salt rotation policy: weekly auto-rotate or operator-managed? Recommend operator-managed to keep longitudinal analysis simple.
- Should we also tag in a Trino session property so downstream stages can route on it? Out of scope; that requires a real plugin patch, not a listener.
- Cardinality: leave `{A, B}` two-valued only; do not allow operators to add `C, D, E` in v1.
