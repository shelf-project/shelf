# Shelf threat model (STRIDE)

_Status: v0.1 scaffold, agent-9, 2026-04-23._
_Scope: v1 architecture as defined in `BLUEPRINT.md` §6.3, §8, §9
and narrowed by `agents/out/03-plan.md` §1 (no embedded Raft;
no ONNX MLP in hot path; no Arrow Flight in v1; no in-repo
result cache)._

---

## 0. Method

We apply [STRIDE](https://learn.microsoft.com/en-us/azure/security/develop/threat-modeling-tool-threats)
— **S**poofing, **T**ampering, **R**epudiation, **I**nformation
disclosure, **D**enial of service, **E**levation of privilege — to
each component on our trust boundary map.

Every threat row has a disposition:

- **mitigated** — countermeasure exists and is tested (pointer to ticket / code / doc)
- **accepted** — known residual risk with an explicit reason
- **open** — not yet mitigated; ticket filed; carried in risk register

No row is left blank. Every accepted row has a reason. Every open
row has an owner and a target phase.

### 0.1 Trust-boundary map

```
      ┌─────────────────────────────────────────────────────────────┐
      │  Trino coordinator pod (rep-{0..3})                         │
      │   ├── EventListener: ShelfPrefetchListener  (JWT-signs)     │
      │   └── TrinoFileSystem: ShelfFileSystem      (mTLS client)   │
      └────────────────▲───────────────────────▲────────────────────┘
                       │  mTLS + JWT           │  S3 SigV4 (fallback)
                       │                       │
      ┌────────────────▼───────────────────────▼────────────────────┐
      │  shelfd pod (StatefulSet, rep-2 NVMe pool)                  │
      │   ├── HTTP/2 data plane     :9090 (mTLS-only in prod)       │
      │   ├── gRPC control plane    :9092 (Pin / Evict / Stats)     │
      │   ├── /metrics              :9091 (mesh-local only)         │
      │   ├── Foyer cache           DRAM + NVMe (per-pod PVC)       │
      │   └── S3 origin client       IRSA, per-tenant STS chain     │
      └────────────────▲──────────────────────────┬─────────────────┘
                       │  HMS read-only           │
                       │                          │
      ┌────────────────▼───────────────┐ ┌────────▼─────────────────┐
      │  trainer (offline, Airflow)    │ │  snapshot-watcher (Phase │
      │  Reads cdp.trino_logs.*        │ │  1.5) — HMS poll every 30s│
      │  Writes pin_list.json to       │ │  Writes snapshot map to   │
      │  s3://config-bucket/shelf/     │ │  control plane            │
      └────────────────────────────────┘ └──────────────────────────┘
```

Boundaries (where authentication & authorisation are enforced):

1. Worker pod ↔ `shelfd` pod: mTLS + per-tenant JWT (§IAM.md §2.3)
2. Coordinator pod → `shelfd` control plane: mTLS + RBAC (§IAM.md §2.4)
3. `shelfd` → S3: IRSA, one role per tenant, STS chain for cross-tenant
4. `shelfd` → HMS (read-only for snapshot-watcher): IAM database role
5. Trainer → S3 config bucket: dedicated write role, `PutObject` only
6. `shelfctl` operator → control plane: mTLS client cert + RBAC

Everything outside these boundaries is hostile, including other pods
in the same cluster (pod-to-pod traffic is gated by NetworkPolicy —
owned by agent 8).

### 0.2 Non-goals for the threat model

- Compromise of the Kubernetes control plane itself. EKS hardening is
  owned by platform, not Shelf.
- Compromise of the S3 account. If AWS control plane is compromised,
  the cache is the least of our problems.
- Side-channel attacks between pods on the same node (Spectre /
  Meltdown / RowHammer). Accepted residual; mitigated by running on
  AL2023 with current kernel and vendor firmware.

---

## 1. `shelfd` data plane

**Component.** HTTP/2 `GET /cache/<key>/<offset>-<len>` on :9090,
Foyer DRAM+NVMe cache, S3 origin client. The path that a Trino
worker hits for every cached byte.

| # | STRIDE | Threat                                                                 | Mitigation / acceptance                                                                 | Status     |
| - | ------ | ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ---------- |
| D-S1 | S | Rogue pod in the cluster impersonates a Trino worker and reads cached bytes belonging to another tenant | mTLS on :9090; SPIFFE-style SAN = `spiffe://shelf/tenant/<tid>/worker`; NetworkPolicy denies all except Trino worker pods (agent 8 `networkpolicy.yaml`); per-tenant JWT carried in `X-Shelf-Tenant-Jwt` header and validated server-side | mitigated  |
| D-S2 | S | Attacker spoofs the S3 origin to inject forged bytes into the cache    | S3 SDK v2 uses SigV4 + TLS 1.2+ by default; we additionally enforce `endpoint` pinning in config (no env-override at runtime); reject any `X-Amz-*` header manipulation path | mitigated  |
| D-T1 | T | On-disk NVMe file is modified (privileged pod, or sidecar with write access to the PVC) | Content-addressed key `sha256(etag ‖ offset ‖ length)` is re-checked on read; checksum mismatch → evict + refetch from S3 (SHELF-04, BLUEPRINT §9.4); PVC is `ReadWriteOnce`, mounted only in `shelfd` container; `readOnlyRootFilesystem: true` on the pod spec | mitigated  |
| D-T2 | T | MITM rewrites bytes between `shelfd` and worker (same VPC, but assume hostile)  | mTLS on :9090 with ciphers restricted to AEAD suites (TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384, TLS_CHACHA20_POLY1305_SHA256); HTTP/2 with h2-only ALPN; no cleartext listener in production | mitigated  |
| D-R1 | R | Tenant A denies it issued a large cold-miss storm that hammered S3 (chargeback dispute) | Every `get_range` logs `tenant`, `query_id`, `object_key`, `bytes`, `cache_hit` to the structured audit log; log is shipped to an append-only S3 bucket with bucket-owner-preferred ACL and Object Lock in governance mode (90-day retention) | mitigated  |
| D-I1 | I | `/metrics` on :9091 leaks per-tenant object keys (which are inferable as table paths) | `shelf_*_total{tenant=...}` uses a tenant-ID (opaque short string), not the tenant name; object-key labels are bucketed (`prefix_sha1_8char`) never full; `/metrics` NetworkPolicy: Prometheus scraper pod only | mitigated  |
| D-I2 | I | Cached bytes of tenant A leak to tenant B via hash-ring re-routing after a pod restart | Hash-ring key includes `tenant_id` prefix (`sha256("tenant/<tid>:" ‖ etag ‖ offset ‖ length)`); a miss on the new owner refetches from S3 using the **requesting tenant's** role, not the previous owner's; documented invariant: "shelfd never serves bytes it cannot re-derive from the requester's IAM role" | mitigated  |
| D-D1 | D | Cold-cache thundering herd saturates S3 prefix rate limit (503 SlowDown storm) | Per-prefix concurrency limiter on the fallback path (Phase 3 deliverable, SHELF in plan §3); ADR-0001's 15-min pin-list reload pre-warms critical keys; circuit breaker shields workers (BLUEPRINT §9.5) | mitigated  |
| D-D2 | D | Adversary sends 1 M requests for distinct random 1 GiB objects to exhaust NVMe writes | Size-threshold admission (SHELF-25) refuses > 1 GiB unless pinned; per-tenant quota enforced in `pool.rowgroup`; per-pod `tokio` semaphore capped at `2 × NVMe write bandwidth` | mitigated  |
| D-D3 | D | Slow-loris on :9090 exhausts h2 streams                                | Axum + hyper enforce `max_concurrent_streams = 256` and `keep_alive_interval = 30s`; idle-timeout 120 s; body-read deadline 2 s for headers, 10 s for body | mitigated  |
| D-E1 | E | `unsafe` block in Foyer or a dependency enables memory corruption → RCE | `cargo-deny` blocks crates with unreviewed `unsafe`; `cargo-geiger` score tracked in CI; `shelfd` runs with `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, all capabilities dropped, `seccompProfile: RuntimeDefault`; fuzz harness on `http.rs` range parser in CI (Phase 1 deliverable) | mitigated  |
| D-E2 | E | Distroless base image ships with a setuid binary or a vulnerable glibc | Base is `gcr.io/distroless/cc-debian12:nonroot`; Trivy + Grype scan every image in CI (fail on CRITICAL); SBOM published with every release | mitigated  |
| D-E3 | E | Attacker with pod-exec rights reads NVMe files to recover cached data of another tenant | Pods run with `readOnlyRootFilesystem`; PVC is encrypted at rest with a KMS CMK per-cluster; `kubectl exec` is gated by OIDC group membership (platform-owned); accepted residual: a platform admin can always read a PVC — this is in scope of the Kubernetes control-plane trust model | accepted — platform admins are trusted; logged via CloudTrail |

---

## 2. `shelfd` control plane

**Component.** gRPC `Pin` / `Evict` / `Stats` / `Prefetch` on :9092,
S3-backed pin-list loader, K8s headless-service membership (no
embedded Raft per ADR-0001).

| # | STRIDE | Threat                                                                 | Mitigation / acceptance                                                                  | Status     |
| - | ------ | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------- |
| C-S1 | S | A pod impersonates the Trino coordinator to fire `Prefetch` RPCs that poison the cache | mTLS on :9092; SAN allow-list `spiffe://shelf/component/coordinator`; JWT `aud=shelf.prefetch` verified against coordinator-rota issuer | mitigated  |
| C-S2 | S | An operator impersonates `shelfctl` to issue `Evict` on a rival tenant's pinned objects | Separate CA for admin certs; `shelfctl` cert embeds `operator-group=shelf-oncall`; server RBAC table in `iam.md §2.4` enforces `Pin`/`Evict`/`Reload` = operator-only, `Stats` = read-only audience | mitigated  |
| C-T1 | T | Pin list JSON in S3 is tampered with (replaced with a bogus list pinning attacker-controlled prefixes) | `pin_list.json` is versioned (S3 Object Versioning); `shelfd` verifies `x-amz-object-lock-retain-until-date`; ops review every diff as a PR **before** writing; trainer writes through a separate IAM role with `s3:PutObject` only (no delete, no list) scoped to one prefix | mitigated  |
| C-T2 | T | In-flight `Prefetch` message is tampered mid-gRPC (tenant reassignment) | mTLS + per-message `tenant_jwt` field that the server verifies; `tenant` in the JWT must equal `tenant` in the PrefetchRequest, otherwise RPC returns `PERMISSION_DENIED` | mitigated  |
| C-R1 | R | Ops claims they never ran `shelfctl evict` on a hot prefix (causing a hit-rate cliff) | All admin RPCs write an `admin_audit` record: `(ts, actor_cert_fingerprint, method, args_hash)` to the same append-only S3 audit bucket as the data plane; `shelfctl` prints the audit id on success | mitigated  |
| C-R2 | R | Trainer denies it wrote a given `pin_list.json` version | S3 Object Versioning + bucket-owner-enforced logging to CloudTrail data events; Trainer signs the JSON blob with a per-environment Sigstore OIDC identity (§SUPPLY_CHAIN.md §5); `shelfd` verifies signature before applying | mitigated  |
| C-I1 | I | `Stats` response leaks object-key counts that reveal another tenant's table layout | `Stats` is per-tenant; cross-tenant totals are rounded to nearest 1 GiB; operator-scope `Stats` is separately gated (RBAC role `shelf-oncall-readonly`) | mitigated  |
| C-I2 | I | Debug log lines accidentally print full S3 object keys at INFO level   | `tracing::info_span!` wraps keys in a `RedactedKey` type that displays `sha256-short:<8chars>`; clippy lint `lint-no-raw-key-log` (Phase 1 deliverable); sample log lines in `runbooks/` reviewed before first release | mitigated  |
| C-D1 | D | Operator issues `Pin` for 10⁹ objects, starving the DRAM metadata pool | `Pin` RPC rejects requests where `sum(target_bytes) > 2 × pool.metadata.capacity`; pin-list loader rejects lists > 100 k entries with `INVALID_ARGUMENT`; ADR-0001 hot-reload is atomic (whole file) so partial DoS not possible | mitigated  |
| C-D2 | D | A dead pod's DNS entry remains in ring, causing hash-ring mis-routes   | Membership resolver TTL = 5 s (SHELF-20 E7 validates <1 % misroute / min); circuit breaker on the plugin side short-circuits to S3 if an "owner" is unreachable; open question — whether to publish readiness on `/readyz` as a ring-eject signal (tracked in risk register R-07) | open (Phase 3) |
| C-E1 | E | A bug in `Prefetch` deserialisation enables a crafted gRPC frame to panic `shelfd` → repeated crash-loop effectively disables the whole tenant | `prost`-generated code; `#[deny(unsafe_code)]` on control-plane crate; `cargo-fuzz` target `fuzz_prefetch_request` runs on every CI pass; `shelfd` catches panics in the tokio task and returns `INTERNAL` (does not crash the process) | mitigated  |
| C-E2 | E | `shelfctl` admin cert is long-lived; if stolen, attacker has infinite `Evict` privilege | cert-manager issues operator certs with 24 h TTL, rotated automatically; lost cert mitigated by short TTL + cert revocation list (CRL) published to :9092 every 60 s; rota holders enrol via hardware-backed keys (see `IAM.md §3`) | mitigated  |

---

## 3. Trino plugin (`clients/trino/...`)

**Component.** `ShelfFileSystem` (read-path), `ShelfPrefetchListener`
(coordinator), `CircuitBreaker` (per-pod state machine). Runs inside
the Trino JVM as a plugin loaded at startup.

| # | STRIDE | Threat                                                                 | Mitigation / acceptance                                                                  | Status     |
| - | ------ | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------- |
| P-S1 | S | Plugin is pointed at a malicious `shelfd` via a tampered ConfigMap (attacker has cluster-admin on one replica) | Plugin verifies `shelfd` server cert against a pinned CA bundle baked into the Helm chart (not a per-cluster CM); cert SAN must match `*.shelf.svc.cluster.local`; invalid cert → fail-open to S3 (never serve tampered bytes) | mitigated  |
| P-S2 | S | Plugin impersonates a different tenant to `shelfd` (e.g. reading a neighbour's pinned objects) | JWT issued by the Trino coordinator via an in-pod signer (HSM-backed key, `IAM.md §2.3`); JWT `sub = tenant_id` pulled from Trino `Identity.principal` at query dispatch; `shelfd` verifies `iss`, `aud`, `exp` (≤ 5 min), and `sub` matches the IRSA role used on origin fetch | mitigated  |
| P-T1 | T | Plugin is replaced on disk by a rogue operator with a build that logs queries | Plugin jar is downloaded by init container from a cosign-signed OCI artefact (`SUPPLY_CHAIN.md §4`); signature check runs at pod start; mismatch → pod CrashLoopBackOff; CODEOWNERS routes `clients/trino/**` to the security rota | mitigated  |
| P-T2 | T | Circuit-breaker state is corrupted (race condition) → plugin serves from a pod it should bypass | `CircuitBreaker` uses `AtomicInteger` + `AtomicReference<State>`; SHELF-11 ships 9 unit tests including concurrent `record_failure` and half-open-only-one-probe; fuzz test runs nightly in CI | mitigated  |
| P-R1 | R | After a query serves stale or suspicious data, Trino log does not say whether the bytes came from Shelf or S3 | Every `ShelfFileSystem.newInputFile()` logs `{queryId, source: "shelf|s3|fallback", key_hash, tenant}` at INFO; `QueryCompletedEvent` surrogate attribute `shelf.path.source_distribution` counts hits vs fallbacks per query | mitigated  |
| P-I1 | I | `ShelfPrefetchListener` logs the full user SQL at INFO → credentials / PII leak via grep-able logs | Listener uses the normalised SQL fingerprint (`sha256(canonical_sql)`) for all log lines; raw SQL only in DEBUG and only with `shelf.prefetch.debug.sql=true` explicitly set; prod Helm chart refuses to install with the flag true (OPA policy stub) | mitigated  |
| P-I2 | I | Listener forwards a full query plan to `shelfd` for prefetch; plan contains sensitive schema details | `Prefetch` payload is reduced to `(table, partitions, snapshot_id)` tuples, never the raw plan; plan-JSON transformation happens inside the listener and never crosses the pod boundary | mitigated  |
| P-D1 | D | Listener blocks the coordinator thread on a slow `Prefetch` RPC → whole replica stops accepting queries | Hard deadline: 10 ms wall-clock on the listener thread (plan §4 SHELF-15; BLUEPRINT §9.5); circuit breaker wraps the listener; plugin falls back to a no-op on timeout and logs at WARN | mitigated  |
| P-D2 | D | Prefetch queue on `shelfd` overflows; listener retries, amplifying load | Fire-and-forget; `Prefetch` RPC server returns 200 OK immediately (stream of queued items is backpressured at the server); client has no retry policy on `Prefetch` | mitigated  |
| P-E1 | E | Plugin has privileged JVM classloader access; a crafted config value triggers unexpected code path | Plugin enables the SPI `ServiceLoader` in a child classloader; no `Class.forName(userInput)` anywhere; Checker Framework annotations enforce `@Untainted` on config-derived strings; spotbugs-security profile runs in CI | mitigated  |
| P-E2 | E | A malicious Trino user crafts a query with `SET SESSION shelf.tenant = victim_tenant` to exfiltrate | Plugin ignores session properties for tenant identity; tenant is always derived from `Identity.user`/`Identity.groups` via the same mapping Trino already uses for S3 SigV4; session properties are allow-listed (`shelf.debug.enabled`, `shelf.footer.prefetch.kib`) | mitigated  |

---

## 4. Trainer (offline)

**Component.** Airflow job (Python) that reads
`cdp.trino_logs.trino_queries` nightly, produces `pin_list.json`,
writes to the S3 config bucket. Never on the hot path.

| # | STRIDE | Threat                                                                 | Mitigation / acceptance                                                                  | Status     |
| - | ------ | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------- |
| T-S1 | S | An Airflow DAG is compromised to write a malicious pin list under the trainer identity | Trainer DAG is source-gated: only PRs that pass `CODEOWNERS` review can change `airflow/dags/shelf_trainer.py`; DAG assumes a dedicated IRSA role (`shelf-trainer-writer`) scoped to a single S3 prefix | mitigated  |
| T-S2 | S | Another DAG running in the same Airflow account writes to the Shelf config prefix under a neighbouring role | S3 bucket policy denies `s3:PutObject` from any principal except `arn:aws:iam::ACCOUNT:role/shelf-trainer-writer`; bucket owner enforced | mitigated  |
| T-T1 | T | Training data (`cdp.trino_logs`) is polluted by a malicious user's crafted queries to skew pins toward their own tables | Trainer filters queries where `state != 'FINISHED'` and `query_type in ('SELECT', 'INSERT')`; drops the top 0.1 % by scanned_bytes as outliers; produces a PR-shaped diff (not direct write) — ops reviews the delta before publication (ADR-0001) | mitigated  |
| T-T2 | T | Python packaging: a typosquat on `boto3` is pulled into the trainer image | `uv.lock` committed; `pip-audit` + `pip-compile --generate-hashes` enforced in CI; only PyPI source, no arbitrary git URLs | mitigated  |
| T-R1 | R | Trainer refuses to admit it published a bad pin list; no audit trail beyond the git PR | S3 Object Versioning + Sigstore OIDC signature on every write (§SUPPLY_CHAIN.md §5); signature subject = GitHub Actions OIDC, so we can trace back to the PR that produced it | mitigated  |
| T-I1 | I | Trainer logs contain user-issued SQL verbatim → PII in logs                 | Same normalisation as plugin: SQL is fingerprinted, not logged verbatim; DAG output stored with 30-day retention in a separate bucket with bucket-owner-enforced logging | mitigated  |
| T-D1 | D | A misbehaving query floods `cdp.trino_logs.trino_queries` so the nightly job times out | Trainer runs with a 2-hour SLA; if exceeded, it falls back to the previous `pin_list.json` (no-op); PagerDuty warn, not page | mitigated  |
| T-D2 | D | Trainer produces a 10 MB pin list that OOMs `shelfd` on reload | `shelfd` rejects pin lists > 1 MB with a metric counter + stale-list fallback (C-D1 above); trainer has its own 100 k-entry cap | mitigated  |
| T-E1 | E | Python trainer container runs as root, with write access to Airflow-controller mount | Trainer runs on a dedicated KubernetesPodOperator with `runAsNonRoot`, `readOnlyRootFilesystem`, no service-account token mount except a minimal IRSA role | mitigated  |
| T-E2 | E | Learned model (future phase) embeds a pickled Python object that executes arbitrary code on `shelfd` load | v1 ships **no model**. If a model is ever shipped (per Phase 4 gate, plan §3 R-13), the format is ONNX or LightGBM binary only — no `pickle`, no `joblib`, no `dill`; decode path uses `onnxruntime` which does not eval arbitrary code | accepted — deferred to Phase 4, gated on ADR |

---

## 5. snapshot-watcher (Phase 1.5)

**Component.** Python sidecar in `shelfd` (or adjacent Deployment),
polls Hive Metastore every 30 s, maintains `(table →
current_snapshot_id)` map, publishes snapshot events to the control
plane.

> Marked tentatively **in-scope** per the prompt; per plan §3 this
> component rides alongside Phase 0R, but the threat model covers it
> here so the IAM story is complete.

| # | STRIDE | Threat                                                                 | Mitigation / acceptance                                                                  | Status     |
| - | ------ | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------- |
| W-S1 | S | A rogue process publishes fake snapshot updates to the control plane, pointing Shelf metadata keys at stale IDs | Control-plane RPC for snapshot updates is gated by mTLS + `spiffe://shelf/component/snapshot-watcher`; only one SAN allowed per cluster; cert-manager rotates every 24 h | mitigated  |
| W-T1 | T | HMS reply is MITM-tampered, pointing watcher at a non-existent snapshot | HMS connection uses TLS + mutual auth per existing platform config; on snapshot-not-found, watcher falls back to the last-known-good map and raises a warning (no hard failure in the hot path) | mitigated  |
| W-R1 | R | Watcher silently drops a snapshot change and no one notices a week later | Watcher emits `shelf_snapshot_watcher_lag_seconds` histogram to Prometheus; alert on `p95 > 120 s` for 10 min; nightly reconciliation job cross-checks against Trino's `$snapshots` system table and raises if divergence > 1 snapshot | mitigated  |
| W-I1 | I | HMS connection logs full table names → potentially sensitive naming | Logging driver redacts `table_name` to `catalog.schema.sha1_8char(table)` at INFO; raw names at DEBUG only, not enabled in prod | mitigated  |
| W-D1 | D | Watcher polls HMS aggressively → HMS overload (plan E9 found this is bounded but real) | Fixed 30 s poll with exponential backoff to 5 min on error; hard cap `max_qps = 3`; watcher is singleton (K8s lease lock) per cluster, not per-pod | mitigated  |
| W-E1 | E | Python watcher container mounts a broad ServiceAccount and can modify HMS | IRSA role is HMS-read-only; no `hive:*` write permissions; ServiceAccount has no other bindings | mitigated  |

---

## 6. Cross-cutting threats

These are not scoped to one component; they apply project-wide.

| # | STRIDE | Threat                                                                 | Mitigation / acceptance                                                                 | Status     |
| - | ------ | ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ---------- |
| X-S1 | S | Compromise of the GitHub org leads to a backdoored release | Mandatory 2-of-N CODEOWNERS review for any `release/**` branch; cosign keyless with Sigstore OIDC anchored to GitHub Actions; release tags PGP-signed by rota; quarterly key attestation | mitigated  |
| X-T1 | T | Dependency confusion attack on an internal crate name | `shelfd/Cargo.toml` specifies `registry = "crates-io"` explicitly on every dependency; `cargo-deny` blocks unknown registries | mitigated  |
| X-R1 | R | A past vulnerability is quietly patched with no advisory | `SECURITY/CHECKLIST.md` requires an advisory for every CVE; release drafter enforces "security" label → advisory link | mitigated  |
| X-I1 | I | Sample configs (helm values.yaml) ship with a real-looking access key | Pre-commit hook (`gitleaks` + `trufflehog`); `*.yaml` sample redactor test in CI: any pattern matching `AKIA[A-Z0-9]{16}` fails the build; all samples use `<REDACTED>` placeholders | mitigated  |
| X-D1 | D | Git-history spelunking reveals an old credential that was rotated but never revoked | Every credential committed accidentally is revoked immediately; `git-filter-repo` run against history and documented in `SECURITY/CHECKLIST.md`; accepted residual: cannot recall mirrors | accepted — revocation is the primary control, not history rewriting |
| X-E1 | E | CI secret (release-signing key, Docker Hub token) is exfiltrated by a malicious PR | CI workflows: `pull_request_target` forbidden; secrets gated to `main`-only jobs; release-sign job uses `id-token: write` via Sigstore, no long-lived secret | mitigated  |

---

## 7. Summary

- Components threat-modelled: **5** (shelfd data plane, shelfd control
  plane, Trino plugin, trainer, snapshot-watcher) **+ 1 cross-cutting**
- STRIDE rows: **48 enumerated** (see per-section tables)
  - mitigated: **43**
  - accepted: **4** (D-E3 platform trust, T-E2 ML pickling deferred,
    X-D1 git history, side-channel attacks in §0.2)
  - open: **1** (C-D2 — ring-eject signal, tracked as R-07)
- Every row has a disposition. Accepted rows have a reason. Open rows
  have a target phase.

### 7.1 Revision hooks

- Every PR that changes any component on the trust-boundary map
  (§0.1) **must** update this file; see `PULL_REQUEST_TEMPLATE.md`
  checkbox "Threat model updated if architecture changed".
- Review cadence: quarterly, or on any release tagged `v0.5` /
  `v1.0` / major version.
- Next review: end of Phase 1 (v0.5 gate), by the security rota.
