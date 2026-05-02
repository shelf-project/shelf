# SHELF-S4 — Trino blob-cache SPI handoff (`trinodb/trino#29184`)

**Status:** source diff staged on a public fork branch; **NOT submitted upstream**.
**Date:** 2026-05-02
**Owner (next step):** maintainer with JDK 25 + Trino dev environment.

---

## TL;DR

- Implemented the **`Plugin.getFileSystemFactories()`** SPI hook against `trinodb/trino` `master`.
- Source diff is **8 files, +295 / −2 LOC**, staged on `aamir306/trino` branch
  **`shelf/fs-spi-hook`** — commit
  [`9d68b98`](https://github.com/aamir306/trino/commit/9d68b98e1dde8ca8aa87e913a511c7df7f3f8b34).
- **Not submitted upstream** because local toolchain is JDK 17 only and the
  `trino-spi:480` jar requires class-file major 69 (JDK 25). Per the SHELF-S4
  brief: *"DO NOT submit a draft if you can't compile it locally"*.
- The diff is a **discussion-shaped PR**, not a ready-to-merge one. Three explicit
  open design questions are baked into the commit body; the maintainer who
  picks it up should resolve them with `@wendigo` (the Trino blob-cache SPI
  author) before opening on `trinodb/trino`.
- This unblocks **SHELF-S5** — wiring `ShelfFileSystem` (the dormant
  package-private factory in `clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java`
  lines 41–47, 60–67) as a real in-process FS factory once the upstream PR lands.

---

## Why this matters for shelf

Workspace memory (locked 2026-04-24, ADR-0012):

> Trino 480's public SPI has **NO** `Plugin.getFileSystemFactories()` and `Connector`
> has **no** `getTrinoFileSystemFactory()` — verified against `trino-spi-480.jar`
> bytecode; built-in FS factories (native-s3, gcs, azure) register via internal
> Guice bindings that aren't on the public SPI, so a plugin **structurally cannot**
> register a `TrinoFileSystemFactory` today.

> v0.5 Shelf→Trino wiring is therefore the **S3-endpoint swap** (SHELF-22 shim
> on port 9092, `s3.endpoint=http://shelfd:9092`). The existing
> `ShelfFileSystemFactory` + `ShelfInputFile` + `ShelfInputStream` (~800 LOC,
> 100+ JUnit tests, package-private-wired via `ShelfPlugin.buildFileSystemFactory`)
> stays **dormant-but-ready** — the override needed is **~5–10 LOC** the day
> upstream lands the hook.

S4 is the day-1 step toward landing that hook upstream. S5 (downstream activation)
becomes a sub-10-LOC patch on `clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java`
once S4 lands.

---

## What was implemented

### Source diff: `aamir306/trino` branch `shelf/fs-spi-hook`

**Branch URL:**
[github.com/aamir306/trino/tree/shelf/fs-spi-hook](https://github.com/aamir306/trino/tree/shelf/fs-spi-hook)

**Commit:** [`9d68b98`](https://github.com/aamir306/trino/commit/9d68b98e1dde8ca8aa87e913a511c7df7f3f8b34) — `Add Plugin.getFileSystemFactories() SPI hook (#29184)`

**Diff stats** (8 files, **+295 / −2** LOC):

| File | Δ | Role |
|---|---|---|
| `core/trino-spi/src/main/java/io/trino/spi/filesystem/TrinoFileSystemFactory.java` | **+66 / −0** (NEW) | SPI marker interface — `getName()`, `Object create(ConnectorIdentity)`, `Object create(ConnectorSession)` defaults. |
| `core/trino-spi/src/main/java/io/trino/spi/Plugin.java` | **+24 / −0** | Adds `default Iterable<TrinoFileSystemFactory> getFileSystemFactories() { return emptyList(); }`. |
| `lib/trino-filesystem/src/main/java/io/trino/filesystem/TrinoFileSystemFactory.java` | **+16 / −2** | Existing engine-internal interface now `extends io.trino.spi.filesystem.TrinoFileSystemFactory` via Java covariant returns; adds `default getName()`. |
| `core/trino-main/src/main/java/io/trino/server/PluginManager.java` | **+21 / −1** | Iterates `plugin.getFileSystemFactories()`, downcasts each to the engine subtype, registers in the new sink. |
| `core/trino-main/src/main/java/io/trino/filesystem/manager/FileSystemFactoryRegistry.java` | **+59 / −0** (NEW) | Engine-wide singleton registry; `addFactory()` rejects duplicate scheme names. |
| `core/trino-main/src/main/java/io/trino/server/ServerMainModule.java` | **+4 / −0** | Guice singleton binding for the registry. |
| `core/trino-main/src/main/java/io/trino/testing/PlanTester.java` | **+3 / −1** | New `FileSystemFactoryRegistry` arg in the test path's direct `new PluginManager(...)` call. |
| `core/trino-main/src/test/java/io/trino/server/TestPluginManagerFileSystemFactories.java` | **+104 / −0** (NEW) | Unit tests — default empty, plugin override, registry add, duplicate-name rejection. |

### Why a marker interface (and not a full move)

The cleanest theoretical design would be to **move** `io.trino.filesystem.TrinoFileSystemFactory`
and its full closure (`TrinoFileSystem`, `Location`, `FileEntry`, `FileIterator`,
`TrinoInputFile`, `TrinoOutputFile`, `TrinoInput`, `TrinoInputStream`, `UriLocation`,
`TrinoFileSystemException`) into `io.trino.spi.filesystem`. That would give plugins
a typed `TrinoFileSystem create(...)` at the SPI boundary and remove the `Object`
return on the marker.

**Why we didn't:**

1. The closure has two **out-of-spi** dependencies that would also need to move
   or be cut:
   - `TrinoFileSystem` imports `io.trino.filesystem.encryption.EncryptionKey` (subpackage).
   - `TrinoOutputFile` imports `io.trino.memory.context.AggregatedMemoryContext` (separate `core/trino-memory-context` module — not currently a dep of `trino-spi`).
2. **~100+ existing files** import `io.trino.filesystem.TrinoFileSystemFactory`
   directly (full grep in commit body). All would need import updates.
3. This is a **maintainer-decision-shaped change**, not a build-config tweak —
   it changes the SPI surface in a way only the Trino core team can sign off on.

The marker interface design is a **deliberate compromise**: it adds the SPI
hook today with `Object`-typed `create()` (the engine downcasts at registration
time), keeps the engine-internal `TrinoFileSystem` closure where it lives, and
leaves the "should we full-move?" question as an explicit design topic in the
PR body for `@wendigo` and the Trino core team to weigh in on.

---

## Local toolchain status

| Tool | Available | Required | Status |
|---|---|---|---|
| `git` | yes | yes | ✓ |
| `mvn` | 3.9.15 | yes | ✓ |
| **JDK** | **17 (Zulu) + 21 (Temurin)** | **25** (`trino-spi:480` is class-file major 69) | **✗ — blocker** |
| `gh` | yes (scopes: `gist, read:org, repo, workflow`) | yes | ✓ |

`./mvnw -pl :trino-spi -am clean install` would fail with
`UnsupportedClassVersionError`. **No local compile attempt was made**, per the
SHELF-S4 brief constraint.

### What the next operator needs

```bash
# Install Temurin JDK 25 (or any JDK 25 distribution)
# macOS via Homebrew:
brew install --cask temurin@25
# or via SDKMAN:
sdk install java 25-tem

# Then check out the branch and validate:
git clone https://github.com/aamir306/trino.git
cd trino && git checkout shelf/fs-spi-hook
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -pl :trino-spi,:trino-main,:trino-filesystem -am clean install -DskipTests=false

# Run the new test specifically:
./mvnw -pl :trino-main test -Dtest=TestPluginManagerFileSystemFactories
```

Expected gates before submitting upstream:
- `./mvnw clean install -pl :trino-spi -am` clean.
- `./mvnw clean install -pl :trino-main -am` clean (covers PluginManager rewire + the new test).
- Existing `lib/trino-filesystem` tests still pass (ensures the `extends` clause + covariant return doesn't break consumers).
- Spot-check at least one existing plugin that uses `TrinoFileSystemFactory`
  (e.g. `plugin/trino-iceberg`) compiles cleanly — if it does, the ~100
  consumer imports are safe.

---

## PR body — exact text to submit when JDK 25 is available

> **Title:** `Add Plugin.getFileSystemFactories() SPI hook (#29184)`
>
> **Repo / base:** `trinodb/trino` `master`
> **Head:** `aamir306:shelf/fs-spi-hook`
> **Mark as: DRAFT** (Trino convention — get reviewer attention as draft, mark ready when CI clean).

Use the **commit body** as the PR description verbatim — it already covers the
full design rationale, backward-compat analysis, and the four open questions
for reviewers. Trim or expand as `@wendigo` / `@martint` give feedback.

A pre-submit Slack DM to `@wendigo` on `trino.io/slack` (per
[`docs/discovery/upstream/contacts.md`](https://github.com/shelf-project/shelf/blob/main/docs/discovery/upstream/contacts.md))
is **strongly recommended** — there's an active overlap with #29184 (the
unified blob-cache SPI he's authoring) and we want him to bless this as
complementary, not competing, before public CI runs start eating reviewer
attention.

---

## Open design questions for the Trino reviewer

These are also embedded in the commit body — surfaced here for visibility:

1. **Full-move vs marker?** Should `TrinoFileSystemFactory` + closure be moved
   into `trino-spi` (eliminates the `Object`-typed `create()` at the SPI
   boundary, ~14-file move + ~100 import-update touch)? The marker design
   keeps the diff small but trades type safety at the SPI boundary.

2. **`io.trino.filesystem.*` in `SPI_PACKAGES`?** Today it isn't —
   plugin classloaders see only `io.trino.spi.*`, `com.fasterxml.jackson.annotation.*`,
   `io.airlift.slice.*`, `io.opentelemetry.{api,context}.*`. Existing iceberg/hive
   plugins ship their own copy of `lib/trino-filesystem` in their plugin distribution,
   creating a class-identity-across-classloaders fragility (the `instanceof
   io.trino.filesystem.TrinoFileSystemFactory` check in `PluginManager` works
   today only because of this). Cleaner is to add `io.trino.filesystem.` to
   `SPI_PACKAGES` so the engine's class is shared, but that pulls
   `lib/trino-filesystem`'s deps (encryption, memory-context) into the parent
   classloader scope — non-trivial.

3. **FileSystemModule consumption.** This patch wires the registry into
   `PluginManager` but the per-catalog `FileSystemModule` (which today binds
   `Map<String, TrinoFileSystemFactory>` for built-in S3/GCS/Azure) **does not
   yet read from the registry**. That's a follow-up patch — the maintainers
   should shape that API (does the plugin factory go into the per-catalog map
   on every instantiation? Engine-global override? Per-catalog opt-in?). Left
   intentionally out of this PR so the hook itself can be reviewed in
   isolation.

4. **Relationship to `trinodb/trino#29184`** (`@wendigo`'s DRAFT
   `Implement unified blob cache plugin SPI`). That PR introduces
   `Plugin.getBlobCacheManagerFactories()` returning a
   `BlobCacheManagerFactory` — a **higher-level cache abstraction** that the
   engine wraps around its own `TrinoFileSystem`. Our hook is **lower-level** —
   it lets a plugin **own the entire filesystem path**, not just intercept the
   cache layer. **The two are complementary**:
   - `getBlobCacheManagerFactories()` (#29184): "let the plugin cache bytes the
     engine reads." Best for Alluxio-style worker-local cache.
   - `getFileSystemFactories()` (this PR): "let the plugin BE the filesystem."
     Best for shelf-style cluster-shared cache where ETag-versioned content-
     addressed keys (ADR-0011) need full read-path control.

   Shelf's design memo (BLUEPRINT.md, ADR-0012) explicitly chose the
   filesystem-factory route because the blob-cache abstraction would force
   shelf to give up control over the connection path that lets it surface
   `shelf_hits_total` / `shelf_misses_total` cleanly per-tag and short-circuit
   negative-cache HEAD lookups via `head_lru`. **Both PRs should land**;
   neither is a substitute for the other.

---

## What this commit deliberately does NOT do

Per the SHELF-S4 brief:

- **Does NOT modify shelf's `clients/trino/` code.** That's SHELF-S5's job
  (the ~5-10 LOC override of `ShelfPlugin.buildFileSystemFactory()` →
  `ShelfPlugin.getFileSystemFactories()`).
- **Does NOT bump Trino version dependency in shelf's chart.** That's an
  operator decision once the upstream PR merges.
- **Does NOT propose any breaking change** to existing `Plugin` SPI methods —
  additive only (new default method returning empty list, plus a new package
  in `trino-spi`).
- **Does NOT submit to `trinodb/trino`.** No `gh pr create` to upstream was
  run. The fork branch is staged for a maintainer with JDK 25 to pick up.

---

## Status of `trinodb/trino#29184`

Verified live 2026-05-02:

- **State:** OPEN, marked `DRAFT: Implement unified blob cache plugin SPI`.
- **Author:** `@wendigo` (Starburst engineer, prolific Trino contributor).
- **Last activity:** 2026-04-30 (active design iteration).
- **Size:** 2696 additions, 1203 deletions across ~80 files.
- **CodeRabbit review:** 12 actionable comments, mostly correctness fixes
  in `plugin/trino-blob-cache-alluxio/.../TracingCacheManager.java` (param
  reordering, `CACHE_FILE_READ_POSITION` vs `CACHE_FILE_READ_SIZE` label,
  missing `pageOffset` in delegate call). None of these affect the SPI
  surface itself — the `BlobCacheManagerFactory` interface looks
  finalised.
- **Plugin.java diff in #29184:** `+6 / −0` lines — adds
  `getBlobCacheManagerFactories()` returning the new `BlobCacheManagerFactory`
  type from `io.trino.spi.cache`. **Distinct from our `getFileSystemFactories()`**
  — both can coexist on `Plugin`.

---

## Next steps

1. **Operator with JDK 25** runs the validation block above and pushes to upstream
   if green.
2. **Slack** `@wendigo` on `trino.io/slack` (channel `#core-dev`) before submitting
   — share the branch URL and ask whether he'd prefer this hook to land
   independently or after #29184. Use the talking points in
   [`docs/discovery/upstream/29184-review-comment.md`](https://github.com/shelf-project/shelf/blob/main/docs/discovery/upstream/29184-review-comment.md).
3. **Once the upstream PR is open**, file SHELF-S5 to wire `ShelfPlugin.getFileSystemFactories()`
   downstream (~5-10 LOC change to `clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java`).
4. **Once the upstream PR merges**, bump shelf's chart `appVersion` to the next
   Trino minor (likely 481+) and stage a per-replica canary for the in-process
   filesystem path. Per workspace memory ("rep-3 is the explicit rollback escape
   hatch"), order: rep-2 → rep-1 → rep-0; rep-3 stays on direct-S3 until rep-0
   has 30 clean days on the in-process path.

---

## Contacts

- **Trino blob-cache SPI author:** `@wendigo` on
  [trino.io/slack](https://trino.io/slack) `#core-dev`. Email per his GitHub
  profile if needed.
- **Trino contributor docs:** [trino.io/development/process](https://trino.io/development/process).
- **DCO requirement:** Trino requires `Signed-off-by:` trailer on every
  commit. The commit on `shelf/fs-spi-hook` is already signed off as
  `aamirpw306 <aamir.k306@yahoo.com>`.
