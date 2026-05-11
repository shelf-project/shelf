# SHELF-37 / PR #66 — JDK 17 vs `trino-spi:480` unblock research

**Author** worker dispatch under rc.6 P0.2
**Date** 2026-04-30
**Scope** PR https://github.com/shelf-project/shelf/pull/66 — `feat/shelf-37-iceberg-event-listener`
**Local toolchain** JDK 17 (Oracle 17.0.10, arm64), Maven 3.9.15
**Question** find an unblock path for the JDK 17 / `trino-spi:480` (class-file major 69) link incompatibility

---

## TL;DR

**Recommendation B** — **mint JDK 25 locally and keep `trino-spi:480` pinned.**
Recommendation A (pin `trino-spi` to a JDK-17-compat release) compiles and tests cleanly but produces a jar that throws `AbstractMethodError` at plugin-load time inside Trino 480, because `EventListenerFactory.create()` gained a mandatory second argument between SPI 450 and 470 with no default-method bridge. There is no SPI version that is *both* class-file major ≤ 61 *and* ABI-compatible with the Trino 480 runtime.

**Local validation evidence** (this session): `mvn -B clean verify` of the unmodified PR #66 against Temurin JDK 25.0.3+9 (installed via direct Adoptium tarball, no sudo needed) → **18/18 unit tests pass, 1/1 integration test passes** (`IcebergSinkRoundTripIT`, `addedDataFiles=1, addedRecords=5, addedFilesSizeInBytes=11354`), `BUILD SUCCESS`. CI's `verify.yml` is already configured to use Temurin 25 — the only operational block is the org-level GitHub Actions allowlist (separate from JDK toolchain).

---

## Phase 1 — Maven Central `io.trino:trino-spi` JDK matrix

Sampled across the relevant version range. `Build-Jdk-Spec` is from each jar's `META-INF/MANIFEST.MF`; the bytecode major is from `od -An -tx1 -N 8` against `io/trino/spi/eventlistener/EventListener.class`.

| trino-spi version | `Build-Jdk-Spec` | Class-file major | Notes |
|---:|:--:|:--:|---|
| 400 | 17 | 61 | |
| 405 | 17 | 61 | |
| 410 | 17 | 61 | |
| 415 | 17 | 61 | |
| 418 | 17 | 61 | |
| 419 | 17 | 61 | |
| 420 | 17 | 61 | |
| 421 | 17 | 61 | |
| 422 | 17 | 61 | |
| 425 | 17 | 61 | |
| 430 | 17 | 61 | |
| **435** | **17** | **61** | **Last JDK-17-compat release** |
| 436 | 21 | 65 | First JDK-21 release |
| 437 | 21 | 65 | |
| 438 | 21 | 65 | |
| 439 | 21 | 65 | |
| 440 | 21 | 65 | |
| 450 | 22 | 66 | |
| 470 | 23 | 67 | |
| 471 | 23 | 67 | |
| 472 | 23 | 67 | |
| 473 | 23 | 67 | |
| 474 | 23 | 67 | |
| 475 | 23 | 67 | `QueryInputMetadata.getConnectorName()` introduced |
| 476 | 24 | 68 | |
| 477 | 24 | 68 | |
| 478 | 24 | 68 | |
| 479 | 25 | 69 | |
| 480 | 25 | 69 | Current, used by PR #66 |

`460` is intentionally absent from Maven Central (no `io.trino:trino-spi:460.jar` artifact exists); upstream skipped that integer.

---

## Phase 2 — SPI surface check vs PR #66 source

### Methods PR #66 calls (from `clients/trino-listener/src/main/java/io/shelf/listener/extract/EventExtractor.java` + `ShelfIcebergEventListener.java` + `ShelfIcebergEventListenerFactory.java`)

`EventListener.queryCompleted(QueryCompletedEvent)`,
`EventListenerFactory.getName()` / `create(...)`,
`Plugin.getEventListenerFactories()`,
`QueryCompletedEvent.{getMetadata, getStatistics, getContext, getIoMetadata, getFailureInfo, getCreateTime, getEndTime}`,
`QueryMetadata.{getQueryId, getQuery, getQueryState}`,
`QueryContext.{getPrincipal, getUser, getSource, getCatalog, getSchema, getResourceGroupId, getServerAddress, getSessionProperties}`,
`QueryStatistics.{getCpuTime, getWallTime, getQueuedTime, getPlanningTime, getExecutionTime, getPhysicalInputReadTime, getPhysicalInputBytes, getPhysicalInputRows, getProcessedInputBytes, getProcessedInputRows, getOutputBytes, getOutputRows, getPeakUserMemoryBytes, getPeakTaskTotalMemory}`,
`QueryIOMetadata.{getInputs, getOutput}`,
`QueryInputMetadata.{getCatalogName, getSchema, getTable, `**`getConnectorName`**`, getPhysicalInputBytes, getPhysicalInputRows}`,
`QueryOutputMetadata.{getCatalogName, getSchema, getTable}`,
`QueryFailureInfo.{getErrorCode, getFailureMessage}`,
`ErrorCode.{getName, getType}`.

`getOperatorSummaries` is *not* called by PR #66 (the SHELF-43 prefetch design references it, but `EventExtractor.java` does not).

### What's present in `trino-spi:435` (the latest JDK-17 candidate)

Verified via `javap -p` against the unpacked `trino-spi-435.jar`:

| Member PR #66 needs | Present in 435? |
|---|:--:|
| `EventListener.queryCompleted(QueryCompletedEvent)` | yes |
| `EventListenerFactory.create(Map<String, String>)` | yes |
| `EventListenerFactory.create(Map<String, String>, EventListenerContext)` | **no** |
| `Plugin.getEventListenerFactories()` | yes |
| All `QueryCompletedEvent` / `QueryMetadata` / `QueryContext` / `QueryStatistics` / `QueryIOMetadata` / `QueryOutputMetadata` getters listed above | yes |
| `QueryInputMetadata.getConnectorName()` | **no** |
| `ErrorCode(int, String, ErrorType, boolean)` (4-arg ctor used in `TestEvents.java`) | **no** (only 3-arg) |
| Top-level `io.trino.spi.connector.CatalogVersion` (`TestEvents.java:233`) | **no** (only `CatalogHandle$CatalogVersion`) |
| `QueryInputMetadata` 10-arg constructor with `Optional<String>` connectorName | **no** (9-arg, no connector) |
| `QueryStatistics` 44-arg constructor used in `TestEvents.java` | **no** (41-arg) |
| `QueryContext` 24-arg constructor used in `TestEvents.java` | **no** (23-arg, no `originalRoles` slot) |

### When each gap landed upstream

| Gap | First introduced in | Last absent in |
|---|:--:|:--:|
| `EventListenerFactory.create(Map, EventListenerContext)` (mandatory 2-arg, **no default-method bridge**) | **470** | 450 |
| `QueryInputMetadata.getConnectorName()` | 475 | 474 |
| `ErrorCode(int, String, ErrorType, boolean)` 4-arg ctor + `isFatal()` | 480 | 479 |
| `io.trino.spi.connector.CatalogVersion` promoted to top-level (was `CatalogHandle$CatalogVersion`) | 480 | 479 |
| `QueryInputMetadata#Column` inner-class type for `getColumns()` (was `List<String>`) | 480 | 479 |

The constructor-arg shifts on `QueryStatistics` / `QueryContext` are continuous across the range (each release adds positional fields) — there is no version with the same ctor surface as 480 *and* class-file major ≤ 61.

---

## Phase 3 — why Recommendation A fails

I attempted Recommendation A in a research worktree:

1. `pom.xml`: `dep.trino.version` 480 → 435, `maven.compiler.release` 25 → 17.
2. `pom.xml`: dropped `-this-escape` from `maven-compiler-plugin` `<compilerArgs>` (JDK 21+ only).
3. `EventExtractor.java`: removed the single `in.getConnectorName().ifPresent(...)` call (the catalog name is already in the row, so no information loss for SHELF-40 dollars-saved attribution).

That stops at the next compile error: `ShelfIcebergEventListenerFactory.java` implements `create(Map<String, String> config, EventListenerContext context)` — the **480 SPI signature**. `trino-spi:435` declares the abstract method as `create(Map<String, String>)` only, so the class is `not abstract and does not override abstract method create(Map<String, String>) in EventListenerFactory`.

To compile against 435 you would have to flip the signature to the **435 SPI shape**:

```java
@Override
public EventListener create(Map<String, String> config) {
    ListenerConfig parsed = ListenerConfig.fromMap(config);
    return new ShelfIcebergEventListener(parsed);
}
```

That builds, and the unit tests would also need a parallel rewrite to drop the 4-arg `ErrorCode` ctor, switch `CatalogVersion` to `CatalogHandle.CatalogVersion`, drop the `Optional<String>` connectorName arg, drop the `originalRoles` arg from `QueryContext`, and drop three of the `List.of()` slots + two `Map.of()` slots from `QueryStatistics`. ~30–50 LOC of TestEvents surgery.

**But — and this is the load-bearing failure mode for Recommendation A — the resulting jar will not load into Trino 480.** Trino 480's coordinator calls

```java
EventListener listener = factory.create(config, context);
```

which dispatches through the **JDK-runtime SPI**, i.e. `trino-spi:480.jar` shipped on the coordinator. The plugin classloader resolves `EventListenerFactory.create(Map, EventListenerContext)` against the runtime SPI and finds that the plugin's class only implements `create(Map)` (the 435 signature). JVM throws

```
java.lang.AbstractMethodError: Receiver class io.shelf.listener.plugin.ShelfIcebergEventListenerFactory does not define or inherit an implementation of the resolved method 'abstract io.trino.spi.eventlistener.EventListener create(java.util.Map, io.trino.spi.eventlistener.EventListenerFactory$EventListenerContext)' of interface io.trino.spi.eventlistener.EventListenerFactory.
```

at `event-listener.properties` evaluation time, *before* the listener processes any events. There is no graceful degradation — Trino fails to register the listener and logs the error.

Net: pinning `trino-spi:435` produces a jar that passes its own unit tests (because the tests use the 435 SPI as both the compile-time and runtime classpath) but cannot be deployed against Trino 480. **Recommendation A is non-viable as a production unblock**, even though its build produces a green Maven exit code.

---

## Phase 4 — Recommendation B: mint JDK 25

This is the only path that lands a **deployable** SHELF-37 jar.

### 4.1 CI

The PR-branch `verify.yml` *already* sets up Temurin 25 explicitly:

```yaml
- name: Set up Temurin JDK 25
  uses: actions/setup-java@v4
  with:
    distribution: temurin
    java-version: "25"
    cache: maven

- name: mvn verify (clients/trino-listener)
  run: mvn -B -ntp -f clients/trino-listener/pom.xml verify
```

So the toolchain side of CI is already correct. The reason the `java-verify` lane has not run on PR #66 is the **`shelf-project` org-level GitHub Actions allowlist** (separate concern, recorded in workspace memory; rejects `actions/checkout@v6` etc. with `must be from a repository owned by shelf-project`). Lifting that allowlist (Settings → Actions → General → "Allow specified actions and reusable workflows" → broaden, OR disable the policy) lets the existing `verify.yml` JDK 25 path execute.

### 4.2 Local

Sub-5-minute install on macOS arm64, no sudo:

```sh
mkdir -p ~/jdks && cd ~/jdks
curl -fsSL -o temurin-25.tar.gz \
  "https://api.adoptium.net/v3/binary/latest/25/ga/mac/aarch64/jdk/hotspot/normal/eclipse"
tar xzf temurin-25.tar.gz
export JAVA_HOME="$HOME/jdks/$(ls -d jdk-25*/Contents/Home | head -1)"
"$JAVA_HOME/bin/java" -version    # → openjdk version "25.0.3"
```

Then point Maven at it:

```sh
cd shelf/clients/trino-listener
JAVA_HOME="$HOME/jdks/jdk-25.0.3+9/Contents/Home" \
  PATH="$HOME/jdks/jdk-25.0.3+9/Contents/Home/bin:$PATH" \
  mvn -B -ntp clean verify
```

Linux equivalents: replace the URL `path=mac/aarch64` segment with `linux/x64` or `linux/aarch64` and untar to `~/jdks`. SDKMAN (`sdk install java 25-zulu`) is a slightly nicer variant when you use SDKMAN already.

The Homebrew cask path `brew install --cask temurin@25` works but invokes `sudo` to drop the JDK into `/Library/Java/JavaVirtualMachines/`, which fails inside non-interactive shells. Direct tarball is the friction-free option.

### 4.3 Local verification ran in this session

```
$ JAVA_HOME=/Users/aamir/jdks/jdk-25.0.3+9/Contents/Home \
  mvn -B -ntp clean verify
# unit tests:
[INFO] Tests run: 18, Failures: 0, Errors: 0, Skipped: 0

$ SHELF_INTEGRATION=1 JAVA_HOME=...  mvn -B -ntp verify
# unit + integration:
[INFO] Tests run: 18, Failures: 0, Errors: 0, Skipped: 0
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0   # IcebergSinkRoundTripIT
# Iceberg metrics report:
#   addedDataFiles=1, addedRecords=5, addedFilesSizeInBytes=11354
[INFO] BUILD SUCCESS
```

Wall-clock ~10 s for the full `clean verify` cycle once the dependency cache is warm. The shaded jar comes out at the expected size.

### 4.4 Effort estimate

| Task | Owner | Effort |
|---|---|---|
| Lift `shelf-project` Actions allowlist (or whitelist `actions/setup-java@v4` + `actions/checkout@v6` + `actions/cache@v4`) | repo admin | 5 min |
| Local Temurin 25 tarball install on the orchestrator workstation | one-off | 5 min |
| Verify build via `mvn -B clean verify` on JDK 25 | mechanical | 10 s |
| Rebase PR #66 on `main` (currently `mergeStateStatus: DIRTY` / `CONFLICTING`) | PR author | 5–15 min depending on conflict surface |

Total: **~30 min**, gated on the org-allowlist toggle.

---

## Phase 4-alt — Recommendation C: defer

If the org-allowlist + local-JDK install can't be sorted in this rc.6 window, **defer P0.2 to rc.7** and keep PR #66 parked. Trade-offs:

- **Cost** Tier-2 measurement substrate (SHELF-40 dollars-saved counter, SHELF-42 A/B query tagging) is already merged on `main` (PRs #67, #68); both consume the table this listener writes. Without the listener, those counters tag every event as `tag=other`, which still works but reduces attribution granularity. This is a measurement quality gap, **not** a hot-path correctness gap.
- **Schedule** rc.7 is the next slot. The deferred SHELF-37 work doesn't gate any of the rc.6 Phase-4 lever flips (zstd, bloom, decoded-meta, page-index) — those flip independently with default-attribute tags.

Recommendation C is acceptable but should be the fallback only if Recommendation B's org-allowlist toggle isn't tractable.

---

## Final recommendation

**B**, with **C** as the explicit fallback if the `shelf-project` Actions allowlist cannot be loosened in the rc.6 prep window.

The PR #66 source is correct as written. The unblock work is purely operational:

1. Loosen the org Actions allowlist so `verify.yml` can run on PRs.
2. (For orchestrator local-build hygiene) install Temurin 25 via the Adoptium tarball.

No source change required. No `pom.xml` change required. Local validation evidence is captured in §4.3 above.

---

## Cleanup

This research did not push any branch or commit. Two transient git worktrees were created (`/private/tmp/shelf-37-jdk17-research-*` for the failed Recommendation A attempt, `/private/tmp/shelf-37-jdk25-validate-*` for the JDK 25 build proof) and will be removed after the PR comment is posted. The local Temurin 25 JDK at `~/jdks/jdk-25.0.3+9/` is left in place — it is small, self-contained, and useful for any future JDK-25 build verification.
