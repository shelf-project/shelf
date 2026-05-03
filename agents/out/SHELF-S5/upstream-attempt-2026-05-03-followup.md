# S5 upstream PR — fix-up attempt 2026-05-03 (followup to PR #117)

## TL;DR

Applied the two mechanical blockers documented in PR #117. Build still fails —
this time on a **Maven reactor cycle** that PR #117's diagnostic missed: the
recommended fix (adding `trino-filesystem` as a compile-scope dependency of
`trino-main`) is structurally impossible because `lib/trino-filesystem` already
holds a **test-scope** dependency back on `trino-main`. Maven 3.9.14 treats this
as a hard cycle in the reactor graph and refuses to plan the build.

Upstream PR was **NOT** submitted — the spec hard-bans submitting an uncompiled
PR to `trinodb/trino`. The fork branch `aamir306/trino:shelf/fs-spi-hook` was
**NOT** force-pushed: the partial fix (test-import only) is not useful without
the pom dep, and the pom dep cannot land mechanically.

This is no longer a tooling-level blocker. It is a **design-level** blocker that
needs an architectural decision before S5 can complete.

## What was applied

### Blocker 1 (PR #117) — checkstyle violation — APPLIED

`core/trino-main/src/test/java/io/trino/server/TestPluginManagerFileSystemFactories.java`

```diff
-import static java.util.List.of;
+import java.util.List;
+
 import static org.assertj.core.api.Assertions.assertThat;
```

```diff
-                return of(custom);
+                return List.of(custom);
```

(Verified locally — file edits applied, no further checkstyle complaints on
this rule.)

### Blocker 2 (PR #117) — missing `trino-filesystem` dep — APPLIED, then EXPOSED A NEW ISSUE

`core/trino-main/pom.xml`, inserted in alphabetical order between
`trino-client` and `trino-geospatial-toolkit`:

```diff
+        <dependency>
+            <groupId>io.trino</groupId>
+            <artifactId>trino-filesystem</artifactId>
+        </dependency>
+
         <dependency>
             <groupId>io.trino</groupId>
             <artifactId>trino-geospatial-toolkit</artifactId>
```

This addition creates the cycle described below.

## The new blocker — Maven reactor cycle

```
$ ./mvnw clean install -pl :trino-spi,:trino-main -am -DskipTests=false -T 1C
[INFO] Scanning for projects...
[ERROR] The projects in the reactor contain a cyclic reference:
        Edge between 'Vertex{label='io.trino:trino-filesystem:481-SNAPSHOT'}'
        and 'Vertex{label='io.trino:trino-main:481-SNAPSHOT'}' introduces to
        cycle in the graph
        io.trino:trino-main:481-SNAPSHOT
          --> io.trino:trino-filesystem:481-SNAPSHOT
            --> io.trino:trino-main:481-SNAPSHOT @
[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/ProjectCycleException
```

### Why

- After Blocker 2's fix: `core/trino-main/pom.xml` declares
  `io.trino:trino-filesystem` at compile scope (forward edge).
- Pre-existing: `lib/trino-filesystem/pom.xml` declares
  `io.trino:trino-main` at **test scope** (back edge), at line 145–149:

  ```xml
  <dependency>
      <groupId>io.trino</groupId>
      <artifactId>trino-main</artifactId>
      <scope>test</scope>
  </dependency>
  ```

- Maven 3.9.14's reactor cycle detector considers **all** scope edges
  (including `test`) when planning the build graph. There is no flag to ignore
  test-scope cycles in modern Maven (Apache Maven 3.6+, see MNG-7136).
- The cycle is detected at reactor-planning time, *before* any module is even
  compiled — so `-DskipTests=true`, `-pl :trino-spi,:trino-filesystem` (without
  `trino-main` in the reactor selection), and similar narrowing flags do **not**
  help: `-am` still adds the cyclic edge.

### Verified — the cycle is fundamental, not a build-flag problem

| Attempt | Command | Result |
| --- | --- | --- |
| 1 | `./mvnw clean install -pl :trino-spi,:trino-main -am -DskipTests=false -T 1C` (the spec command) | **FAIL — ProjectCycleException** |
| 2 | Reverted Blocker 2 (no pom edit), kept Blocker 1 → ran `./mvnw clean install -pl :trino-spi,:trino-main -am -DskipTests=true -T 1C` | **FAIL — 6× `cannot find symbol: TrinoFileSystemFactory`** in `FileSystemFactoryRegistry.java` and `PluginManager.java` (the original Blocker 2). Confirms PR #117's diagnosis that the dep IS needed, but adding it triggers the cycle. |
| 3 | Re-applied Blocker 2; ran `./mvnw clean install -pl :trino-spi,:trino-filesystem -am -DskipTests=true -T 1C` (narrowed `-pl` selector, hoping reactor would skip trino-main) | **FAIL — same cycle.** Maven reads ALL pom files for cycle detection, regardless of `-pl` filter. |

The trino-filesystem→trino-main test-scope edge is load-bearing — three test
files in `lib/trino-filesystem/src/test/` import `io.trino.testing.*`
(QueryRunner, TestingTelemetry, TestingConnectorSession,
io.trino.memory.context.AggregatedMemoryContext) which live in trino-main:

```
$ rg 'import io\.trino\.' lib/trino-filesystem/src/test/ \
  | grep -v 'io.trino.filesystem\|io.trino.spi'
.../TestCacheFileSystemAccessOperations.java:30: import io.trino.testing.TestingTelemetry;
.../TestCacheFileSystemAccessOperations.java:31: import io.trino.testing.connector.TestingConnectorSession;
.../TestCacheFileSystemEncryption.java:29:      import io.trino.memory.context.AggregatedMemoryContext;
.../tracing/CacheFileSystemTraceUtils.java:17:  import io.trino.testing.QueryRunner;
```

So the back-edge cannot be removed without also moving or refactoring those
tests — which is a real design change, beyond the "two mechanical fixes" scope
of this fix-up dispatch.

## Why PR #117's diagnostic missed it

PR #117 stopped after observing `[ERROR] cannot find symbol: TrinoFileSystemFactory`
and concluded "trino-main needs `trino-filesystem` in its pom". That conclusion
is correct in isolation — but PR #117 did not actually apply the fix and
re-run the build. Adding the dep surfaces the deeper Maven-reactor cycle that
the missing-symbol error was *masking*.

The cycle is, in retrospect, a structural consequence of the marker-interface
design choice the original commit (`9d68b98`) made:

- `trino-main` (PluginManager) needs to `instanceof`-check
  `io.trino.filesystem.TrinoFileSystemFactory` — the *engine-internal* type that
  lives in lib/trino-filesystem. (This is intentional per the PR description's
  "PluginManager downcasts the SPI type to the engine type" wording.)
- That requires lib/trino-filesystem on trino-main's compile classpath →
  forward edge.
- lib/trino-filesystem already has a test-scope back edge to trino-main (uses
  `io.trino.testing.QueryRunner` etc) → cycle.

## Options for the operator (in order of design impact, lowest to highest)

### Option A — Drop the engine-type downcast in `PluginManager`

Have `FileSystemFactoryRegistry` and `PluginManager` operate purely on the new
`io.trino.spi.filesystem.TrinoFileSystemFactory` SPI marker; do **not**
`instanceof`-check the engine-internal type. Then trino-main needs nothing
from lib/trino-filesystem at compile time and the cycle disappears.

Cost: registry holds `Object`-typed `create()` returns (per the SPI marker's
default signature). Consumers (FileSystemModule, per-catalog Guice bindings)
that need the engine-internal type would have to do their own downcast at
consumption time. Slightly worse type ergonomics, but **architecturally
honest** — the SPI is the contract, the engine type is implementation detail.

LOC: ~10–15 lines changed across `FileSystemFactoryRegistry.java`,
`PluginManager.java`. No SPI surface change. Likely cleanest path.

### Option B — Move `FileSystemFactoryRegistry` to lib/trino-filesystem-manager

`lib/trino-filesystem-manager` already depends on lib/trino-filesystem at
compile and does **not** depend on trino-main (verified). If
FileSystemFactoryRegistry is in trino-filesystem-manager, trino-main can
depend on trino-filesystem-manager (compile) — but then trino-main →
trino-filesystem-manager → trino-filesystem → trino-main(test) is *also* a
cycle. So this option does **not** work for the registry on its own; it would
also require Option A's downcast removal (or splitting trino-filesystem).

### Option C — Split lib/trino-filesystem into two modules

`lib/trino-filesystem-api` (public types: TrinoFileSystemFactory,
TrinoFileSystem, etc — no test-scope dep on trino-main) and
`lib/trino-filesystem-impl` (with the test back-edge). trino-main depends on
the API module only.

Cost: real module split, ~20 file moves, all consumers (~12 modules across
core/, lib/, plugin/) update their poms. Largest change of the three.

### Option D — Move the offending tests out of lib/trino-filesystem

Relocate `TestCacheFileSystemAccessOperations`,
`TestCacheFileSystemEncryption`, `CacheFileSystemTraceUtils` into a separate
test-only module (e.g. trino-filesystem-tests) that depends on both
trino-filesystem and trino-main. Removes the back-edge.

Cost: a new test module + pom + 3 file moves + adjusting any CI / surefire
config that targets `lib/trino-filesystem` directly.

### Recommendation

**Option A.** It is the only option that ships in a single Trino PR without
any new module / file moves, and it actually improves the SPI separation
(plugins don't need lib/trino-filesystem on their classpath at all). The
trade-off — `Object`-typed returns inside the engine — is exactly what the SPI
marker was designed to do, so consuming Option A pays principal on the design
debt rather than papering over it.

## What was NOT done

- ❌ The fork branch `aamir306/trino:shelf/fs-spi-hook` was **NOT**
  force-pushed. HEAD is still `9d68b98e1dde8ca8aa87e913a511c7df7f3f8b34`.
  Pushing the partial fix (test-import only, without the pom dep) would leave
  the branch in a still-uncompilable state and would not advance the upstream
  submission.
- ❌ Upstream PR was **NOT** submitted to `trinodb/trino`. The build fails;
  the spec hard-bans submitting an uncompiled PR.

## Environment captured

- macOS 15.6, Darwin 24.6.0, arm64 (M-series, Apple Silicon)
- JDK 25 (Temurin 25.0.3+9 LTS) at `/tmp/jdk25/Contents/Home` (already in
  place from PR #117's Tier-3 install)
- Maven 3.9.14 (`./mvnw` from the fork, distribution
  `apache-maven-3.9.14-bin.zip`)
- Build logs: `/tmp/trino-build-fixup.log` (with pom edit — cycle),
  `/tmp/trino-build-no-pom.log` (without pom edit — missing-symbol),
  `/tmp/trino-build-fs-only.log` (narrowed -pl — still cycle).
- Fork HEAD on remote (unchanged): `9d68b98e1dde8ca8aa87e913a511c7df7f3f8b34`
- Local working tree at `/tmp/trino-s5-fixup` had both blocker fixes applied
  (uncommitted) — discarded; the fork branch tip is unchanged.
- 4-hour wall-clock budget used: ~30 min.

## Refs

- PR #117 (merged 2026-05-03 09:35 UTC, commit `864a1e7`) — original
  diagnostic, fix-up dispatch source.
- `agents/out/SHELF-S5/upstream-attempt-2026-05-03.md` — PR #117 result doc.
- `agents/out/SHELF-S5/activation-prep.md` — post-merge SHELF-S5 activation
  patch (still ~25 LOC, still blocked on upstream merge).
- Trino fork: `https://github.com/aamir306/trino` branch
  `shelf/fs-spi-hook` @ `9d68b98e1dde8ca8aa87e913a511c7df7f3f8b34`.
- Maven reactor cycle background: MNG-7136, MNG-3023.
