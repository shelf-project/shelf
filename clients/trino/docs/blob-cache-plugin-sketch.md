# `plugin/trino-blob-cache-shelf/` — sketch

Java code-block sketch of the Trino plugin module Shelf will land upstream once [trinodb/trino#29184](https://github.com/trinodb/trino/pull/29184) merges. **Do not compile** — the SPI types referenced (`BlobCache`, `BlobCacheManager`, `BlobCacheManagerFactory`, `BlobSource`, `Blob`, `CacheKey`, `CacheTier`) don't exist in the public Trino SPI yet. This sketch exists so:

1. We can react in hours (not weeks) when #29184 lands — the structure is already designed.
2. We have a concrete artefact to point `@wendigo` and reviewers at when discussing the SPI shape ("here's what a real consumer's wiring looks like").
3. The wiring choices (per-pool factory registration, ETag-conditional `BlobSource`, fail-open on shelfd unreachable) are documented before they're implemented, so the implementer doesn't accidentally regress them.

The intended landing path: when #29184 merges, copy this sketch into `clients/trino-blob-cache/` as a real Maven module, drop the `// TODO(post-#29184)` placeholders for live SPI imports, run `mvn package`, and submit as a PR adding `plugin/trino-blob-cache-shelf/` upstream.

## Module layout (planned)

```
clients/trino-blob-cache/
├── pom.xml
├── README.md
└── src/
    ├── main/
    │   ├── java/io/shelf/trino/blob/
    │   │   ├── ShelfBlobCachePlugin.java
    │   │   ├── ShelfBlobCacheManagerFactory.java
    │   │   ├── ShelfBlobCacheManager.java
    │   │   ├── ShelfBlobCache.java
    │   │   ├── ShelfBlobSource.java
    │   │   ├── ShelfHttpClient.java         # internal: HTTP/2 client to shelfd
    │   │   ├── ShelfCacheKeyEncoder.java    # internal: CacheKey ↔ Shelf's content-addressed digest
    │   │   └── ShelfBlobCacheConfig.java    # config-shaped for cache-manager.config-files
    │   └── resources/META-INF/services/
    │       └── io.trino.spi.Plugin           # SPI service-loader entry
    └── test/
        └── java/io/shelf/trino/blob/
            ├── TestShelfBlobCachePlugin.java
            ├── TestShelfBlobCache.java
            └── TestShelfHttpClient.java     # against MinIO + shelfd via testcontainers
```

## `pom.xml` skeleton

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>io.trino</groupId>
        <artifactId>trino-root</artifactId>
        <version>1.0.0-SNAPSHOT</version>  <!-- TODO(post-#29184): match the merged Trino version -->
        <relativePath>../../pom.xml</relativePath>
    </parent>

    <artifactId>trino-blob-cache-shelf</artifactId>
    <packaging>trino-plugin</packaging>
    <description>Trino blob-cache plugin backed by shelfd (shelf-project)</description>

    <properties>
        <air.main.basedir>${project.parent.basedir}</air.main.basedir>
    </properties>

    <dependencies>
        <!-- The SPI (compile + provided — Trino runtime supplies it) -->
        <dependency>
            <groupId>io.trino</groupId>
            <artifactId>trino-spi</artifactId>
            <scope>provided</scope>
        </dependency>

        <!-- Bootstrap / Guice / Configuration via airlift, like every other plugin -->
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>configuration</artifactId>
        </dependency>
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>bootstrap</artifactId>
        </dependency>

        <!-- HTTP/2 client for shelfd. The S3 shim Shelf already speaks
             is sufficient; we use airlift's http-client for parity with
             other Trino modules. -->
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>http-client</artifactId>
        </dependency>

        <!-- Test-only -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

## `ShelfBlobCachePlugin.java`

```java
package io.shelf.trino.blob;

// TODO(post-#29184): import io.trino.spi.cache.BlobCacheManagerFactory once SPI lands
// TODO(post-#29184): import io.trino.spi.Plugin once getBlobCacheManagerFactories() ships

public final class ShelfBlobCachePlugin /* extends Plugin */ {
    /**
     * Register two factories — one per Shelf pool — so operators can
     * route Iceberg-metadata reads and rowgroup reads to different
     * cache instances if they want, or share one if they don't.
     *
     * Names are stable across versions:
     *   "shelf-metadata"  → DRAM-only metadata pool
     *   "shelf-rowgroup"  → DRAM + NVMe rowgroup pool
     *
     * Operators select via cache-manager.config-files.
     */
    // @Override
    public Iterable<Object /* BlobCacheManagerFactory */> getBlobCacheManagerFactories() {
        return java.util.List.of(
            new ShelfBlobCacheManagerFactory(ShelfPool.METADATA),
            new ShelfBlobCacheManagerFactory(ShelfPool.ROWGROUP)
        );
    }

    enum ShelfPool {
        METADATA,
        ROWGROUP;
    }
}
```

## `ShelfBlobCacheManagerFactory.java`

```java
package io.shelf.trino.blob;

// TODO(post-#29184): import io.trino.spi.cache.{BlobCacheManager, BlobCacheManagerFactory, CacheTier};

public final class ShelfBlobCacheManagerFactory /* implements BlobCacheManagerFactory */ {
    private final ShelfBlobCachePlugin.ShelfPool pool;

    ShelfBlobCacheManagerFactory(ShelfBlobCachePlugin.ShelfPool pool) {
        this.pool = pool;
    }

    // @Override
    public String name() {
        return switch (pool) {
            case METADATA -> "shelf-metadata";
            case ROWGROUP -> "shelf-rowgroup";
        };
    }

    /**
     * Per Shelf's feedback on #29184 (issue #2 in
     * docs/discovery/upstream/29184-spi-feedback.md), the SPI should
     * accept a free-form CacheTier alongside the MEMORY/DISK enum hint.
     * If the SPI only ships MEMORY/DISK, we collapse:
     *   metadata → MEMORY (DRAM-only)
     *   rowgroup → DISK    (DRAM + NVMe; the NVMe is the dominant tier)
     *
     * If the SPI grows CacheTier.named("metadata"), update this to
     * preserve the operator-facing pool names.
     */
    // @Override
    public Object /* CacheTier */ cacheTier() {
        return switch (pool) {
            case METADATA -> /* CacheTier.MEMORY */ null;
            case ROWGROUP -> /* CacheTier.DISK   */ null;
        };
    }

    // @Override
    public Object /* BlobCacheManager */ create(java.util.Map<String, String> config) {
        ShelfBlobCacheConfig cfg = ShelfBlobCacheConfig.fromProperties(config);
        return new ShelfBlobCacheManager(pool, cfg);
    }
}
```

## `ShelfBlobCacheManager.java`

```java
package io.shelf.trino.blob;

// TODO(post-#29184): import io.trino.spi.cache.{BlobCache, BlobCacheManager, CacheTier};

public final class ShelfBlobCacheManager /* implements BlobCacheManager */ {
    private final ShelfBlobCachePlugin.ShelfPool pool;
    private final ShelfBlobCacheConfig config;
    private final ShelfHttpClient http;

    ShelfBlobCacheManager(ShelfBlobCachePlugin.ShelfPool pool, ShelfBlobCacheConfig config) {
        this.pool = pool;
        this.config = config;
        this.http = new ShelfHttpClient(config.endpoint(), config.timeouts());
    }

    /**
     * The cache instance is shared across the Trino process — one per
     * (pool, manager) pair. Connection pooling, circuit breaker, and
     * metrics live inside the http client.
     */
    // @Override
    public Object /* BlobCache */ blobCache() {
        return new ShelfBlobCache(pool, http, config);
    }

    // @Override
    public void shutdown() {
        http.close();
    }
}
```

## `ShelfBlobCache.java`

```java
package io.shelf.trino.blob;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

// TODO(post-#29184): import io.trino.spi.cache.{BlobCache, BlobSource, CacheKey};

public final class ShelfBlobCache /* implements BlobCache */ {
    private final ShelfBlobCachePlugin.ShelfPool pool;
    private final ShelfHttpClient http;
    private final ShelfBlobCacheConfig config;

    ShelfBlobCache(ShelfBlobCachePlugin.ShelfPool pool, ShelfHttpClient http, ShelfBlobCacheConfig config) {
        this.pool = pool;
        this.http = http;
        this.config = config;
    }

    /**
     * Fail-open contract: if shelfd is unreachable, the circuit
     * breaker in ShelfHttpClient opens and we delegate every request
     * to {@code source}. Trino sees no error, just slightly higher
     * latency until shelfd recovers.
     *
     * Maps the engine's CacheKey to Shelf's content-addressed digest
     * via ShelfCacheKeyEncoder; see issue #1 in the SPI feedback doc.
     */
    // @Override
    public Object /* BlobSource */ get(Object /* CacheKey */ key, Object /* BlobSource */ source) {
        if (http.isCircuitOpen()) {
            return source;
        }
        byte[] digest = ShelfCacheKeyEncoder.encode(key);
        return new ShelfBlobSource(http, pool, digest, source);
    }

    /**
     * Per issue #3 in the SPI feedback doc: Shelf's keys are
     * content-addressed, so invalidation is structurally unnecessary.
     * Bump an observability counter; otherwise no-op.
     */
    // @Override
    public void invalidate(Object /* CacheKey */ key) {
        ShelfMetrics.invalidateNoOp(pool);
    }

    // @Override
    public void invalidate(Collection<Object /* CacheKey */> keys) {
        ShelfMetrics.invalidateNoOp(pool, keys.size());
    }

    /**
     * The metadata-only path proposed in issue #4. If the merged SPI
     * ships {@code length(CacheKey, InputFile)}, this implementation
     * goes through the metadata pool's HEAD path, never materialising
     * the rowgroup body.
     */
    // @Override (post-merge)
    public long length(Object /* CacheKey */ key, Object /* InputFile */ delegate) throws IOException {
        if (http.isCircuitOpen()) {
            return ((InputFileLike) delegate).length();
        }
        byte[] digest = ShelfCacheKeyEncoder.encode(key);
        return http.head(pool, digest).length();
    }

    /** Stand-in for the engine's InputFile until the SPI types are imported. */
    interface InputFileLike { long length() throws IOException; }
}
```

## `ShelfBlobSource.java`

```java
package io.shelf.trino.blob;

import java.io.IOException;
import java.io.InputStream;

// TODO(post-#29184): implement io.trino.spi.cache.BlobSource

public final class ShelfBlobSource {
    private final ShelfHttpClient http;
    private final ShelfBlobCachePlugin.ShelfPool pool;
    private final byte[] digest;
    private final Object /* BlobSource */ origin;

    ShelfBlobSource(ShelfHttpClient http, ShelfBlobCachePlugin.ShelfPool pool, byte[] digest, Object origin) {
        this.http = http;
        this.pool = pool;
        this.digest = digest;
        this.origin = origin;
    }

    /**
     * Open a stream of the cached blob. On miss, shelfd populates the
     * cache transparently (it's a read-through cache); on success
     * subsequent calls hit the cache.
     */
    public InputStream stream() throws IOException {
        try {
            return http.get(pool, digest);
        }
        catch (IOException e) {
            // Fail-open: degrade to direct origin read.
            ShelfMetrics.fallthrough(pool);
            return ((BlobSourceLike) origin).stream();
        }
    }

    public long length() throws IOException {
        try {
            return http.head(pool, digest).length();
        }
        catch (IOException e) {
            ShelfMetrics.fallthrough(pool);
            return ((BlobSourceLike) origin).length();
        }
    }

    /** Stand-in for the engine's BlobSource until the SPI types are imported. */
    interface BlobSourceLike {
        InputStream stream() throws IOException;
        long length() throws IOException;
    }
}
```

## `META-INF/services/io.trino.spi.Plugin`

```
io.shelf.trino.blob.ShelfBlobCachePlugin
```

## Operator-facing config (cache-manager.config-files)

The operator drops two property files into the Trino coordinator/worker config dir, both named in `cache-manager.config-files`:

```properties
# /etc/trino/cache-manager/shelf-metadata.properties
cache-manager.name=shelf-metadata
shelf.endpoint=http://shelf-pool.shelf.svc.cluster.local:9090
shelf.connect-timeout=2s
shelf.request-timeout=10s
shelf.circuit-breaker.failure-rate-threshold=50
shelf.circuit-breaker.cool-down=30s
```

```properties
# /etc/trino/cache-manager/shelf-rowgroup.properties
cache-manager.name=shelf-rowgroup
shelf.endpoint=http://shelf-pool.shelf.svc.cluster.local:9090
shelf.connect-timeout=2s
shelf.request-timeout=30s        # rowgroup blobs are bigger
shelf.circuit-breaker.failure-rate-threshold=50
shelf.circuit-breaker.cool-down=30s
```

## Tests

Three layers, in order of cost:

1. **Unit** — `TestShelfBlobCachePlugin` asserts the plugin registers exactly two factories (`shelf-metadata`, `shelf-rowgroup`) with the right tier. `TestShelfBlobCache` asserts `invalidate` is a no-op + counter bump, `get` returns a `BlobSource` whose `stream` calls `ShelfHttpClient` correctly (mocked).
2. **Integration via testcontainers** — `TestShelfHttpClient` spins up `shelf-project/shelfd:1.0.0` (container) + MinIO, runs a real GET / HEAD round-trip, asserts the bytes match what MinIO holds. Same shape as Alluxio's `lib/trino-filesystem-cache-alluxio/` test pattern.
3. **End-to-end via Trino product test** (post-merge) — wire the plugin into a Trino product test against the smoke harness. Asserts a TPC-H query Q3 hits the `shelf-rowgroup` cache for footer + row-group reads.

## Migration from the S3-shim path

Operators currently using Shelf via the S3-endpoint-swap path (`s3.endpoint=http://shelf-pool.shelf.svc.cluster.local:9092`) can run **both paths simultaneously** during migration:

| Catalog | Path | Use during migration |
|---|---|---|
| `iceberg_via_shim` | `s3.endpoint=http://shelf-pool:9092` (current) | Production traffic |
| `iceberg_via_blob_cache` | New plugin via `cache-manager.config-files` | Canary |

Once the blob-cache path proves equivalent (24 h byte-identity diff harness on canonical queries), drop the S3 shim path and decommission port 9092 on the Shelf StatefulSet.

## Out of scope for the initial PR

- **Prefetch hooks** — Shelf's existing `EventListenerFactory` for `QueryCreatedEvent` is unchanged; it stays a separate plugin (already shipping). The blob-cache plugin only handles the synchronous read path.
- **Pin list** — operator-controlled pinning stays a `shelfctl pin` operation, not exposed via the SPI. No reason to mirror it in the Trino plugin.
- **Per-table metric labels** — proposed for a follow-on PR once we see what `BlobCache.get` callers actually pass through `CacheKey`.

## When to drop the `// TODO(post-#29184)` markers

When all of the following are true:

1. #29184 is merged on `trinodb/trino` `main`
2. A Trino release containing the merged SPI is tagged (probably `v500` or later)
3. The `trino-spi` artefact for that version is published to Maven Central
4. We bump our parent pom to that version

Then the `provided` dependency resolves, the imports compile, and the plugin is ready for a `mvn package` + upstream PR submission.

## See also

- [docs/discovery/trino-upstream-strategy.md](../../docs/discovery/trino-upstream-strategy.md) — overall engagement plan
- [docs/discovery/upstream/29184-spi-feedback.md](../../docs/discovery/upstream/29184-spi-feedback.md) — SPI feedback that shaped this sketch
- [agents/out/adr/0011-content-addressed-cache-keys.md](../../agents/out/adr/0011-content-addressed-cache-keys.md) — Shelf's cache key design
- [agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md](../../agents/out/adr/0012-trino-read-path-endpoint-swap-then-blob-cache-spi.md) — two-stage Trino integration plan
