/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.shelf.filesystem;

import io.shelf.client.FooterPrefetcher;
import io.shelf.client.Key;
import io.shelf.client.MembershipResolver;
import io.shelf.client.Pool;
import io.shelf.client.PrefetchMetrics;
import io.shelf.client.RangeFetcher;
import io.shelf.config.ShelfConfig;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shelf implementation of {@link TrinoFileSystem} that wraps a delegate
 * {@link TrinoFileSystem} (e.g. Trino's native S3 implementation).
 *
 * <p><b>Fail-open invariant (BLUEPRINT §9.5).</b> Trino must <em>never</em>
 * observe a Shelf-specific error. Every Shelf-originated failure —
 * connection closed, 503, 504, TimeoutException — is caught inside
 * {@link ShelfInputStream} and transparently degraded to a direct read
 * against the delegate. The only errors that surface here are legitimate
 * S3 errors (AccessDenied, NoSuchKey, real network partitions), which Trino
 * already handles.
 *
 * <p><b>Delegation scope.</b> All write operations, all listing operations,
 * and all metadata calls (exists, length, lastModified) go straight to the
 * delegate. Only the read path through {@link TrinoInputFile#newStream()}
 * is intercepted.
 *
 * <p><b>Pool selection.</b> {@link ShelfFileSystem#newInputFile(Location)}
 * looks at the filename suffix to pick between the metadata pool and the
 * rowgroup pool. Iceberg {@code .json} / {@code .avro} and Parquet
 * {@code .parquet} footers go to metadata; everything else goes to rowgroup.
 * This is a coarse heuristic per BLUEPRINT §6.1; a connector-aware strategy
 * lands in SHELF-17.
 *
 * <p><b>Concurrency.</b> Instances are obtained per Trino session via
 * {@link ShelfFileSystemFactory}. Methods are called from worker threads
 * under Trino's usual per-split concurrency model; implementations must be
 * lock-light on the hot path.
 */
public final class ShelfFileSystem
        implements TrinoFileSystem
{
    private static final Logger log = Logger.getLogger(ShelfFileSystem.class.getName());
    private static final PrefetchMetrics EMPTY_METRICS = new PrefetchMetrics();

    private final ShelfConfig config;
    private final TrinoFileSystem delegate;
    private final RangeFetcher fetcher;
    private final MembershipResolver resolver;
    /** Nullable — when absent, footer prefetch is a no-op regardless of config. */
    private final FooterPrefetcher footerPrefetcher;

    public ShelfFileSystem(
            ShelfConfig config,
            TrinoFileSystem delegate,
            RangeFetcher fetcher,
            MembershipResolver resolver)
    {
        this(config, delegate, fetcher, resolver, null);
    }

    public ShelfFileSystem(
            ShelfConfig config,
            TrinoFileSystem delegate,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            FooterPrefetcher footerPrefetcher)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.footerPrefetcher = footerPrefetcher;
    }

    public ShelfConfig config()
    {
        return config;
    }

    MembershipResolver resolver()
    {
        return resolver;
    }

    /**
     * @return the metrics sink for the installed {@link FooterPrefetcher},
     *         or an empty sentinel if no prefetcher is wired. Never null —
     *         callers can unconditionally read counters and observe zero
     *         when prefetch is disabled.
     */
    public PrefetchMetrics prefetchMetrics()
    {
        return footerPrefetcher != null ? footerPrefetcher.metrics() : EMPTY_METRICS;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        TrinoInputFile inner = delegate.newInputFile(location);
        maybePrefetchFooter(inner, location);
        return wrapInputFile(inner, location);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return wrapInputFile(delegate.newInputFile(location, length), location);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return wrapInputFile(delegate.newInputFile(location, length, lastModified), location);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return delegate.newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        delegate.deleteFile(location);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        delegate.deleteFiles(locations);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        delegate.deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        delegate.renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return delegate.listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return delegate.directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        delegate.createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        delegate.renameDirectory(source, target);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return delegate.listDirectories(location);
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
    }

    private TrinoInputFile wrapInputFile(TrinoInputFile inner, Location location)
    {
        if (!config.isEnabled()) {
            return inner;
        }
        return new ShelfInputFile(inner, fetcher, resolver, poolFor(location));
    }

    // Track D1 — pool-routing extension. Iceberg produces a handful of
    // metadata formats that the original BLUEPRINT §6.1 heuristic
    // (".json" / ".avro") misses:
    //
    //   *.stats.puffin / *.puffin          — Puffin statistics blobs
    //                                        (Iceberg v3+ CDC + NDV stats)
    //   *-pos-deletes-*.parquet,
    //   *-positions.parquet                — position-delete files
    //   *-equality-deletes-*.parquet,
    //   *-equality.parquet                 — equality-delete files
    //
    // All five are small (< 10 MB typical), queried on *every* read of
    // the table they describe, and benefit from the metadata pool's
    // FrozenHot DRAM behaviour. Leaving them on the rowgroup hybrid
    // pool is wasteful — they thrash against row-group bytes, get
    // evicted by S3-FIFO, and re-fetched from S3 on the next query.
    //
    // Parquet page-index + bloom-filter slices also belong here but
    // are byte-range extractions from inside a larger .parquet file;
    // routing for those is D3, driven by the plugin's prefetch
    // extractor rather than the filename heuristic.
    static Pool poolFor(Location location)
    {
        String path = location.path().toLowerCase(Locale.ROOT);

        // Iceberg + HMS metadata: manifest-list, manifests, snapshot,
        // partition statistics, historical metadata.json.
        if (path.endsWith(".json") || path.endsWith(".avro") || path.endsWith("metadata.json")) {
            return Pool.METADATA;
        }
        // Puffin statistics — Iceberg v2+ NDV + CDC metadata. See
        // https://iceberg.apache.org/puffin-spec/.
        if (path.endsWith(".puffin") || path.endsWith(".stats.puffin") || path.endsWith(".stats")) {
            return Pool.METADATA;
        }
        // Position-delete + equality-delete files. These are Parquet
        // by extension but semantically metadata: they're read on
        // every scan against the table and are small.
        if (path.endsWith("-pos-deletes.parquet")
                || path.endsWith("-positions.parquet")
                || path.endsWith("-equality-deletes.parquet")
                || path.endsWith("-equality.parquet")
                || path.contains("/deletes/")
                || path.contains("-deletes-")) {
            return Pool.METADATA;
        }
        return Pool.ROWGROUP;
    }

    /**
     * The Parquet <em>footer</em> (last {@code N} bytes of a
     * {@code .parquet} file) is metadata payload per BLUEPRINT §6.1 and
     * lives in the metadata pool — even though {@link #poolFor} routes
     * the body of the same file to {@link Pool#ROWGROUP}. Kept as an
     * explicit helper rather than a constant so the contract is easy
     * to grep for and, if we later add page-index prefetch
     * (out of scope for SHELF-15), there is a single call site to
     * update.
     */
    static Pool poolForFooter()
    {
        return Pool.METADATA;
    }

    /**
     * Best-effort trigger for SHELF-15 Parquet-footer prefetch. Fires
     * when:
     * <ul>
     *   <li>the plugin is enabled,</li>
     *   <li>prefetch is enabled in config,</li>
     *   <li>a prefetcher is wired in,</li>
     *   <li>the path ends with {@code .parquet} (case-insensitive), and</li>
     *   <li>the {@link MembershipResolver} has a target for the file's key.</li>
     * </ul>
     *
     * <p>Fail-open: any failure in lookup (including
     * {@link IOException} from the delegate's {@code length()} /
     * {@code lastModified()}) is swallowed at FINE level. Prefetch is
     * optional; if we cannot trigger it, the foreground read path does
     * exactly the work it would without Shelf prefetch.
     */
    private void maybePrefetchFooter(TrinoInputFile inner, Location location)
    {
        if (!config.isEnabled() || !config.isPrefetchEnabled() || footerPrefetcher == null) {
            return;
        }
        String path = location.path();
        if (!endsWithIgnoreCase(path, ".parquet")) {
            return;
        }
        try {
            long length = inner.length();
            if (length <= 0L) {
                return;
            }
            byte[] etag = ShelfInputFile.deriveEtagBytes(length, inner.lastModified());
            // Route on a stable file-level identity — see ShelfInputFile
            // for the rationale (per-range routing would fragment the
            // working set across pods).
            Key routingKey = Key.fromTuple(etag, 0L, length, 0);
            Optional<MembershipResolver.Target> target = resolver.ownerFor(routingKey.asBytes());
            if (target.isEmpty()) {
                return;
            }
            int prefetchBytes = config.getFooterPrefetchKib() * 1024;
            // SHELF-16: prefetch must land bytes under the exact key
            // the foreground footer read will query, so we derive the
            // key from the actual footer byte range
            // [length - window, length). The window-clamp mirrors the
            // prefetcher's internal math (see FooterPrefetcher.prefetch).
            long window = Math.min((long) prefetchBytes, length);
            long footerOffset = length - window;
            Key footerKey = Key.fromTuple(etag, footerOffset, window, 0);
            footerPrefetcher.prefetch(target.get(), footerKey.toHex(), length, prefetchBytes);
        }
        catch (Throwable t) {
            // BLUEPRINT §9.5: prefetch never surfaces to Trino. Swallow
            // IOException from the delegate, RuntimeException from any
            // bug in the prefetcher, anything.
            log.log(Level.FINE, t, () -> "footer prefetch trigger failed for " + location);
        }
    }

    private static boolean endsWithIgnoreCase(String path, String suffix)
    {
        int pl = path.length();
        int sl = suffix.length();
        if (pl < sl) {
            return false;
        }
        return path.regionMatches(true, pl - sl, suffix, 0, sl);
    }
}
