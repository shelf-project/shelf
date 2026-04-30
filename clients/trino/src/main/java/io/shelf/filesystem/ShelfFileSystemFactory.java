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
import io.shelf.client.MembershipResolver;
import io.shelf.client.PrefetchMetrics;
import io.shelf.client.RangeFetcher;
import io.shelf.config.ShelfConfig;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Builds per-identity {@link ShelfFileSystem} instances.
 *
 * <p>Trino instantiates this factory once per catalog and invokes
 * {@link #create(ConnectorIdentity)} per query. The factory owns the long-lived
 * resources (HTTP client pool, {@link MembershipResolver}); the per-identity
 * {@link ShelfFileSystem} references them and the resolver owns the
 * {@code Map<String, CircuitBreaker>} keyed by pod id.
 *
 * <p>Endpoint / breaker selection for a given read goes through
 * {@link MembershipResolver#ownerFor(byte[])} — see
 * {@link ShelfInputFile#newStream()} for the per-stream binding.
 */
public final class ShelfFileSystemFactory
        implements TrinoFileSystemFactory, AutoCloseable
{
    private static final Logger log = Logger.getLogger(ShelfFileSystemFactory.class.getName());

    /**
     * SHELF-35. Catalog property name owned by Trino's Iceberg connector.
     * When unset or {@code true} (the default), the connector keeps a
     * JVM-local metadata cache that shadows Shelf for warm reads, which
     * makes Shelf's hit-ratio counters under-report.
     */
    static final String ICEBERG_METADATA_CACHE_KEY = "iceberg.metadata-cache.enabled";

    /**
     * SHELF-35. Documentation anchor surfaced in the startup banner. Kept
     * as a constant so the test can pin the exact URL the operator sees.
     * Update in lockstep with the docs site.
     */
    static final String METADATA_CACHE_DOC_URL =
            "https://shelf-project.dev/docs/troubleshooting#iceberg-metadata-cache";

    /**
     * SHELF-35. JVM-wide guard: the banner is logged at most once per
     * (catalog-name) per JVM, even if a catalog is reloaded and a new
     * factory instance is constructed. Keys are catalog names; values
     * are sentinel {@code Boolean.TRUE} entries.
     */
    private static final ConcurrentHashMap<String, Boolean> WARNED_CATALOGS = new ConcurrentHashMap<>();

    private final ShelfConfig config;
    private final TrinoFileSystemFactory delegateFactory;
    private final RangeFetcher fetcher;
    private final MembershipResolver resolver;
    /** Non-null only when both {@code shelf.enabled} and {@code shelf.prefetch.enabled} are true. */
    private final FooterPrefetcher footerPrefetcher;

    public ShelfFileSystemFactory(
            ShelfConfig config,
            TrinoFileSystemFactory delegateFactory,
            RangeFetcher fetcher,
            MembershipResolver resolver)
    {
        this(config, delegateFactory, fetcher, resolver, buildPrefetcherIfEnabled(config, fetcher), null, Map.of());
    }

    /**
     * SHELF-35 entry point. Same as the four-arg constructor, plus the
     * raw catalog name and full catalog properties map so the factory
     * can warn the operator when {@code iceberg.metadata-cache.enabled}
     * is unset or {@code true}. The properties map is read once; nothing
     * else is retained from it.
     */
    public ShelfFileSystemFactory(
            ShelfConfig config,
            TrinoFileSystemFactory delegateFactory,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            String catalogName,
            Map<String, String> catalogProperties)
    {
        this(config, delegateFactory, fetcher, resolver,
                buildPrefetcherIfEnabled(config, fetcher),
                catalogName, catalogProperties);
    }

    /**
     * Test seam: caller-supplied {@link FooterPrefetcher}. The factory
     * owns the prefetcher's lifecycle <em>only</em> when it built it
     * itself (via the public constructor). Prefetchers handed in here
     * are the caller's responsibility.
     */
    ShelfFileSystemFactory(
            ShelfConfig config,
            TrinoFileSystemFactory delegateFactory,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            FooterPrefetcher footerPrefetcher)
    {
        this(config, delegateFactory, fetcher, resolver, footerPrefetcher, null, Map.of());
    }

    ShelfFileSystemFactory(
            ShelfConfig config,
            TrinoFileSystemFactory delegateFactory,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            FooterPrefetcher footerPrefetcher,
            String catalogName,
            Map<String, String> catalogProperties)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.delegateFactory = Objects.requireNonNull(delegateFactory, "delegateFactory");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.footerPrefetcher = footerPrefetcher;
        maybeWarnAboutIcebergMetadataCache(catalogName, catalogProperties);
    }

    private static FooterPrefetcher buildPrefetcherIfEnabled(ShelfConfig config, RangeFetcher fetcher)
    {
        if (config.isEnabled() && config.isPrefetchEnabled()) {
            return new FooterPrefetcher(fetcher, new PrefetchMetrics());
        }
        return null;
    }

    public MembershipResolver resolver()
    {
        return resolver;
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        Objects.requireNonNull(identity, "identity");
        TrinoFileSystem delegate = delegateFactory.create(identity);
        return new ShelfFileSystem(config, delegate, fetcher, resolver, footerPrefetcher);
    }

    @Override
    public void close()
    {
        if (footerPrefetcher != null) {
            footerPrefetcher.close();
        }
    }

    /**
     * SHELF-35. Emits the metadata-cache warning banner at most once per
     * catalog per JVM. No-op when the catalog name is null/blank — that
     * is the legacy four-arg test path which has no catalog identity to
     * key the warning on.
     *
     * <p>The warning fires when {@link #ICEBERG_METADATA_CACHE_KEY} is
     * absent (Trino's default is {@code true}) or set to anything that
     * does not parse as the literal {@code "false"} after lower-casing
     * and trimming. Anything else — including {@code "true"}, an empty
     * string, or whitespace — leaves the cache enabled and we warn.
     */
    private static void maybeWarnAboutIcebergMetadataCache(String catalogName, Map<String, String> catalogProperties)
    {
        if (catalogName == null || catalogName.isBlank()) {
            return;
        }
        Map<String, String> props = catalogProperties != null ? catalogProperties : Map.of();
        if (cacheExplicitlyDisabled(props)) {
            return;
        }
        // putIfAbsent returns null on first insert; non-null on subsequent calls.
        if (WARNED_CATALOGS.putIfAbsent(catalogName, Boolean.TRUE) != null) {
            return;
        }
        log.log(Level.INFO, buildBanner(catalogName));
    }

    private static boolean cacheExplicitlyDisabled(Map<String, String> props)
    {
        String raw = props.get(ICEBERG_METADATA_CACHE_KEY);
        if (raw == null) {
            return false;
        }
        return "false".equals(raw.trim().toLowerCase(Locale.ROOT));
    }

    static String buildBanner(String catalogName)
    {
        String rule = "=========================================================================================";
        return rule + System.lineSeparator()
                + "[Shelf] Trino's iceberg.metadata-cache is ENABLED for catalog '" + catalogName + "'." + System.lineSeparator()
                + "[Shelf] This JVM-local cache shadows Shelf for warm metadata reads \u2014 hit-ratio" + System.lineSeparator()
                + "[Shelf] counters will under-report and Shelf metrics will be misleading." + System.lineSeparator()
                + "[Shelf] Recommendation: set the following in " + catalogName + ".properties:" + System.lineSeparator()
                + "[Shelf]" + System.lineSeparator()
                + "[Shelf]     " + ICEBERG_METADATA_CACHE_KEY + "=false" + System.lineSeparator()
                + "[Shelf]" + System.lineSeparator()
                + "[Shelf] See: " + METADATA_CACHE_DOC_URL + System.lineSeparator()
                + rule;
    }

    /**
     * Test-only: clear the JVM-wide "already warned" set so a single
     * test class can exercise multiple catalog-name scenarios without
     * leaking state between methods. Package-private on purpose.
     */
    static void resetWarningGuardForTesting()
    {
        WARNED_CATALOGS.clear();
    }
}
