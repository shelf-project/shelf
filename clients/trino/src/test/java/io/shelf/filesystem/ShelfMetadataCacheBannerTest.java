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

import io.shelf.client.CircuitBreaker;
import io.shelf.client.MembershipResolver;
import io.shelf.client.RangeFetcher;
import io.shelf.config.ShelfConfig;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SHELF-35. Verifies the operator-facing startup banner that warns when
 * Trino's Iceberg connector is using its JVM-local metadata cache, which
 * shadows Shelf for warm metadata reads and skews hit-ratio counters.
 *
 * <p>The factory keys its "already warned" guard on the catalog name
 * and the guard is JVM-wide. Each test method here uses unique catalog
 * names and explicitly resets the guard in {@link #resetGuard()} so the
 * tests do not interfere with each other regardless of execution order.
 */
class ShelfMetadataCacheBannerTest
{
    private static final String LOGGER_NAME = ShelfFileSystemFactory.class.getName();

    private CapturingHandler handler;
    private Logger logger;
    private Level previousLevel;
    private boolean previousUseParent;

    @BeforeEach
    void installHandler()
    {
        ShelfFileSystemFactory.resetWarningGuardForTesting();
        logger = Logger.getLogger(LOGGER_NAME);
        previousLevel = logger.getLevel();
        previousUseParent = logger.getUseParentHandlers();
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
        handler = new CapturingHandler();
        logger.addHandler(handler);
    }

    @AfterEach
    void resetGuard()
    {
        if (logger != null) {
            logger.removeHandler(handler);
            logger.setLevel(previousLevel);
            logger.setUseParentHandlers(previousUseParent);
        }
        ShelfFileSystemFactory.resetWarningGuardForTesting();
    }

    @Test
    void bannerLogsWhenPropertyUnset()
    {
        String catalog = "iceberg-prod-unset";

        newFactory(catalog, Map.of());

        List<LogRecord> banners = bannersFor(catalog);
        assertThat(banners)
                .as("missing property defaults to enabled — banner must fire once")
                .hasSize(1);
        LogRecord record = banners.get(0);
        assertThat(record.getLevel()).isEqualTo(Level.INFO);
        assertThat(record.getMessage())
                .contains("[Shelf] Trino's iceberg.metadata-cache is ENABLED for catalog '" + catalog + "'.")
                .contains("iceberg.metadata-cache.enabled=false")
                .contains(catalog + ".properties")
                .contains(ShelfFileSystemFactory.METADATA_CACHE_DOC_URL);
    }

    @Test
    void bannerSilentWhenPropertyDisabled()
    {
        String catalog = "iceberg-prod-disabled";

        newFactory(catalog, Map.of(ShelfFileSystemFactory.ICEBERG_METADATA_CACHE_KEY, "false"));

        assertThat(bannersFor(catalog))
                .as("explicit opt-out must suppress the banner entirely")
                .isEmpty();
    }

    @Test
    void bannerLogsOncePerCatalog()
    {
        String first = "iceberg-cat-A";
        String second = "iceberg-cat-B";
        Map<String, String> enabled = Map.of(ShelfFileSystemFactory.ICEBERG_METADATA_CACHE_KEY, "true");

        newFactory(first, enabled);
        newFactory(second, enabled);

        assertThat(bannersFor(first)).hasSize(1);
        assertThat(bannersFor(second)).hasSize(1);
        assertThat(allBanners()).hasSize(2);
    }

    @Test
    void bannerDeduplicatesAcrossFactoryReinstantiation()
    {
        String catalog = "iceberg-reload";
        Map<String, String> props = new HashMap<>();
        // property absent — same as Trino default; both constructions must collapse to one banner.

        newFactory(catalog, props);
        newFactory(catalog, props);

        assertThat(bannersFor(catalog))
                .as("the JVM-wide guard must collapse repeat factory builds for the same catalog")
                .hasSize(1);
    }

    private ShelfFileSystemFactory newFactory(String catalog, Map<String, String> catalogProperties)
    {
        return new ShelfFileSystemFactory(
                ShelfConfig.fromMap(Map.of(
                        ShelfConfig.KEY_ENABLED, "false",
                        ShelfConfig.KEY_ENDPOINT, "shelf.shelf.svc.cluster.local:9090")),
                new NoopDelegateFactory(),
                (ep, pool, k, off, len) -> new byte[0],
                MembershipResolver.fixed(
                        "shelf-0",
                        URI.create("http://shelf-0.shelf.svc.cluster.local:9090"),
                        new CircuitBreaker("shelf-0")),
                catalog,
                catalogProperties);
    }

    private List<LogRecord> bannersFor(String catalog)
    {
        String marker = "catalog '" + catalog + "'";
        List<LogRecord> out = new ArrayList<>();
        for (LogRecord r : handler.records) {
            String msg = r.getMessage();
            if (msg != null && msg.contains(marker) && msg.contains("iceberg.metadata-cache is ENABLED")) {
                out.add(r);
            }
        }
        return out;
    }

    private List<LogRecord> allBanners()
    {
        List<LogRecord> out = new ArrayList<>();
        for (LogRecord r : handler.records) {
            String msg = r.getMessage();
            if (msg != null && msg.contains("iceberg.metadata-cache is ENABLED")) {
                out.add(r);
            }
        }
        return out;
    }

    private static final class CapturingHandler
            extends Handler
    {
        final List<LogRecord> records = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record)
        {
            records.add(record);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    /** Minimal {@link TrinoFileSystemFactory} that returns an unusable filesystem; the constructor is what we test. */
    private static final class NoopDelegateFactory
            implements TrinoFileSystemFactory
    {
        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            return new NoopFileSystem();
        }
    }

    private static final class NoopFileSystem
            implements TrinoFileSystem
    {
        @Override public TrinoInputFile newInputFile(Location location) { throw new UnsupportedOperationException(); }
        @Override public TrinoInputFile newInputFile(Location location, long length) { throw new UnsupportedOperationException(); }
        @Override public TrinoInputFile newInputFile(Location location, long length, Instant lastModified) { throw new UnsupportedOperationException(); }
        @Override public TrinoOutputFile newOutputFile(Location location) { throw new UnsupportedOperationException(); }
        @Override public void deleteFile(Location location) {}
        @Override public void deleteFiles(Collection<Location> locations) {}
        @Override public void deleteDirectory(Location location) {}
        @Override public void renameFile(Location source, Location target) {}
        @Override public FileIterator listFiles(Location location) { return FileIterator.empty(); }
        @Override public Optional<Boolean> directoryExists(Location location) { return Optional.empty(); }
        @Override public void createDirectory(Location location) {}
        @Override public void renameDirectory(Location source, Location target) {}
        @Override public Set<Location> listDirectories(Location location) { return Set.of(); }
        @Override public Optional<Location> createTemporaryDirectory(Location target, String prefix, String relPrefix) { return Optional.empty(); }
    }
}
