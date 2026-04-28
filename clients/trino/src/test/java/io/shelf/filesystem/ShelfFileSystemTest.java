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
import io.shelf.client.FooterPrefetcher;
import io.shelf.client.MembershipResolver;
import io.shelf.client.Pool;
import io.shelf.client.PrefetchMetrics;
import io.shelf.client.RangeFetcher;
import io.shelf.client.ShelfHttpClient.ShelfUnavailableException;
import io.shelf.config.ShelfConfig;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ShelfFileSystemTest
{
    private static final Location DATA = Location.of("s3://bucket/table/rg-0.parquet");
    private static final Location META = Location.of("s3://bucket/table/metadata/v1.metadata.json");

    @Test
    void poolForUsesMetadataPoolForJsonAndAvro()
    {
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/v1.metadata.json"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/snap-42.avro"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/rg-0.parquet"))).isEqualTo(Pool.ROWGROUP);
    }

    @Test
    void poolForRoutesIcebergMetadataSurfaceToMetadataPool()
    {
        // Track D1 — Puffin stats + position/equality deletes all
        // belong in the DRAM-only metadata pool (BLUEPRINT §6.1b).
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/t-uuid.stats.puffin"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/ndv.puffin"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/ndv.stats"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/00001-pos-deletes.parquet"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/00001-positions.parquet"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/00001-equality-deletes.parquet"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/00001-equality.parquet"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/deletes/foo.parquet"))).isEqualTo(Pool.METADATA);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/part-00001-deletes-x.parquet"))).isEqualTo(Pool.METADATA);

        // Regression guard: regular data parquet still rowgroup.
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/part-0.parquet"))).isEqualTo(Pool.ROWGROUP);
        assertThat(ShelfFileSystem.poolFor(Location.of("s3://b/a/data/00001.parquet"))).isEqualTo(Pool.ROWGROUP);
    }

    @Test
    void writeOperationsDelegateVerbatim()
            throws IOException
    {
        RecordingDelegate delegate = new RecordingDelegate();
        ShelfFileSystem fs = new ShelfFileSystem(
                enabledConfig(),
                delegate,
                alwaysSucceedFetcher(),
                fixedResolver());

        fs.deleteFile(DATA);
        fs.createDirectory(DATA);

        assertThat(delegate.deleted).containsExactly(DATA);
        assertThat(delegate.createdDirs).containsExactly(DATA);
    }

    @Test
    void disabledConfigReturnsDelegateInputFileUnmodified()
    {
        RecordingDelegate delegate = new RecordingDelegate();
        TrinoInputFile inner = new FakeInputFile(DATA, new byte[] {9});
        delegate.inputFiles.put(DATA, inner);

        ShelfFileSystem fs = new ShelfFileSystem(
                ShelfConfig.fromMap(Map.of()),      // disabled by default
                delegate,
                alwaysSucceedFetcher(),
                fixedResolver());

        assertThat(fs.newInputFile(DATA))
                .as("disabled Shelf must not wrap the delegate")
                .isSameAs(inner);
    }

    @Test
    void enabledConfigWrapsInputFileWithShelf()
            throws IOException
    {
        byte[] payload = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(DATA, new FakeInputFile(DATA, payload));

        AtomicInteger shelfCalls = new AtomicInteger();
        RangeFetcher fetcher = (ep, pool, k, off, len) -> {
            shelfCalls.incrementAndGet();
            return Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        };

        ShelfFileSystem fs = new ShelfFileSystem(
                enabledConfig(),
                delegate,
                fetcher,
                fixedResolver());

        TrinoInputFile wrapped = fs.newInputFile(DATA);
        try (TrinoInputStream in = wrapped.newStream()) {
            byte[] buf = new byte[payload.length];
            int n = in.read(buf, 0, payload.length);
            assertThat(n).isEqualTo(payload.length);
            assertThat(buf).isEqualTo(payload);
        }
        assertThat(shelfCalls.get()).isEqualTo(1);
    }

    @Test
    void failsOpenWhenShelfIsUnreachable()
            throws IOException
    {
        byte[] payload = new byte[]{10, 20, 30, 40};
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(META, new FakeInputFile(META, payload));

        RangeFetcher broken = (ep, pool, k, off, len) -> {
            throw new ShelfUnavailableException("unreachable");
        };

        ShelfFileSystem fs = new ShelfFileSystem(
                enabledConfig(),
                delegate,
                broken,
                fixedResolver());

        TrinoInputFile wrapped = fs.newInputFile(META);
        try (TrinoInputStream in = wrapped.newStream()) {
            byte[] buf = new byte[payload.length];
            int n = in.read(buf, 0, payload.length);
            assertThat(n).isEqualTo(payload.length);
            assertThat(buf)
                    .as("fail-open: Trino sees the delegate's bytes, not a Shelf error")
                    .isEqualTo(payload);
        }
    }

    @Test
    void listFilesAndDirectoryOpsDelegate()
            throws IOException
    {
        RecordingDelegate delegate = new RecordingDelegate();
        ShelfFileSystem fs = new ShelfFileSystem(
                enabledConfig(), delegate, alwaysSucceedFetcher(), fixedResolver());

        fs.directoryExists(DATA);
        fs.listDirectories(DATA);
        fs.createTemporaryDirectory(DATA, "pfx", "rel");

        assertThat(delegate.directoryExistsCalls).isEqualTo(1);
        assertThat(delegate.listDirectoriesCalls).isEqualTo(1);
        assertThat(delegate.createTempCalls).isEqualTo(1);
    }

    @Test
    void parquetPathTriggersFooterPrefetchWithMetadataPoolAndClampedRange()
    {
        int kib = 64;
        long fileLength = 10L * 1024 * 1024;
        byte[] payload = new byte[(int) fileLength];
        Location parquet = Location.of("s3://bucket/table/data/part-0.PARQUET");

        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(parquet, new FakeInputFile(parquet, payload));

        RecordingFetcher recording = new RecordingFetcher();
        PrefetchMetrics metrics = new PrefetchMetrics();
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(
                recording, new DirectExecutorService(), metrics);

        ShelfFileSystem fs = new ShelfFileSystem(
                prefetchEnabledConfig(kib),
                delegate,
                alwaysSucceedFetcher(),
                fixedResolver(),
                prefetcher);

        fs.newInputFile(parquet);

        assertThat(recording.calls.get())
                .as(".parquet (case-insensitive) must trigger one prefetch")
                .isEqualTo(1);
        assertThat(recording.lastPool.get())
                .as("footer prefetch routes to the metadata pool, not the rowgroup pool used by body reads")
                .isEqualTo(Pool.METADATA);
        assertThat(recording.lastOffset.get()).isEqualTo(fileLength - kib * 1024L);
        assertThat(recording.lastLength.get()).isEqualTo(kib * 1024L);
        assertThat(metrics.footerPrefetchScheduled()).isEqualTo(1);
        assertThat(metrics.footerPrefetchCompleted()).isEqualTo(1);
        assertThat(fs.prefetchMetrics()).isSameAs(metrics);
    }

    @Test
    void orcPathDoesNotTriggerFooterPrefetch()
    {
        Location orc = Location.of("s3://bucket/table/data/part-0.orc");
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(orc, new FakeInputFile(orc, new byte[256]));

        RecordingFetcher recording = new RecordingFetcher();
        PrefetchMetrics metrics = new PrefetchMetrics();
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(
                recording, new DirectExecutorService(), metrics);

        ShelfFileSystem fs = new ShelfFileSystem(
                prefetchEnabledConfig(64),
                delegate,
                alwaysSucceedFetcher(),
                fixedResolver(),
                prefetcher);

        fs.newInputFile(orc);

        assertThat(recording.calls.get()).isZero();
        assertThat(metrics.footerPrefetchScheduled()).isZero();
    }

    @Test
    void emptyRingSkipsPrefetchAndLeavesMetricsZero()
            throws IOException
    {
        byte[] payload = new byte[2048];
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(DATA, new FakeInputFile(DATA, payload));

        RecordingFetcher recording = new RecordingFetcher();
        PrefetchMetrics metrics = new PrefetchMetrics();
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(
                recording, new DirectExecutorService(), metrics);

        MembershipResolver empty = new MembershipResolver(
                () -> List.of(),
                java.net.http.HttpClient.newHttpClient(),
                java.time.Duration.ofSeconds(1),
                java.time.Duration.ofSeconds(1));

        ShelfFileSystem fs = new ShelfFileSystem(
                prefetchEnabledConfig(64),
                delegate,
                alwaysSucceedFetcher(),
                empty,
                prefetcher);

        fs.newInputFile(DATA);

        assertThat(recording.calls.get())
                .as("empty ring has no endpoint to route a prefetch to")
                .isZero();
        assertThat(metrics.footerPrefetchScheduled()).isZero();
        empty.close();
    }

    @Test
    void prefetchTriggerPreservesTrinoInputFileDelegation()
            throws IOException
    {
        Location parquet = Location.of("s3://bucket/table/data/part-0.parquet");
        byte[] payload = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(parquet, new FakeInputFile(parquet, payload));

        RecordingFetcher recording = new RecordingFetcher();
        PrefetchMetrics metrics = new PrefetchMetrics();
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(
                recording, new DirectExecutorService(), metrics);

        ShelfFileSystem fs = new ShelfFileSystem(
                prefetchEnabledConfig(64),
                delegate,
                (ep, pool, k, off, len) -> Arrays.copyOfRange(payload, (int) off, (int) (off + len)),
                fixedResolver(),
                prefetcher);

        TrinoInputFile wrapped = fs.newInputFile(parquet);

        // Even though prefetch just fired, the file's length / lastModified
        // / exists still delegate to the underlying file, and newStream()
        // still returns functional bytes.
        assertThat(wrapped.length()).isEqualTo(payload.length);
        assertThat(wrapped.lastModified()).isEqualTo(Instant.EPOCH);
        assertThat(wrapped.exists()).isTrue();
        try (TrinoInputStream in = wrapped.newStream()) {
            byte[] buf = new byte[payload.length];
            int n = in.read(buf, 0, payload.length);
            assertThat(n).isEqualTo(payload.length);
            assertThat(buf).isEqualTo(payload);
        }
    }

    @Test
    void prefetchDisabledInConfigSkipsPrefetcher()
    {
        Location parquet = Location.of("s3://bucket/table/data/part-0.parquet");
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.inputFiles.put(parquet, new FakeInputFile(parquet, new byte[8192]));

        RecordingFetcher recording = new RecordingFetcher();
        PrefetchMetrics metrics = new PrefetchMetrics();
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(
                recording, new DirectExecutorService(), metrics);

        // Plugin enabled, but prefetch disabled.
        ShelfConfig disabled = ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "true",
                ShelfConfig.KEY_ENDPOINT, "shelf.local:9090"));
        ShelfFileSystem fs = new ShelfFileSystem(
                disabled, delegate, alwaysSucceedFetcher(), fixedResolver(), prefetcher);

        fs.newInputFile(parquet);

        assertThat(recording.calls.get()).isZero();
        assertThat(metrics.footerPrefetchScheduled()).isZero();
    }

    private static ShelfConfig enabledConfig()
    {
        return ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "true",
                ShelfConfig.KEY_ENDPOINT, "shelf.local:9090"));
    }

    private static ShelfConfig prefetchEnabledConfig(int kib)
    {
        return ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "true",
                ShelfConfig.KEY_ENDPOINT, "shelf.local:9090",
                ShelfConfig.KEY_PREFETCH_ENABLED, "true",
                ShelfConfig.KEY_FOOTER_PREFETCH_KIB, Integer.toString(kib)));
    }

    private static RangeFetcher alwaysSucceedFetcher()
    {
        return (ep, pool, k, off, len) -> new byte[(int) len];
    }

    private static MembershipResolver fixedResolver()
    {
        return MembershipResolver.fixed(
                "shelf-0",
                URI.create("http://shelf.local:9090"),
                new CircuitBreaker("shelf-0"));
    }

    /** Hand-rolled TrinoFileSystem stub that records method calls. */
    private static final class RecordingDelegate
            implements TrinoFileSystem
    {
        final Map<Location, TrinoInputFile> inputFiles = new HashMap<>();
        final java.util.List<Location> deleted = new java.util.ArrayList<>();
        final java.util.List<Location> createdDirs = new java.util.ArrayList<>();
        int directoryExistsCalls;
        int listDirectoriesCalls;
        int createTempCalls;

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return inputFiles.getOrDefault(location, new FakeInputFile(location, new byte[0]));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return newInputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return newInputFile(location);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(Location location)
        {
            deleted.add(location);
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
        {
            deleted.addAll(locations);
        }

        @Override
        public void deleteDirectory(Location location) {}

        @Override
        public void renameFile(Location source, Location target) {}

        @Override
        public FileIterator listFiles(Location location)
        {
            return FileIterator.empty();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            directoryExistsCalls++;
            return Optional.empty();
        }

        @Override
        public void createDirectory(Location location)
        {
            createdDirs.add(location);
        }

        @Override
        public void renameDirectory(Location source, Location target) {}

        @Override
        public Set<Location> listDirectories(Location location)
        {
            listDirectoriesCalls++;
            return Set.of();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            createTempCalls++;
            return Optional.empty();
        }
    }

    /** In-memory TrinoInputFile backed by a byte array. */
    private static final class FakeInputFile
            implements TrinoInputFile
    {
        private final Location location;
        private final byte[] payload;

        FakeInputFile(Location location, byte[] payload)
        {
            this.location = location;
            this.payload = payload;
        }

        @Override
        public TrinoInput newInput()
        {
            throw new UnsupportedOperationException("not exercised in this test");
        }

        @Override
        public TrinoInputStream newStream()
        {
            return new InMemoryStream(payload);
        }

        @Override
        public long length()
        {
            return payload.length;
        }

        @Override
        public Instant lastModified()
        {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists()
        {
            return true;
        }

        @Override
        public Location location()
        {
            return location;
        }
    }

    /** Records a single rangeGet; fills the response with zero-bytes of the right length. */
    private static final class RecordingFetcher
            implements RangeFetcher
    {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<String> lastEndpoint = new AtomicReference<>();
        final AtomicReference<Pool> lastPool = new AtomicReference<>();
        final AtomicReference<String> lastContentKey = new AtomicReference<>();
        final AtomicLong lastOffset = new AtomicLong();
        final AtomicLong lastLength = new AtomicLong();

        @Override
        public byte[] rangeGet(String endpoint, Pool pool, String contentKey, long offset, long length)
        {
            calls.incrementAndGet();
            lastEndpoint.set(endpoint);
            lastPool.set(pool);
            lastContentKey.set(contentKey);
            lastOffset.set(offset);
            lastLength.set(length);
            return new byte[(int) length];
        }
    }

    /** Runs each submitted task on the calling thread; ensures the prefetch completes before assertions. */
    private static final class DirectExecutorService
            extends AbstractExecutorService
    {
        private volatile boolean shutdown;

        @Override
        public void execute(Runnable command)
        {
            command.run();
        }

        @Override
        public void shutdown()
        {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return shutdown;
        }

        @Override
        public boolean isTerminated()
        {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
        {
            return true;
        }
    }

    private static final class InMemoryStream
            extends TrinoInputStream
    {
        private final byte[] data;
        private long position;

        InMemoryStream(byte[] data)
        {
            this.data = data;
        }

        @Override
        public long getPosition()
        {
            return position;
        }

        @Override
        public void seek(long newPosition)
        {
            position = newPosition;
        }

        @Override
        public int read()
        {
            if (position >= data.length) {
                return -1;
            }
            return data[(int) position++] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len)
        {
            if (position >= data.length) {
                return -1;
            }
            int n = (int) Math.min(len, data.length - position);
            System.arraycopy(data, (int) position, b, off, n);
            position += n;
            return n;
        }
    }
}
