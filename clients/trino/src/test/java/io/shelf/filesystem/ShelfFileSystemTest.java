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
import io.shelf.client.Pool;
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
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
    void writeOperationsDelegateVerbatim()
            throws IOException
    {
        RecordingDelegate delegate = new RecordingDelegate();
        ShelfFileSystem fs = new ShelfFileSystem(
                enabledConfig(),
                delegate,
                alwaysSucceedFetcher(),
                new CircuitBreaker("shelf-0"));

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
                new CircuitBreaker("shelf-0"));

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
                new CircuitBreaker("shelf-0"));

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
                new CircuitBreaker("shelf-0"));

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
                enabledConfig(), delegate, alwaysSucceedFetcher(), new CircuitBreaker("shelf-0"));

        fs.directoryExists(DATA);
        fs.listDirectories(DATA);
        fs.createTemporaryDirectory(DATA, "pfx", "rel");

        assertThat(delegate.directoryExistsCalls).isEqualTo(1);
        assertThat(delegate.listDirectoriesCalls).isEqualTo(1);
        assertThat(delegate.createTempCalls).isEqualTo(1);
    }

    private static ShelfConfig enabledConfig()
    {
        return ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "true",
                ShelfConfig.KEY_ENDPOINT, "shelf.local:9090"));
    }

    private static RangeFetcher alwaysSucceedFetcher()
    {
        return (ep, pool, k, off, len) -> new byte[(int) len];
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
