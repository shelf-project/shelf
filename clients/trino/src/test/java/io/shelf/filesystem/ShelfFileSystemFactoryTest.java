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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link ShelfFileSystemFactory} routes Shelf reads
 * through the {@link MembershipResolver}-selected target for the
 * key, rather than a hard-coded endpoint + breaker pair.
 */
class ShelfFileSystemFactoryTest
{
    private static final Location DATA = Location.of("s3://bucket/t/rg-0.parquet");

    @Test
    void factoryPassesResolverSelectedTargetToInputStream()
            throws IOException
    {
        byte[] payload = new byte[]{1, 2, 3, 4};
        FakeInputFile inner = new FakeInputFile(DATA, payload);
        FakeDelegateFactory delegateFactory = new FakeDelegateFactory(inner);

        URI target = URI.create("http://shelf-7.shelf.svc.cluster.local:9090");
        MembershipResolver resolver = MembershipResolver.fixed(
                "shelf-7", target, new CircuitBreaker("shelf-7"));

        AtomicReference<String> seenEndpoint = new AtomicReference<>();
        RangeFetcher recordingFetcher = (ep, pool, k, off, len) -> {
            seenEndpoint.set(ep);
            return Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        };

        ShelfFileSystemFactory factory = new ShelfFileSystemFactory(
                enabledConfig(), delegateFactory, recordingFetcher, resolver);

        TrinoFileSystem fs = factory.create(ConnectorIdentity.ofUser("anonymous"));
        try (TrinoInputStream in = fs.newInputFile(DATA).newStream()) {
            byte[] buf = new byte[payload.length];
            int n = in.read(buf, 0, payload.length);
            assertThat(n).isEqualTo(payload.length);
            assertThat(buf).isEqualTo(payload);
        }

        assertThat(seenEndpoint.get())
                .as("fetcher must be called with the URI published by the resolver")
                .isEqualTo(target.toString());
    }

    @Test
    void emptyResolverFallsThroughToDelegateWithoutCallingShelf()
            throws IOException
    {
        byte[] payload = new byte[]{9, 9, 9, 9};
        FakeInputFile inner = new FakeInputFile(DATA, payload);
        FakeDelegateFactory delegateFactory = new FakeDelegateFactory(inner);

        // A resolver whose snapshot is empty — simulates "no pods reachable".
        MembershipResolver empty = new MembershipResolver(
                () -> java.util.List.of(),
                java.net.http.HttpClient.newHttpClient(),
                java.time.Duration.ofSeconds(1),
                java.time.Duration.ofSeconds(1));

        AtomicReference<Boolean> shelfCalled = new AtomicReference<>(false);
        RangeFetcher fetcher = (ep, pool, k, off, len) -> {
            shelfCalled.set(true);
            throw new AssertionError("should not be called when ring is empty");
        };

        ShelfFileSystemFactory factory = new ShelfFileSystemFactory(
                enabledConfig(), delegateFactory, fetcher, empty);

        TrinoFileSystem fs = factory.create(ConnectorIdentity.ofUser("anonymous"));
        try (TrinoInputStream in = fs.newInputFile(DATA).newStream()) {
            byte[] buf = new byte[payload.length];
            int n = in.read(buf, 0, payload.length);
            assertThat(n).isEqualTo(payload.length);
            assertThat(buf).isEqualTo(payload);
        }

        assertThat(shelfCalled.get())
                .as("empty ring must bypass Shelf entirely")
                .isFalse();
        empty.close();
    }

    private static ShelfConfig enabledConfig()
    {
        return ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "true",
                ShelfConfig.KEY_ENDPOINT, "shelf.shelf.svc.cluster.local:9090"));
    }

    private static final class FakeDelegateFactory
            implements TrinoFileSystemFactory
    {
        private final TrinoInputFile inputFile;

        FakeDelegateFactory(TrinoInputFile inputFile)
        {
            this.inputFile = inputFile;
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            return new FakeFileSystem(inputFile);
        }
    }

    private static final class FakeFileSystem
            implements TrinoFileSystem
    {
        private final TrinoInputFile inputFile;

        FakeFileSystem(TrinoInputFile inputFile)
        {
            this.inputFile = inputFile;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return inputFile;
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return inputFile;
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return inputFile;
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(Location location) {}

        @Override
        public void deleteFiles(Collection<Location> locations) {}

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
            return Optional.empty();
        }

        @Override
        public void createDirectory(Location location) {}

        @Override
        public void renameDirectory(Location source, Location target) {}

        @Override
        public Set<Location> listDirectories(Location location)
        {
            return Set.of();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location target, String prefix, String relPrefix)
        {
            return Optional.empty();
        }
    }

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
            throw new UnsupportedOperationException();
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
