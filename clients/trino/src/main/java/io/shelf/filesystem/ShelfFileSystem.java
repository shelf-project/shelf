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

import io.shelf.client.MembershipResolver;
import io.shelf.client.Pool;
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
    private final ShelfConfig config;
    private final TrinoFileSystem delegate;
    private final RangeFetcher fetcher;
    private final MembershipResolver resolver;

    public ShelfFileSystem(
            ShelfConfig config,
            TrinoFileSystem delegate,
            RangeFetcher fetcher,
            MembershipResolver resolver)
    {
        this.config = Objects.requireNonNull(config, "config");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    public ShelfConfig config()
    {
        return config;
    }

    MembershipResolver resolver()
    {
        return resolver;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return wrapInputFile(delegate.newInputFile(location), location);
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

    static Pool poolFor(Location location)
    {
        String path = location.path().toLowerCase(Locale.ROOT);
        if (path.endsWith(".json") || path.endsWith(".avro") || path.endsWith("metadata.json")) {
            return Pool.METADATA;
        }
        return Pool.ROWGROUP;
    }
}
