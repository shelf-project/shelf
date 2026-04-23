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

import io.shelf.config.ShelfConfig;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Shelf implementation of {@link TrinoFileSystem}.
 *
 * <p><b>Fail-open invariant (BLUEPRINT §9.5).</b> Trino must <em>never</em>
 * observe a Shelf-specific error. Every Shelf-originated failure —
 * connection closed, 503, 504, TimeoutException — is caught inside this
 * class (or its delegates) and transparently degraded to a direct-S3 read.
 * The only errors that surface are legitimate S3 errors (AccessDenied,
 * NoSuchKey, real network partitions to S3), which Trino already handles.
 *
 * <p><b>Not-yet-implemented.</b> This is the Phase 0 scaffold. Bodies are
 * {@code UnsupportedOperationException} stubs. Every method is tagged with
 * the ticket that delivers its real implementation.
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

    public ShelfFileSystem(ShelfConfig config)
    {
        this.config = Objects.requireNonNull(config, "config");
    }

    public ShelfConfig config()
    {
        return config;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        // TODO(SHELF-10): wrap ShelfInputFile; delegate to S3 for non-Shelf prefixes.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.newInputFile not wired");
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        // TODO(SHELF-10): length-hint path; plumb into ShelfInputFile.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.newInputFile(length) not wired");
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        // TODO(SHELF-10): length+lastModified-hint path.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.newInputFile(length,lastModified) not wired");
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        // TODO(SHELF-10): writes bypass Shelf entirely; straight delegation to S3.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.newOutputFile not wired");
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        // TODO(SHELF-10): delegate to S3 via the underlying TrinoFileSystem.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.deleteFile not wired");
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        // TODO(SHELF-10): batch delegation to the underlying S3 filesystem.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.deleteFiles not wired");
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        // TODO(SHELF-10): delegate to S3.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.deleteDirectory not wired");
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        // TODO(SHELF-10): S3 delegation (unsupported on blob stores).
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.renameFile not wired");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        // TODO(SHELF-10): delegate to S3 (listing is not a cacheable workload in v1).
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.listFiles not wired");
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        // TODO(SHELF-10): delegate to S3.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.directoryExists not wired");
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        // TODO(SHELF-10): delegate to S3 (no-op on blob stores).
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.createDirectory not wired");
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        // TODO(SHELF-10): S3 delegation.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.renameDirectory not wired");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        // TODO(SHELF-10): S3 delegation.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.listDirectories not wired");
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        // TODO(SHELF-10): S3 delegation.
        throw new UnsupportedOperationException("SHELF-10: ShelfFileSystem.createTemporaryDirectory not wired");
    }
}
