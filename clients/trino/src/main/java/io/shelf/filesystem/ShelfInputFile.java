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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Skeleton {@link TrinoInputFile}. Inherits the fail-open invariant from
 * {@link ShelfFileSystem}: any Shelf failure during {@link #newInput()} or
 * {@link #newStream()} degrades to a direct-S3 read.
 */
public final class ShelfInputFile
        implements TrinoInputFile
{
    private final Location location;

    public ShelfInputFile(Location location)
    {
        this.location = Objects.requireNonNull(location, "location");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        // TODO(SHELF-10): returns a ShelfInput that issues range-GETs against
        //   Shelf with per-RPC deadlines, falling through to S3 on any failure.
        throw new UnsupportedOperationException("SHELF-10: ShelfInputFile.newInput not wired");
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        // TODO(SHELF-10): returns a ShelfInputStream; see class javadoc on
        //   the fall-through-to-S3 invariant.
        throw new UnsupportedOperationException("SHELF-10: ShelfInputFile.newStream not wired");
    }

    @Override
    public long length()
            throws IOException
    {
        // TODO(SHELF-10): HEAD via Shelf's /cache/<key> HEAD path (SHELF-07),
        //   falling through to the underlying S3 TrinoInputFile#length().
        throw new UnsupportedOperationException("SHELF-10: ShelfInputFile.length not wired");
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        // TODO(SHELF-10): Shelf does not track last-modified; delegate to S3.
        throw new UnsupportedOperationException("SHELF-10: ShelfInputFile.lastModified not wired");
    }

    @Override
    public boolean exists()
            throws IOException
    {
        // TODO(SHELF-10): existence check is authoritative at S3, not at Shelf.
        throw new UnsupportedOperationException("SHELF-10: ShelfInputFile.exists not wired");
    }

    @Override
    public Location location()
    {
        return location;
    }
}
