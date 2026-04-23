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

import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

/**
 * Skeleton {@link TrinoInputStream}.
 *
 * <p><b>Fall-through-to-S3 invariant.</b> Every read path in this stream
 * has two legs:
 * <ol>
 *   <li>Ask Shelf via an HTTP/2 range-GET with a deadline (default 200 ms,
 *       see {@link io.shelf.client.ShelfHttpClient#DEFAULT_TIMEOUT}).</li>
 *   <li>On <em>any</em> Shelf-originated failure
 *       ({@code IOException}, {@code TimeoutException}, HTTP 503/504,
 *       connection closed, circuit-breaker open), silently re-issue the
 *       same byte range against the underlying S3 filesystem and return
 *       that result to Trino.</li>
 * </ol>
 *
 * <p>The stream must never propagate a Shelf-specific error to Trino.
 * Only legitimate S3 errors (AccessDenied, NoSuchKey, real network partitions
 * to S3) bubble up. This is the concrete read-path expression of the
 * fail-open invariant documented on {@link ShelfFileSystem}.
 */
public final class ShelfInputStream
        extends TrinoInputStream
{
    @Override
    public long getPosition()
            throws IOException
    {
        // TODO(SHELF-10): position tracking independent of range-GET boundaries,
        //   so seek() can batch into a single new range-GET on the next read.
        throw new UnsupportedOperationException("SHELF-10: ShelfInputStream.getPosition not wired");
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        // TODO(SHELF-10): seek may span Shelf-cached and uncached ranges; log
        //   coarse-grained position for the prefetch listener (SHELF-15 / SHELF-16).
        throw new UnsupportedOperationException("SHELF-10: ShelfInputStream.seek not wired");
    }

    @Override
    public int read()
            throws IOException
    {
        // TODO(SHELF-10): single-byte read implemented via a pooled buffer;
        //   every Shelf failure falls through to the underlying S3 stream.
        throw new UnsupportedOperationException("SHELF-10: ShelfInputStream.read not wired");
    }
}
