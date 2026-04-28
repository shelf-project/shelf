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

import io.shelf.client.Key;
import io.shelf.client.MembershipResolver;
import io.shelf.client.Pool;
import io.shelf.client.RangeFetcher;
import io.shelf.client.RowGroupIndex;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Shelf-aware {@link TrinoInputFile} that wraps a delegate
 * {@link TrinoInputFile} (usually an S3-backed one from Trino's native
 * filesystem factory).
 *
 * <p>Metadata methods ({@link #length()}, {@link #lastModified()},
 * {@link #exists()}) always delegate — Shelf is not an authoritative source
 * of S3 object metadata. Only the read path goes through Shelf, and only via
 * {@link #newStream()}.
 *
 * <p>Inherits the fail-open invariant from {@link ShelfFileSystem}: any
 * Shelf-originated failure during a read degrades transparently to a
 * direct-delegate read.
 *
 * <p><b>Target selection (Phase-1).</b> {@link #newStream()} asks the
 * {@link MembershipResolver} for the owning pod at stream construction
 * time and captures that {@code (endpoint, CircuitBreaker)} pair for
 * the life of the stream. If the ring is empty (every pod unreachable,
 * DNS failure with no previous snapshot), the delegate stream is
 * returned directly — Trino reads S3 and never sees a Shelf error.
 * Subsequent {@code newStream()} calls observe fresh membership.
 *
 * <p><b>Row-group keying (SHELF-16).</b> The file-level ETag bytes are
 * derived from {@code (lastModified, length)}; the {@link ShelfInputStream}
 * combines them with each read's {@code (offset, length)} and an
 * ordinal from {@link RowGroupIndex} to produce per-range keys. By
 * default the index is {@link RowGroupIndex#constantZero()} — a
 * stateless sentinel that preserves pre-SHELF-16 key behaviour for
 * non-Parquet files and for Parquet files whose footer has not yet
 * been parsed.
 */
public final class ShelfInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile delegate;
    private final RangeFetcher fetcher;
    private final MembershipResolver resolver;
    private final Pool pool;
    private final RowGroupIndex index;

    /**
     * Backwards-compatible constructor that wires in the permissive
     * {@link RowGroupIndex#constantZero()} default. Existing callers
     * (plugin factory, filesystem wrapper, tests) keep working; the
     * row-group variant ships in the four-arg-plus-index constructor
     * below.
     */
    public ShelfInputFile(
            TrinoInputFile delegate,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            Pool pool)
    {
        this(delegate, fetcher, resolver, pool, RowGroupIndex.constantZero());
    }

    public ShelfInputFile(
            TrinoInputFile delegate,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            Pool pool,
            RowGroupIndex index)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.index = Objects.requireNonNull(index, "index");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        // TODO(SHELF-17): serve TrinoInput via cached range reads too.
        //   Trino-side pressure is strongest on newStream(), so we ship the
        //   stream path first and delegate newInput() verbatim for now.
        return delegate.newInput();
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        long length = delegate.length();
        Instant lastModified = delegate.lastModified();
        byte[] etag = deriveEtagBytes(length, lastModified);
        // Routing key for membership: the file-level key (ordinal 0)
        // keeps every read of a given file on the same shelfd pod.
        // This is deliberate — per-ordinal routing would fragment the
        // working set across too many pods and defeat locality.
        Key routingKey = Key.fromTuple(etag, 0L, Math.max(length, 1L), 0);
        Optional<MembershipResolver.Target> target = resolver.ownerFor(routingKey.asBytes());
        if (target.isEmpty()) {
            // Ring is empty — nothing to route to. Fail open: Trino
            // reads straight from S3 via the delegate stream.
            return delegate.newStream();
        }
        MembershipResolver.Target t = target.get();
        return new ShelfInputStream(
                delegate.newStream(),
                fetcher,
                t.breaker(),
                t.endpoint().toString(),
                pool,
                etag,
                index,
                length);
    }

    @Override
    public long length()
            throws IOException
    {
        return delegate.length();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return delegate.lastModified();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return delegate.exists();
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }

    /**
     * Derive an opaque "version identity" byte string from the
     * information Trino's SPI exposes on an input file
     * ({@code lastModified} + {@code length}). SHELF-07's HEAD endpoint
     * will later substitute the real S3 ETag; the wire format stays
     * identical because both forms are fed through {@link Key#fromTuple}
     * as opaque bytes.
     *
     * <p>Exposed package-private so {@link ShelfFileSystem} can compute
     * the same identity for the footer prefetch path (SHELF-15): the
     * prefetcher must land bytes under the exact key the subsequent
     * {@link ShelfInputStream} read will query, or the hit ratio
     * collapses to zero.
     */
    static byte[] deriveEtagBytes(long length, Instant lastModified)
    {
        String versionIdentity = lastModified.toEpochMilli() + "-" + length;
        return versionIdentity.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * File-level content key (offset {@code 0}, length = file length,
     * ordinal {@code 0}). Pre-SHELF-16 callers that still want a
     * "one key per file" view can use this, but the read path now
     * uses {@link Key#fromTuple(byte[], long, long, int)} per range.
     *
     * @deprecated Prefer {@link #deriveEtagBytes(long, Instant)} plus a
     *             per-range {@code Key.fromTuple} at the call site.
     *             Retained for the SHELF-15 prefetch path until it is
     *             rewritten to target the specific footer byte range.
     */
    @Deprecated
    static Key deriveContentKey(long length, Instant lastModified)
    {
        return Key.fromTuple(deriveEtagBytes(length, lastModified), 0L, Math.max(length, 1L), 0);
    }
}
