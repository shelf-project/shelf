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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
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
 */
public final class ShelfInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile delegate;
    private final RangeFetcher fetcher;
    private final MembershipResolver resolver;
    private final Pool pool;

    public ShelfInputFile(
            TrinoInputFile delegate,
            RangeFetcher fetcher,
            MembershipResolver resolver,
            Pool pool)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.pool = Objects.requireNonNull(pool, "pool");
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
        Key key = deriveContentKey(length, delegate.lastModified());
        Optional<MembershipResolver.Target> target = resolver.ownerFor(key.asBytes());
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
                key.toHex(),
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
     * Derive a content-addressed key from the information Trino already has
     * on an input file. This is a phase-1 compromise: the Trino SPI does not
     * expose S3 ETag, so we build an "opaque version identity" from
     * {@code (lastModified, length)} which changes whenever the underlying
     * S3 object is overwritten. SHELF-07's HEAD endpoint will let us swap in
     * the real ETag without changing any wire format.
     *
     * <p>Exposed package-private so {@link ShelfFileSystem} can compute
     * the same key for the footer prefetch path (SHELF-15): the
     * prefetcher must land bytes under the exact key the subsequent
     * {@link ShelfInputStream} read will query, or the hit ratio
     * collapses to zero.
     */
    static Key deriveContentKey(long length, Instant lastModified)
    {
        String versionIdentity = lastModified.toEpochMilli() + "-" + length;
        return Key.fromTuple(versionIdentity, 0L, length, 0);
    }
}
