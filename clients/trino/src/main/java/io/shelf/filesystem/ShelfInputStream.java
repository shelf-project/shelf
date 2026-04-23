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
import io.shelf.client.ShelfHttpClient;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.util.Objects;

/**
 * Shelf-aware {@link TrinoInputStream}.
 *
 * <p><b>Fail-open invariant (BLUEPRINT §9.5).</b> Every read path has two
 * legs:
 * <ol>
 *   <li>If the circuit breaker is closed, ask Shelf via
 *       {@link RangeFetcher#rangeGet} with a per-RPC deadline.</li>
 *   <li>On <em>any</em> Shelf-originated failure —
 *       {@link ShelfHttpClient.ShelfUnavailableException},
 *       any {@link IOException} from the fetcher — silently re-issue the
 *       same byte range against the underlying {@link TrinoInputStream}
 *       delegate, record the failure on the breaker, and return the
 *       delegate's bytes to Trino.</li>
 * </ol>
 *
 * <p>Failover is sticky per stream: once a Shelf call fails for a stream,
 * the stream uses the delegate for the remainder of its lifetime. Trino
 * never sees a Shelf-specific error.
 */
public final class ShelfInputStream
        extends TrinoInputStream
{
    private final TrinoInputStream delegate;
    private final RangeFetcher fetcher;
    private final CircuitBreaker breaker;
    private final String endpoint;
    private final Pool pool;
    private final String contentKey;
    private final long length;

    private long position;
    private boolean closed;
    /** True once any Shelf call has failed for this stream; reads stay on the delegate. */
    private boolean stickyDelegate;

    public ShelfInputStream(
            TrinoInputStream delegate,
            RangeFetcher fetcher,
            CircuitBreaker breaker,
            String endpoint,
            Pool pool,
            String contentKey,
            long length)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.breaker = Objects.requireNonNull(breaker, "breaker");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.contentKey = Objects.requireNonNull(contentKey, "contentKey");
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        this.length = length;
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    @Override
    public void seek(long newPosition)
            throws IOException
    {
        ensureOpen();
        if (newPosition < 0) {
            throw new IOException("negative seek: " + newPosition);
        }
        if (stickyDelegate) {
            delegate.seek(newPosition);
        }
        this.position = newPosition;
    }

    @Override
    public int read()
            throws IOException
    {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        if (n == -1) {
            return -1;
        }
        return one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        Objects.requireNonNull(b, "b");
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException("off=" + off + " len=" + len + " b.length=" + b.length);
        }
        ensureOpen();
        if (len == 0) {
            return 0;
        }
        if (position >= length) {
            return -1;
        }
        int want = (int) Math.min((long) len, length - position);

        if (!stickyDelegate && !breaker.isOpen()) {
            try {
                byte[] bytes = fetcher.rangeGet(endpoint, pool, contentKey, position, want);
                System.arraycopy(bytes, 0, b, off, want);
                breaker.recordSuccess();
                position += want;
                return want;
            }
            catch (IOException e) {
                // Covers both ShelfUnavailableException (which extends
                // IOException) and any IOException the fetcher may raise
                // (connection closed, read interrupted, etc).
                breaker.recordFailure();
                stickyDelegate = true;
                delegate.seek(position);
            }
        }
        else if (!stickyDelegate) {
            // Breaker is open: skip Shelf for the remainder of this stream
            // and let the delegate drive the read.
            stickyDelegate = true;
            delegate.seek(position);
        }

        int n = delegate.read(b, off, want);
        if (n > 0) {
            position += n;
        }
        return n;
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            delegate.close();
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("stream closed");
        }
    }
}
