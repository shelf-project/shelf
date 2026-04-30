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
import io.shelf.client.Key;
import io.shelf.client.ParquetFooterIndex;
import io.shelf.client.Pool;
import io.shelf.client.RangeFetcher;
import io.shelf.client.RowGroupIndex;
import io.shelf.client.ShelfHttpClient.ShelfUnavailableException;
import io.trino.filesystem.TrinoInputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for the fail-open invariant (BLUEPRINT §9.5). Trino must
 * never observe a Shelf-specific exception.
 *
 * <p>Also pins the SHELF-16 per-range keying invariant: two reads
 * against different row-group ordinals must produce distinct
 * {@code contentKey} strings on the wire.
 */
class ShelfInputStreamTest
{
    private static final String ENDPOINT = "shelf.shelf.svc.cluster.local:9090";
    private static final byte[] ETAG = "test-etag".getBytes(StandardCharsets.UTF_8);
    private static final RowGroupIndex ZERO = RowGroupIndex.constantZero();

    @Test
    void hitReturnsFromShelfWithoutTouchingDelegate()
            throws IOException
    {
        byte[] payload = bytes(0, 128);
        DelegateStream delegate = new DelegateStream(payload);
        AtomicInteger shelfCalls = new AtomicInteger();
        RangeFetcher fetcher = (ep, pool, k, off, len) -> {
            shelfCalls.incrementAndGet();
            return Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        };
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");

        try (ShelfInputStream in = new ShelfInputStream(
                delegate, fetcher, breaker, ENDPOINT, Pool.ROWGROUP, ETAG, ZERO, payload.length)) {
            byte[] buf = new byte[64];
            int n = in.read(buf, 0, 64);
            assertThat(n).isEqualTo(64);
            assertThat(buf).isEqualTo(Arrays.copyOfRange(payload, 0, 64));
            assertThat(in.getPosition()).isEqualTo(64);
        }
        assertThat(shelfCalls.get()).isEqualTo(1);
        assertThat(delegate.reads).isZero();
    }

    @Test
    void shelfFailureFallsThroughToDelegateAndReturnsItsBytes()
            throws IOException
    {
        byte[] payload = bytes(0, 128);
        DelegateStream delegate = new DelegateStream(payload);
        RangeFetcher failing = (ep, pool, k, off, len) -> {
            throw new ShelfUnavailableException("simulated 503");
        };
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");

        try (ShelfInputStream in = new ShelfInputStream(
                delegate, failing, breaker, ENDPOINT, Pool.ROWGROUP, ETAG, ZERO, payload.length)) {
            byte[] buf = new byte[64];
            int n = in.read(buf, 0, 64);
            assertThat(n).isEqualTo(64);
            assertThat(buf).isEqualTo(Arrays.copyOfRange(payload, 0, 64));
        }
        assertThat(delegate.reads).isEqualTo(1);
    }

    @Test
    void failureIsStickyWithinStream()
            throws IOException
    {
        byte[] payload = bytes(0, 128);
        DelegateStream delegate = new DelegateStream(payload);
        AtomicInteger shelfCalls = new AtomicInteger();
        RangeFetcher halfBroken = (ep, pool, k, off, len) -> {
            shelfCalls.incrementAndGet();
            throw new ShelfUnavailableException("simulated 503");
        };
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");

        try (ShelfInputStream in = new ShelfInputStream(
                delegate, halfBroken, breaker, ENDPOINT, Pool.ROWGROUP, ETAG, ZERO, payload.length)) {
            byte[] buf = new byte[32];
            in.read(buf, 0, 32);
            in.read(buf, 0, 32);
            in.read(buf, 0, 32);
        }
        assertThat(shelfCalls.get())
                .as("sticky: only the first read should try Shelf")
                .isEqualTo(1);
        assertThat(delegate.reads).isEqualTo(3);
    }

    @Test
    void openBreakerSkipsShelfEntirely()
            throws IOException
    {
        byte[] payload = bytes(0, 32);
        DelegateStream delegate = new DelegateStream(payload);
        AtomicInteger shelfCalls = new AtomicInteger();
        RangeFetcher fetcher = (ep, pool, k, off, len) -> {
            shelfCalls.incrementAndGet();
            return Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        };
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");
        for (int i = 0; i < CircuitBreaker.DEFAULT_FAILURE_THRESHOLD; i++) {
            breaker.recordFailure();
        }
        assertThat(breaker.isOpen()).isTrue();

        try (ShelfInputStream in = new ShelfInputStream(
                delegate, fetcher, breaker, ENDPOINT, Pool.ROWGROUP, ETAG, ZERO, payload.length)) {
            byte[] buf = new byte[32];
            in.read(buf, 0, 32);
        }
        assertThat(shelfCalls.get()).isZero();
        assertThat(delegate.reads).isEqualTo(1);
    }

    @Test
    void seekUpdatesPositionAndShelfReadsFromNewOffset()
            throws IOException
    {
        byte[] payload = bytes(0, 128);
        DelegateStream delegate = new DelegateStream(payload);
        List<long[]> shelfRanges = new ArrayList<>();
        RangeFetcher fetcher = (ep, pool, k, off, len) -> {
            shelfRanges.add(new long[] {off, len});
            return Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        };
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");

        try (ShelfInputStream in = new ShelfInputStream(
                delegate, fetcher, breaker, ENDPOINT, Pool.ROWGROUP, ETAG, ZERO, payload.length)) {
            in.seek(64);
            byte[] buf = new byte[16];
            in.read(buf, 0, 16);
            assertThat(buf).isEqualTo(Arrays.copyOfRange(payload, 64, 80));
            assertThat(in.getPosition()).isEqualTo(80);
        }
        assertThat(shelfRanges).hasSize(1);
        assertThat(shelfRanges.get(0)).containsExactly(64L, 16L);
    }

    @Test
    void readPastEndReturnsMinusOne()
            throws IOException
    {
        byte[] payload = bytes(0, 16);
        RangeFetcher fetcher = (ep, pool, k, off, len) -> Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        CircuitBreaker breaker = new CircuitBreaker("shelf-0");
        try (ShelfInputStream in = new ShelfInputStream(
                new DelegateStream(payload), fetcher, breaker, ENDPOINT, Pool.ROWGROUP, ETAG, ZERO, payload.length)) {
            in.seek(payload.length);
            int n = in.read(new byte[4], 0, 4);
            assertThat(n).isEqualTo(-1);
        }
    }

    @Test
    void singleByteReadDelegatesToBulkPath()
            throws IOException
    {
        byte[] payload = bytes(0, 8);
        RangeFetcher fetcher = (ep, pool, k, off, len) -> Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        try (ShelfInputStream in = new ShelfInputStream(
                new DelegateStream(payload),
                fetcher,
                new CircuitBreaker("shelf-0"),
                ENDPOINT,
                Pool.ROWGROUP,
                ETAG,
                ZERO,
                payload.length)) {
            assertThat(in.read()).isEqualTo(0);
            assertThat(in.read()).isEqualTo(1);
            assertThat(in.read()).isEqualTo(2);
            assertThat(in.getPosition()).isEqualTo(3);
        }
    }

    /**
     * SHELF-16: two reads whose byte ranges live in different row
     * groups must go on the wire with distinct content keys, because
     * the {@link RowGroupIndex} resolves to distinct ordinals and the
     * key hash consumes the ordinal.
     */
    @Test
    void contentKeyDiffersBetweenRowGroupOrdinals()
            throws IOException
    {
        // Two row groups back-to-back in the file:
        //   rg#0: [0,   64)    → ordinal 0
        //   rg#1: [64, 128)    → ordinal 1
        ParquetFooterIndex index = ParquetFooterIndex.of(List.of(
                new ParquetFooterIndex.RowGroup(0L, 64L, 0),
                new ParquetFooterIndex.RowGroup(64L, 64L, 1)));

        byte[] payload = bytes(0, 128);
        List<String> keys = new ArrayList<>();
        RangeFetcher recording = (ep, pool, k, off, len) -> {
            keys.add(k);
            return Arrays.copyOfRange(payload, (int) off, (int) (off + len));
        };

        try (ShelfInputStream in = new ShelfInputStream(
                new DelegateStream(payload),
                recording,
                new CircuitBreaker("shelf-0"),
                ENDPOINT,
                Pool.ROWGROUP,
                ETAG,
                index,
                payload.length)) {
            // First read sits entirely inside rg#0.
            byte[] buf1 = new byte[32];
            in.read(buf1, 0, 32);
            // Seek to the start of rg#1 and issue the same-shape read.
            in.seek(64);
            byte[] buf2 = new byte[32];
            in.read(buf2, 0, 32);
        }

        assertThat(keys).hasSize(2);
        assertThat(keys.get(0))
                .as("rg#0 read must hash under ordinal 0")
                .isEqualTo(Key.fromTuple(ETAG, 0L, 32L, 0).toHex());
        assertThat(keys.get(1))
                .as("rg#1 read must hash under ordinal 1")
                .isEqualTo(Key.fromTuple(ETAG, 64L, 32L, 1).toHex());
        assertThat(keys.get(0))
                .as("SHELF-16: (file X, rg 0) and (file X, rg 1) must produce distinct keys")
                .isNotEqualTo(keys.get(1));
    }

    /** Minimal in-memory delegate stream that records how often it's invoked. */
    private static final class DelegateStream
            extends TrinoInputStream
    {
        private final byte[] data;
        private long position;
        int reads;

        DelegateStream(byte[] data)
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
            reads++;
            int n = (int) Math.min(len, data.length - position);
            System.arraycopy(data, (int) position, b, off, n);
            position += n;
            return n;
        }
    }

    private static byte[] bytes(int start, int len)
    {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) ((start + i) & 0xff);
        }
        return out;
    }
}
