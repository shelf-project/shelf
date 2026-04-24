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
package io.shelf.client;

import io.shelf.client.ShelfHttpClient.ShelfUnavailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JDK-only tests for {@link FooterPrefetcher}.
 *
 * <p>Mockito struggles with Trino's sealed interfaces on JDK 25; hand-rolled
 * fakes keep the tests deterministic. The executor is a direct/synchronous
 * one so {@code future.get()} resolves before the assertion runs, even in
 * the happy path.
 */
class FooterPrefetcherTest
{
    private static final MembershipResolver.Target TARGET = new MembershipResolver.Target(
            "shelf-0",
            URI.create("http://shelf-0.shelf.svc.cluster.local:9090"),
            new CircuitBreaker("shelf-0"));
    private static final String CONTENT_KEY = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    private static final long FILE_LENGTH = 10L * 1024 * 1024;     // 10 MiB
    private static final int WINDOW_BYTES = 64 * 1024;             // 64 KiB

    private DirectExecutorService executor;
    private PrefetchMetrics metrics;

    @BeforeEach
    void setUp()
    {
        executor = new DirectExecutorService();
        metrics = new PrefetchMetrics();
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdown();
    }

    @Test
    void happyPathSubmitsOneRangeGetToMetadataPoolAndCompletes()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        RecordingFetcher fetcher = new RecordingFetcher(new byte[WINDOW_BYTES]);
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(fetcher, executor, metrics);

        CompletableFuture<Void> f = prefetcher.prefetch(TARGET, CONTENT_KEY, FILE_LENGTH, WINDOW_BYTES);
        f.get(2, TimeUnit.SECONDS);

        assertThat(fetcher.calls.get()).isEqualTo(1);
        assertThat(fetcher.lastEndpoint.get()).isEqualTo(TARGET.endpoint().toString());
        assertThat(fetcher.lastPool.get()).isEqualTo(Pool.METADATA);
        assertThat(fetcher.lastContentKey.get()).isEqualTo(CONTENT_KEY);
        assertThat(fetcher.lastOffset.get()).isEqualTo(FILE_LENGTH - WINDOW_BYTES);
        assertThat(fetcher.lastLength.get()).isEqualTo(WINDOW_BYTES);

        assertThat(metrics.footerPrefetchScheduled()).isEqualTo(1);
        assertThat(metrics.footerPrefetchCompleted()).isEqualTo(1);
        assertThat(metrics.footerPrefetchFailed()).isEqualTo(0);
    }

    @Test
    void failurePathSwallowsShelfUnavailableAndStillCompletes()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        RangeFetcher broken = (ep, pool, k, off, len) -> {
            throw new ShelfUnavailableException("shelfd is down");
        };
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(broken, executor, metrics);

        CompletableFuture<Void> f = prefetcher.prefetch(TARGET, CONTENT_KEY, FILE_LENGTH, WINDOW_BYTES);
        f.get(2, TimeUnit.SECONDS);

        assertThat(f.isCompletedExceptionally())
                .as("fail-open: the future must never bubble the ShelfUnavailableException out")
                .isFalse();
        assertThat(metrics.footerPrefetchScheduled()).isEqualTo(1);
        assertThat(metrics.footerPrefetchCompleted()).isEqualTo(0);
        assertThat(metrics.footerPrefetchFailed()).isEqualTo(1);
    }

    @Test
    void smallFileClampsRequestToFileLength()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        // File is 1 KiB, prefetch window asks for 64 KiB. The request must
        // be clamped to (offset=0, length=1024) rather than issuing a
        // negative offset or asking for more bytes than exist.
        long tinyFile = 1024L;
        RecordingFetcher fetcher = new RecordingFetcher(new byte[(int) tinyFile]);
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(fetcher, executor, metrics);

        prefetcher.prefetch(TARGET, CONTENT_KEY, tinyFile, WINDOW_BYTES).get(2, TimeUnit.SECONDS);

        assertThat(fetcher.calls.get()).isEqualTo(1);
        assertThat(fetcher.lastOffset.get()).isEqualTo(0L);
        assertThat(fetcher.lastLength.get()).isEqualTo(tinyFile);
        assertThat(metrics.footerPrefetchCompleted()).isEqualTo(1);
    }

    @Test
    void zeroPrefetchBytesMakesPrefetchANoOp()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        RecordingFetcher fetcher = new RecordingFetcher(new byte[WINDOW_BYTES]);
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(fetcher, executor, metrics);

        CompletableFuture<Void> f = prefetcher.prefetch(TARGET, CONTENT_KEY, FILE_LENGTH, 0);
        f.get(1, TimeUnit.SECONDS);

        assertThat(fetcher.calls.get()).isEqualTo(0);
        assertThat(metrics.footerPrefetchScheduled()).isEqualTo(0);
        assertThat(metrics.footerPrefetchCompleted()).isEqualTo(0);
        assertThat(metrics.footerPrefetchFailed()).isEqualTo(0);
    }

    @Test
    void catchesThrowableFromFetcherIncludingRuntimeException()
            throws ExecutionException, InterruptedException, TimeoutException
    {
        // Even a non-IOException Throwable (OOME, NullPointerException, an
        // Error bubbling out of a buggy HTTP client) must be caught per
        // BLUEPRINT §9.5.
        RangeFetcher exploding = (ep, pool, k, off, len) -> {
            throw new OutOfMemoryError("synthetic");
        };
        FooterPrefetcher prefetcher = FooterPrefetcher.withExecutor(exploding, executor, metrics);

        CompletableFuture<Void> f = prefetcher.prefetch(TARGET, CONTENT_KEY, FILE_LENGTH, WINDOW_BYTES);
        f.get(2, TimeUnit.SECONDS);

        assertThat(f.isCompletedExceptionally()).isFalse();
        assertThat(metrics.footerPrefetchFailed()).isEqualTo(1);
    }

    /** Records one rangeGet call and returns a configurable byte array. */
    private static final class RecordingFetcher
            implements RangeFetcher
    {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<String> lastEndpoint = new AtomicReference<>();
        final AtomicReference<Pool> lastPool = new AtomicReference<>();
        final AtomicReference<String> lastContentKey = new AtomicReference<>();
        final AtomicLong lastOffset = new AtomicLong();
        final AtomicLong lastLength = new AtomicLong();
        private final byte[] payload;

        RecordingFetcher(byte[] payload)
        {
            this.payload = payload;
        }

        @Override
        public byte[] rangeGet(String endpoint, Pool pool, String contentKey, long offset, long length)
        {
            calls.incrementAndGet();
            lastEndpoint.set(endpoint);
            lastPool.set(pool);
            lastContentKey.set(contentKey);
            lastOffset.set(offset);
            lastLength.set(length);
            byte[] out = new byte[(int) length];
            int copy = (int) Math.min(length, payload.length);
            System.arraycopy(payload, 0, out, 0, copy);
            return out;
        }
    }

    /** Runs tasks on the submitting thread. Nothing is ever queued. */
    private static final class DirectExecutorService
            extends AbstractExecutorService
    {
        private volatile boolean shutdown;

        @Override
        public void execute(Runnable command)
        {
            if (shutdown) {
                throw new java.util.concurrent.RejectedExecutionException("direct executor shut down");
            }
            command.run();
        }

        @Override
        public void shutdown()
        {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return shutdown;
        }

        @Override
        public boolean isTerminated()
        {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
        {
            return true;
        }
    }
}
