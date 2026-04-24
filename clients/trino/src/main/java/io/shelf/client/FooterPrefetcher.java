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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Best-effort Parquet-footer prefetcher (SHELF-15, BLUEPRINT §7.3).
 *
 * <p>When Trino opens an Iceberg / Parquet file, the first thing every
 * reader does is seek to the last 8 bytes (metadata length + magic),
 * then to the metadata block immediately before it, and then back into
 * the data pages. Those two seeks alone are two round-trips to S3.
 * Shelf can cover them by priming the metadata pool with the final
 * 64 KiB of the object <em>before</em> Trino issues the first
 * {@code newStream()} read.
 *
 * <p><b>Shape of the request.</b> The prefetch issues exactly one
 * {@code rangeGet(Pool.METADATA, endpoint, contentKey, length - N, N)}
 * where {@code N} is the configured window (default 64 KiB, max 256).
 * Pool selection is {@link Pool#METADATA} <em>even though</em>
 * {@link io.shelf.filesystem.ShelfFileSystem#poolFor} routes Parquet
 * body reads to {@link Pool#ROWGROUP}: the <em>footer</em> of a
 * Parquet file is metadata payload per BLUEPRINT §6.1, and lives in
 * the metadata pool.
 *
 * <p><b>Fail-open.</b> Every failure (executor saturation,
 * {@code ShelfUnavailableException}, {@code OutOfMemoryError}, any
 * {@link Throwable}) is caught at the executor-task boundary and
 * silently recorded on {@link PrefetchMetrics#footerPrefetchFailed}.
 * The returned {@link CompletableFuture} always completes normally,
 * never exceptionally, so the foreground read path cannot trip on a
 * prefetch side-effect. The foreground read remains entirely
 * responsible for correctness — a prefetch miss just means the first
 * read does the same work it would do without Shelf prefetch.
 *
 * <p><b>Executor sizing.</b> Default 2 threads with a bounded
 * 64-element queue and {@link ThreadPoolExecutor.CallerRunsPolicy}.
 * The intent is: fire-and-forget on a cool coordinator / worker;
 * backpressure the <em>submitting</em> thread (which is Trino's
 * {@code newInputFile} caller) the moment Shelf is slow enough that
 * the queue fills. We never want prefetch to expand unboundedly and
 * cause an OOM on a hot coordinator.
 *
 * <p><b>Thread safety.</b> Instances are safe to share across all
 * catalog threads. {@link #close()} drains the executor and must be
 * called on plugin shutdown.
 *
 * @see PrefetchMetrics
 * @see io.shelf.filesystem.ShelfFileSystem
 */
public final class FooterPrefetcher
        implements AutoCloseable
{
    private static final Logger log = Logger.getLogger(FooterPrefetcher.class.getName());

    /** Default worker-pool size. Bigger pools don't help — prefetch is I/O-bound and already multiplexed over HTTP/2. */
    public static final int DEFAULT_POOL_SIZE = 2;
    /** Default bounded work queue length. Past this, {@link ThreadPoolExecutor.CallerRunsPolicy} applies backpressure. */
    public static final int DEFAULT_QUEUE_CAPACITY = 64;
    /** Drain budget on {@link #close()}. */
    public static final long CLOSE_DRAIN_SECONDS = 2L;

    private final RangeFetcher fetcher;
    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private final PrefetchMetrics metrics;

    /**
     * Build a prefetcher with a default 2-thread executor. The caller
     * owns the returned object's lifecycle and must invoke
     * {@link #close()} to drain the executor on shutdown.
     */
    public FooterPrefetcher(RangeFetcher fetcher)
    {
        this(fetcher, new PrefetchMetrics());
    }

    /** As {@link #FooterPrefetcher(RangeFetcher)} with a caller-supplied metrics sink. */
    public FooterPrefetcher(RangeFetcher fetcher, PrefetchMetrics metrics)
    {
        this(fetcher, defaultExecutor(), true, metrics);
    }

    /**
     * Build a prefetcher with a caller-supplied executor. Useful in
     * tests that want a direct/synchronous executor. The caller keeps
     * ownership of {@code executor} — {@link #close()} will <em>not</em>
     * shut it down.
     */
    public static FooterPrefetcher withExecutor(
            RangeFetcher fetcher,
            ExecutorService executor,
            PrefetchMetrics metrics)
    {
        return new FooterPrefetcher(fetcher, executor, false, metrics);
    }

    FooterPrefetcher(
            RangeFetcher fetcher,
            ExecutorService executor,
            boolean ownsExecutor,
            PrefetchMetrics metrics)
    {
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownsExecutor = ownsExecutor;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    /** @return the metrics sink this prefetcher writes to. Never null. */
    public PrefetchMetrics metrics()
    {
        return metrics;
    }

    /**
     * Schedule a best-effort footer prefetch for {@code contentKey} on
     * {@code target}. The request covers
     * {@code [fileLength - min(prefetchBytes, fileLength), fileLength)}.
     *
     * @return a future that completes normally once the prefetch task
     *         finishes (success or caught failure); never completes
     *         exceptionally.
     */
    public CompletableFuture<Void> prefetch(
            MembershipResolver.Target target,
            String contentKey,
            long fileLength,
            int prefetchBytes)
    {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(contentKey, "contentKey");
        if (prefetchBytes <= 0 || fileLength <= 0L) {
            return CompletableFuture.completedFuture(null);
        }
        long length = Math.min((long) prefetchBytes, fileLength);
        long offset = fileLength - length;

        metrics.incrementScheduled();
        try {
            return CompletableFuture.runAsync(() -> doPrefetch(target, contentKey, offset, length), executor);
        }
        catch (RejectedExecutionException e) {
            // Only reachable after close(); CallerRunsPolicy absorbs
            // saturation while the executor is running. Count as failed
            // so operators can see prefetch being dropped after shutdown
            // rather than silently disappearing.
            metrics.incrementFailed();
            log.log(Level.FINE, e, () -> "footer prefetch rejected for " + contentKey);
            return CompletableFuture.completedFuture(null);
        }
    }

    private void doPrefetch(
            MembershipResolver.Target target,
            String contentKey,
            long offset,
            long length)
    {
        // Catching Throwable is deliberate (BLUEPRINT §9.5 fail-open
        // invariant). A prefetch failure — including OOM, an assertion,
        // or a programming error — must never propagate and must never
        // cause the foreground Trino read to fail. All other layers in
        // this plugin keep the Throwable-catch ban; this is the single
        // documented exception.
        try {
            fetcher.rangeGet(target.endpoint().toString(), Pool.METADATA, contentKey, offset, length);
            metrics.incrementCompleted();
        }
        catch (Throwable t) {
            metrics.incrementFailed();
            log.log(Level.FINE, t, () -> "footer prefetch failed for " + contentKey
                    + " at " + target.endpoint() + " [" + offset + "+" + length + "]");
        }
    }

    @Override
    public void close()
    {
        if (!ownsExecutor) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(CLOSE_DRAIN_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static ExecutorService defaultExecutor()
    {
        final AtomicInteger n = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "shelf-footer-prefetch-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        return new ThreadPoolExecutor(
                DEFAULT_POOL_SIZE,
                DEFAULT_POOL_SIZE,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY),
                tf,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
