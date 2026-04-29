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
package io.shelf.listener.writer;

import io.shelf.listener.config.ListenerConfig;
import io.shelf.listener.extract.ExtractedRow;
import io.shelf.listener.metrics.ListenerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drains the bounded ingest queue, batches rows, and flushes via
 * {@link IcebergSink}. Exits cleanly on {@link #shutdown(long)}.
 *
 * <p>The thread is intentionally simple: one consumer pulls from the
 * queue, accumulates up to {@code batchMaxRows} or until the
 * {@code batchMaxInterval} elapses, then calls {@code sink.write(batch)}.
 * Failures bump {@code shelf_listener_write_errors_total}; we never
 * propagate to the SPI thread.
 */
public final class WriterThread
        extends Thread
{
    private static final Logger LOG = LoggerFactory.getLogger(WriterThread.class);

    private final BlockingQueue<ExtractedRow> queue;
    private final IcebergSink sink;
    private final ListenerMetrics metrics;
    private final int batchMaxRows;
    private final long batchMaxIntervalMs;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public WriterThread(
            BlockingQueue<ExtractedRow> queue,
            IcebergSink sink,
            ListenerMetrics metrics,
            ListenerConfig config)
    {
        super("shelf-listener-writer");
        setDaemon(true);
        this.queue = queue;
        this.sink = sink;
        this.metrics = metrics;
        this.batchMaxRows = config.batchMaxRows();
        this.batchMaxIntervalMs = config.batchMaxInterval().toMillis();
    }

    @Override
    public void run()
    {
        List<ExtractedRow> batch = new ArrayList<>(batchMaxRows);
        long batchStartedNanos = System.nanoTime();

        while (running.get() || !queue.isEmpty()) {
            try {
                long elapsedMs = (System.nanoTime() - batchStartedNanos) / 1_000_000L;
                long pollWaitMs = running.get()
                        ? Math.max(0L, batchMaxIntervalMs - elapsedMs)
                        // Once shutdown is signalled, drain at full speed.
                        : 0L;
                ExtractedRow row = queue.poll(pollWaitMs, TimeUnit.MILLISECONDS);
                if (row != null) {
                    batch.add(row);
                    metrics.setQueueDepth(queue.size());
                }
                boolean intervalElapsed = (System.nanoTime() - batchStartedNanos) / 1_000_000L
                        >= batchMaxIntervalMs;
                boolean batchFull = batch.size() >= batchMaxRows;
                boolean drainOnShutdown = !running.get() && !batch.isEmpty();
                if ((batchFull || (intervalElapsed && !batch.isEmpty()) || drainOnShutdown)) {
                    flush(batch);
                    batch = new ArrayList<>(batchMaxRows);
                    batchStartedNanos = System.nanoTime();
                }
                else if (intervalElapsed) {
                    // Empty batch + interval up: just reset the clock so we
                    // do not livelock on long idle gaps.
                    batchStartedNanos = System.nanoTime();
                }
            }
            catch (InterruptedException e) {
                // Treat interrupt as a shutdown signal. We deliberately do
                // not re-assert the interrupt flag here so the remaining
                // queue items can be drained via the loop below before the
                // thread exits.
                running.set(false);
            }
            catch (Throwable t) {
                metrics.recordWriteError("unknown");
                LOG.warn("writer loop error: {}", t.toString());
            }
        }

        if (!batch.isEmpty()) {
            flush(batch);
        }
    }

    private void flush(List<ExtractedRow> batch)
    {
        long startNanos = System.nanoTime();
        try {
            int n = sink.write(batch);
            for (int i = 0; i < n; i++) {
                metrics.recordEvent("written");
            }
        }
        catch (org.apache.iceberg.exceptions.CommitFailedException
                | org.apache.iceberg.exceptions.CommitStateUnknownException e) {
            metrics.recordWriteError("iceberg_commit");
            LOG.warn("Iceberg commit failed; {} events dropped", batch.size());
        }
        catch (RuntimeException e) {
            // Fallback: still classify Iceberg-namespace exceptions as commit failures.
            if (e.getClass().getName().startsWith("org.apache.iceberg.exceptions.")) {
                metrics.recordWriteError("iceberg_commit");
            }
            else {
                metrics.recordWriteError("unknown");
            }
            LOG.warn("flush failed: {}", e.toString());
        }
        catch (java.io.IOException e) {
            metrics.recordWriteError("serialization");
            LOG.warn("flush IO error: {}", e.toString());
        }
        finally {
            double seconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
            metrics.recordWriteSeconds(seconds);
            metrics.setQueueDepth(queue.size());
        }
    }

    public void shutdown(long awaitMillis)
    {
        running.set(false);
        // Wake the poll(...) without waiting up to batchMaxIntervalMs.
        this.interrupt();
        try {
            this.join(awaitMillis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
