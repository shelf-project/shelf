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
package io.shelf.listener;

import io.shelf.listener.config.FailMode;
import io.shelf.listener.config.ListenerConfig;
import io.shelf.listener.metrics.ListenerMetrics;
import io.shelf.listener.support.TestEvents;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the three documented fail-mode behaviours when the bounded
 * queue cannot accept a row:
 *
 * <ul>
 *   <li>{@link FailMode#DROP} returns within the offer-timeout-zero
 *       budget and increments {@code dropped{queue_full}}.</li>
 *   <li>{@link FailMode#BLOCK} blocks for at most the configured
 *       block-timeout and then drops, never raising to Trino.</li>
 *   <li>{@link FailMode#LOG_ONLY} short-circuits before the queue;
 *       counters reflect dropped events with reason {@code log_only}.</li>
 * </ul>
 *
 * <p>The tests construct a listener with {@code writeEnabled=false}
 * (and therefore {@code sink=null}, no writer thread, no Iceberg deps
 * exercised) and a queue capacity of 1, so the very first
 * {@code queryCompleted} call takes the lone slot and every subsequent
 * one exercises the failure path. Because no writer drains the queue,
 * we get a deterministic full state.
 */
class FailModeTest
{
    private static ListenerConfig config(FailMode mode, int capacity, long blockMs)
    {
        ListenerConfig.Builder b = new ListenerConfig.Builder();
        b.catalogName = "hive";
        b.tableSchema = "trino_logs";
        b.tableName = "queries";
        b.failMode = mode;
        b.queueCapacity = capacity;
        b.queueBlockTimeout = Duration.ofMillis(blockMs);
        // writeEnabled defaults to true; we deliberately keep it that way
        // so the listener attempts to open a sink, but supply null in the
        // explicit two-arg constructor below to skip the IcebergSink.
        b.writeEnabled = true;
        return b.build();
    }

    @Test
    void dropReturnsImmediatelyAndCountsQueueFull()
    {
        ListenerConfig cfg = config(FailMode.DROP, 1, 0);
        try (ShelfIcebergEventListener listener = new ShelfIcebergEventListener(cfg, /* sink */ null)) {
            QueryCompletedEvent ev = TestEvents.canonical();
            listener.queryCompleted(ev);
            // Second call: queue full → drop
            long t0 = System.nanoTime();
            listener.queryCompleted(ev);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            assertThat(elapsedMs).isLessThan(50L);
            ListenerMetrics.Snapshot s = listener.metrics().snapshot();
            assertThat(s.events.get("received")).isEqualTo(2L);
            assertThat(s.dropped.get("queue_full")).isEqualTo(1L);
        }
    }

    @Test
    void blockHonoursTimeoutAndDropsAfter()
    {
        long blockMs = 30L;
        ListenerConfig cfg = config(FailMode.BLOCK, 1, blockMs);
        try (ShelfIcebergEventListener listener = new ShelfIcebergEventListener(cfg, /* sink */ null)) {
            QueryCompletedEvent ev = TestEvents.canonical();
            listener.queryCompleted(ev);
            // Second call: queue full → block up to blockMs, then drop.
            long t0 = System.nanoTime();
            listener.queryCompleted(ev);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            assertThat(elapsedMs).isGreaterThanOrEqualTo(blockMs - 5L);
            // Bound the upper end so a regression to "block forever" surfaces.
            assertThat(elapsedMs).isLessThan(blockMs + 500L);
            assertThat(listener.metrics().snapshot().dropped.get("queue_full")).isEqualTo(1L);
        }
    }

    @Test
    void logOnlyShortCircuitsBeforeQueue()
    {
        ListenerConfig cfg = config(FailMode.LOG_ONLY, 1, 0);
        try (ShelfIcebergEventListener listener = new ShelfIcebergEventListener(cfg, /* sink */ null)) {
            QueryCompletedEvent ev = TestEvents.canonical();
            for (int i = 0; i < 5; i++) {
                listener.queryCompleted(ev);
            }
            assertThat(listener.queueDepth()).isZero();
            ListenerMetrics.Snapshot s = listener.metrics().snapshot();
            assertThat(s.events.get("received")).isEqualTo(5L);
            assertThat(s.dropped.get("log_only")).isEqualTo(5L);
            assertThat(s.events.get("written")).isZero();
        }
    }
}
