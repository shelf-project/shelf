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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight in-process counters for Shelf's best-effort prefetch paths
 * (SHELF-15 footer prefetch today; SHELF-17 row-group lands later).
 *
 * <p>This class deliberately does <em>not</em> depend on Micrometer,
 * Dropwizard, OpenTelemetry, or any Trino-internal metrics API. The
 * plugin surface in BLUEPRINT §6.2 only promises counters a test can
 * observe; a real exporter is wired up in the operator layer (agent 8).
 *
 * <p>All counters are plain {@link AtomicLong}s; reads and writes are
 * lock-free and safe to call from any thread. Counters are monotonic and
 * never decremented.
 *
 * <p><b>Semantics.</b>
 * <ul>
 *   <li>{@code footerPrefetchScheduled} — incremented once per call to
 *       {@link FooterPrefetcher#prefetch} that actually submits a task
 *       to the executor (no-op calls, e.g. disabled prefetch window,
 *       do not increment).</li>
 *   <li>{@code footerPrefetchCompleted} — incremented when the async
 *       {@code rangeGet} returned a 2xx body without throwing.</li>
 *   <li>{@code footerPrefetchFailed} — incremented on <em>any</em>
 *       {@link Throwable} from the async task, including
 *       {@code ShelfUnavailableException}, timeouts, or the catch-all
 *       {@link Throwable} defence at the executor-task boundary
 *       (BLUEPRINT §9.5: a prefetch failure must never reach Trino).</li>
 * </ul>
 *
 * <p>Invariant: {@code scheduled == completed + failed + inFlight},
 * where {@code inFlight} is implicit (tasks submitted but not yet
 * resolved).
 */
public final class PrefetchMetrics
{
    private final AtomicLong footerPrefetchScheduled = new AtomicLong();
    private final AtomicLong footerPrefetchCompleted = new AtomicLong();
    private final AtomicLong footerPrefetchFailed = new AtomicLong();

    public PrefetchMetrics() {}

    /** @return number of footer-prefetch tasks successfully submitted. */
    public long footerPrefetchScheduled()
    {
        return footerPrefetchScheduled.get();
    }

    /** @return number of footer-prefetch tasks that finished a 2xx {@code rangeGet}. */
    public long footerPrefetchCompleted()
    {
        return footerPrefetchCompleted.get();
    }

    /** @return number of footer-prefetch tasks that terminated exceptionally. */
    public long footerPrefetchFailed()
    {
        return footerPrefetchFailed.get();
    }

    /* Package-private mutators. Only {@link FooterPrefetcher} increments. */

    void incrementScheduled()
    {
        footerPrefetchScheduled.incrementAndGet();
    }

    void incrementCompleted()
    {
        footerPrefetchCompleted.incrementAndGet();
    }

    void incrementFailed()
    {
        footerPrefetchFailed.incrementAndGet();
    }
}
