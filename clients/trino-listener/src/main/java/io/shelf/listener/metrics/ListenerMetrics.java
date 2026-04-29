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
package io.shelf.listener.metrics;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Hand-rolled metrics registry. We intentionally do NOT pull in Prometheus
 * client_java or Micrometer — both are large and bring transitive deps
 * that conflict with Trino's own metrics stack. The registry exposes:
 *
 * <ul>
 *   <li>{@code shelf_listener_events_total{outcome}} — outcome ∈
 *       {@code received | written | dropped}.</li>
 *   <li>{@code shelf_listener_queue_depth} — gauge, sampled at scrape.</li>
 *   <li>{@code shelf_listener_write_seconds_*} — fixed exponential-bucket
 *       histogram.</li>
 *   <li>{@code shelf_listener_write_errors_total{reason}} — reason ∈
 *       {@code iceberg_commit | serialization | unknown}.</li>
 *   <li>{@code shelf_listener_dropped_total{reason}} — reason ∈
 *       {@code queue_full | log_only | shutdown}.</li>
 * </ul>
 *
 * <p>Same instance is registered as a JMX MBean (see {@link ListenerMBean})
 * and is the source for the Prom HTTP exporter (see {@link PromExporter}).
 */
public final class ListenerMetrics
{
    /** Buckets in seconds; 500 µs → ~33 s, mirrors the shelfd convention. */
    public static final double[] WRITE_BUCKETS = new double[] {
            0.0005, 0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064,
            0.128, 0.256, 0.512, 1.024, 2.048, 4.096, 8.192, 16.384, 32.768
    };

    private final ConcurrentMap<String, AtomicLong> events = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> writeErrors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> dropped = new ConcurrentHashMap<>();

    private final AtomicLong queueDepth = new AtomicLong();
    private final AtomicLong queueCapacity = new AtomicLong();

    private final AtomicLongArray writeBuckets = new AtomicLongArray(WRITE_BUCKETS.length + 1);
    private final AtomicLong writeSecondsSumMicros = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    public ListenerMetrics()
    {
        // Pre-populate well-known label values so scrapes never see a
        // missing series; downstream alerts can use rate(...) without an
        // `or vector(0)` hack.
        for (String o : new String[] {"received", "written", "dropped"}) {
            events.put(o, new AtomicLong());
        }
        for (String r : new String[] {"iceberg_commit", "serialization", "unknown"}) {
            writeErrors.put(r, new AtomicLong());
        }
        for (String r : new String[] {"queue_full", "log_only", "shutdown"}) {
            dropped.put(r, new AtomicLong());
        }
    }

    public void recordEvent(String outcome)
    {
        events.computeIfAbsent(outcome, k -> new AtomicLong()).incrementAndGet();
    }

    public void recordWriteError(String reason)
    {
        writeErrors.computeIfAbsent(reason, k -> new AtomicLong()).incrementAndGet();
    }

    public void recordDropped(String reason)
    {
        dropped.computeIfAbsent(reason, k -> new AtomicLong()).incrementAndGet();
    }

    public void setQueueDepth(long depth) { queueDepth.set(depth); }

    public void setQueueCapacity(long cap) { queueCapacity.set(cap); }

    public void recordWriteSeconds(double seconds)
    {
        // Per-bucket cumulative count; final bucket holds +Inf overflow.
        int idx = Arrays.binarySearch(WRITE_BUCKETS, seconds);
        if (idx < 0) {
            idx = -idx - 1;
        }
        if (idx > WRITE_BUCKETS.length) {
            idx = WRITE_BUCKETS.length;
        }
        writeBuckets.incrementAndGet(idx);
        writeSecondsSumMicros.addAndGet((long) (seconds * 1_000_000));
        writeCount.incrementAndGet();
    }

    /** Snapshot into a sorted map for stable JSON / Prometheus output. */
    public Snapshot snapshot()
    {
        Map<String, Long> e = new TreeMap<>();
        events.forEach((k, v) -> e.put(k, v.get()));
        Map<String, Long> we = new TreeMap<>();
        writeErrors.forEach((k, v) -> we.put(k, v.get()));
        Map<String, Long> d = new TreeMap<>();
        dropped.forEach((k, v) -> d.put(k, v.get()));

        long[] buckets = new long[writeBuckets.length()];
        long cumulative = 0;
        for (int i = 0; i < buckets.length; i++) {
            cumulative += writeBuckets.get(i);
            buckets[i] = cumulative;
        }
        return new Snapshot(
                e, we, d, queueDepth.get(), queueCapacity.get(),
                buckets, writeSecondsSumMicros.get() / 1_000_000.0, writeCount.get());
    }

    /** Read-only snapshot; safe to share across threads. */
    public static final class Snapshot
    {
        public final Map<String, Long> events;
        public final Map<String, Long> writeErrors;
        public final Map<String, Long> dropped;
        public final long queueDepth;
        public final long queueCapacity;
        public final long[] writeBucketsCumulative;
        public final double writeSecondsSum;
        public final long writeCount;

        Snapshot(Map<String, Long> events,
                Map<String, Long> writeErrors,
                Map<String, Long> dropped,
                long queueDepth,
                long queueCapacity,
                long[] writeBucketsCumulative,
                double writeSecondsSum,
                long writeCount)
        {
            this.events = events;
            this.writeErrors = writeErrors;
            this.dropped = dropped;
            this.queueDepth = queueDepth;
            this.queueCapacity = queueCapacity;
            this.writeBucketsCumulative = writeBucketsCumulative;
            this.writeSecondsSum = writeSecondsSum;
            this.writeCount = writeCount;
        }
    }
}
